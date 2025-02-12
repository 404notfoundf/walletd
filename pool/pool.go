package pool

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/walletd/internal/threadgroup"
	"go.uber.org/zap"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	DxPoolName = "dxpool"
)

type Pool struct {
	// blockMem map[types.BlockHeader]*types.Block // Mappings from headers to the blocks they are derived from.
	// blockTxns       *txnList                           // list of transactions that are supposed to be solved in the next block
	// headerMem       []types.BlockHeader // A circular list of headers that have been given out from the api recently.
	sourceBlock     types.Block // The block from which new headers for mining are created.
	sourceBlockTime time.Time   // How long headers have been using the same block (different from 'recent block').
	// memProgress     int                 // The index of the most recent header used in headerMem.

	// Transaction pool variables.
	// fullSets        map[modules.TransactionSetID][]int
	// blockMapHeap    *mapHeap
	// overflowMapHeap *mapHeap
	// setCounter int
	// splitSets       map[splitSetID]*splitSet

	// log
	log *zap.Logger

	// chainManager
	cm ChainManager

	// store
	store Store

	// threadGroup
	tg *threadgroup.ThreadGroup

	mu sync.Mutex // protects the fields below

	s Syncer
	// persistDir string

	setting PoolInternalSettings

	persist persistence

	// block template
	Template Template
}

type (
	ChainManager interface {
		Tip() types.ChainIndex
		TipState() consensus.State
		PoolTransactions() []types.Transaction
		V2PoolTransactions() []types.V2Transaction
		AddBlocks([]types.Block) error

		OnReorg(fn func(types.ChainIndex)) (cancel func())
		UpdatesSince(index types.ChainIndex, max int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
	}

	Store interface {
		UpdateChainState(reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error
		ResetChainState() error
		LastCommittedIndex() (types.ChainIndex, error)
	}

	Syncer interface {
		BroadcastHeader(types.BlockHeader)
		BroadcastV2BlockOutline(bo gateway.V2BlockOutline)
	}
)

func (p *Pool) syncStore(ctx context.Context, store Store, cm ChainManager, index types.ChainIndex, batchSize int) error {
	p.log.Info("current chain manager", zap.Any("chain tip", cm.Tip()))
	p.log.Info("current store ", zap.Any("store tip", index))

	p.log.Info("pool transaction ", zap.Any("len ", len(cm.PoolTransactions())))
	p.log.Info("pool v2 transaction ", zap.Any("len ", len(cm.V2PoolTransactions())))
	for index != cm.Tip() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		crus, caus, err := cm.UpdatesSince(index, batchSize)
		log.Printf("height %v: %v applied blocks, %v reverted blocks", index.Height, len(caus), len(crus))

		if err != nil {
			return fmt.Errorf("failed to subscribe to chain manager: %w", err)
		} else if err := store.UpdateChainState(crus, caus); err != nil {
			return fmt.Errorf("failed to update chain state: %w", err)
		}

		switch {
		case len(caus) > 0:

			index = caus[len(caus)-1].State.Index
			p.persist.SetTarget(caus[len(caus)-1].State.ChildTarget)

			p.sourceBlock.ParentID = caus[len(caus)-1].Block.ParentID
			p.sourceBlock.Timestamp = time.Now()

			p.sourceBlock = p.buildForBlock(true)

		case len(crus) > 0:
			index = crus[len(crus)-1].State.Index

		}
	}
	return nil
}

func (p *Pool) startServer() {
	p.log.Info("--- waiting for sync ---")
	ctx, cancel, err := p.tg.AddWithContext(context.Background())
	if err != nil {
		log.Panic("failed to add to thread group", zap.Error(err))
	}
	defer cancel()

	select {
	case <-ctx.Done():
		return
	default:
	}

	for {
		if IsSynced(p.cm.TipState()) {
			parentID := p.cm.Tip().ID
			p.mu.Lock()
			p.sourceBlock.ParentID = parentID
			p.sourceBlock = p.buildForBlock(true)
			p.mu.Unlock()

			p.persist.SetTarget(p.cm.TipState().ChildTarget)
			p.log.Info("	Starting stratum Server")

			go p.startHttp(p.setting.port)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func NewPool(cm ChainManager, store Store, s Syncer, opts ...Option) (*Pool, error) {
	p := &Pool{
		cm:    cm,
		store: store,
		s:     s,
		log:   zap.NewNop(),
		tg:    threadgroup.New(),
	}

	for _, opt := range opts {
		opt(p)
	}

	go p.startServer()

	// start a goroutine to sync the store with the chain manager
	reorgChan := make(chan struct{}, 1)
	reorgChan <- struct{}{}
	unsubscribe := cm.OnReorg(func(index types.ChainIndex) {
		select {
		case reorgChan <- struct{}{}:
		default:
		}
	})

	go func() {
		defer unsubscribe()

		log := p.log.Named("pool sync")
		ctx, cancel, err := p.tg.AddWithContext(context.Background())
		if err != nil {
			log.Panic("failed to add to thread group", zap.Error(err))
		}
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case <-reorgChan:
			}

			p.mu.Lock()

			// get store
			lastTip, err := store.LastCommittedIndex()
			if err != nil {
				log.Panic("failed to get last committed index", zap.Error(err))
			}

			err = p.syncStore(ctx, store, cm, lastTip, p.setting.syncBatchSize)
			if err != nil {
				switch {
				case errors.Is(err, context.Canceled):
					p.mu.Unlock()
					return
				case strings.Contains(err.Error(), "missing block at index"): // unfortunate, but not exposed by coreutils
					log.Warn("missing block at index, resetting chain state", zap.Stringer("id", lastTip.ID), zap.Uint64("height", lastTip.Height))
					if err := store.ResetChainState(); err != nil {
						log.Panic("failed to reset wallet state", zap.Error(err))
					}
					// trigger resync
					select {
					case reorgChan <- struct{}{}:
					default:
					}
				default:
					log.Panic("failed to sync store", zap.Error(err))
				}
			}
			p.mu.Unlock()
		}
	}()

	return p, nil
}

func (p *Pool) Close() error {
	p.tg.Stop()
	return nil
}

func (p *Pool) coinB1() types.Transaction {
	s := fmt.Sprintf("\000     Software: siad-miningpool-module v%d.%02d\nPool name: \"%s\"     \000", MajorVersion, MinorVersion, p.setting.name)
	if ((len(PrefixNonSia[:]) + len(s)) % 2) != 0 {
		// odd length, add extra null
		s = s + "\000"
	}
	cb := make([]byte, len(PrefixNonSia[:])+len(s)) // represents the bytes appended later
	n := copy(cb, PrefixNonSia[:])
	copy(cb[n:], s)
	return types.Transaction{
		ArbitraryData: [][]byte{cb},
	}
}

func (p *Pool) coinB1Txn() string {
	coinbaseTxn := p.coinB1()
	buf := new(bytes.Buffer)
	MarshalSiaArbDataNoSignatures(coinbaseTxn, buf)
	b := buf.Bytes()
	binary.LittleEndian.PutUint64(b[72:87], binary.LittleEndian.Uint64(b[72:87])+8)
	return hex.EncodeToString(b)
}

func (p *Pool) coinB2() string {
	return "0000000000000000"
}

func (p *Pool) startHttp(port string) {
	http.HandleFunc("/getblocktmp", func(writer http.ResponseWriter, request *http.Request) {
		getBlockTemplate(writer, request, p)
	})

	http.HandleFunc("/submit", func(writer http.ResponseWriter, request *http.Request) {
		handleBlockSubmit(writer, request, p)
	})
	http.ListenAndServe(port, nil)
}

func getBlockTemplate(w http.ResponseWriter, r *http.Request, p *Pool) {
	p.sourceBlock = p.buildForBlock(false)

	cs := p.cm.TipState()
	p.sourceBlock.ParentID = cs.Index.ID

	nbits := fmt.Sprintf("%08x", BigToCompact(BlockIdToBigInt(p.persist.Target)))

	var buf bytes.Buffer
	WriteUint64(&buf, uint64(p.sourceBlock.Timestamp.Unix()))
	ntime := hex.EncodeToString(buf.Bytes())

	WriteJSON(w, ConsensusNotify{
		Target:    p.persist.Target,
		Height:    cs.Index.Height + 1,
		Block:     p.sourceBlock,
		Coinbase1: p.coinB1Txn(),
		Coinbase2: p.coinB2(),
		Merkle:    ReadMerkleBranches(p.sourceBlock),
		Nbits:     nbits,
		Ntime:     ntime,
	})
}

func handleBlockSubmit(w http.ResponseWriter, r *http.Request, p *Pool) {
	var block types.Block
	response := make(map[string]types.Block)
	if err := json.NewDecoder(r.Body).Decode(&response); err != nil {
		WriteJSON(w, HandleSubmitResponse{
			Message: err.Error(),
		})
	}

	if _, ok := response["block"]; ok {
		block = response["block"]
	}

	err := p.manageSubmitBlock(block)

	WriteJSON(w, HandleSubmitResponse{
		Message: func() string {
			if err != nil {
				return err.Error()
			}
			return ""
		}(),
	})
}
