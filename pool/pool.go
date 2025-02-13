package pool

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/walletd/internal/threadgroup"
	"go.uber.org/zap"
	"net/http"
	"sync"
	"time"
)

type Pool struct {
	sourceBlock     types.Block // The block from which new headers for mining are created.
	sourceBlockTime time.Time   // How long headers have been using the same block (different from 'recent block').

	// log
	log *zap.Logger

	// chainManager
	cm ChainManager

	// store
	store Store

	// threadGroup
	tg *threadgroup.ThreadGroup

	mu sync.Mutex // protects the fields

	s Syncer

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
		Block(id types.BlockID) (types.Block, bool)
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

func NewPool(cm ChainManager, s Syncer, opts ...Option) (*Pool, error) {
	if cm == nil {
		return nil, ErrNilChainManager
	}

	if s == nil {
		return nil, ErrNilSyncer
	}

	p := &Pool{
		cm:  cm,
		s:   s,
		log: zap.NewNop(),
		tg:  threadgroup.New(),
	}

	for _, opt := range opts {
		opt(p)
	}

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

		ctx, cancel, err := p.tg.AddWithContext(context.Background())
		if err != nil {
			p.log.Panic("failed to add to thread group", zap.Error(err))
		}
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case <-reorgChan:
			}

			p.processHeightChange()
		}
	}()

	go p.StartServer()

	return p, nil
}

func (p *Pool) StartServer() {
	p.log.Info("      Waiting for consensus synchronization... \n")
	ctx, cancel, err := p.tg.AddWithContext(context.Background())
	if err != nil {
		p.log.Panic("failed to add to thread group", zap.Error(err))
	}

	defer cancel()
	select {
	case <-ctx.Done():
		return
	default:
	}

	for {
		// first sync
		state := p.cm.TipState()
		if IsSynced(state) {
			finalBlock, _ := p.cm.Block(state.Index.ID)

			p.mu.Lock()
			p.sourceBlock = p.buildBlockForWork(true)
			p.sourceBlock.ParentID = finalBlock.ID()
			p.sourceBlock.Timestamp = MedianTimestamp(state)
			p.mu.Unlock()

			go p.startHttp(p.setting.port)
			return
		}

		time.Sleep(time.Second)
	}
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
	MarshalArbDataNoSignatures(coinbaseTxn, buf)
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
	p.sourceBlock = p.buildBlockForWork(false)

	chainIndex := p.cm.Tip()

	block, _ := p.cm.Block(chainIndex.ID)

	p.sourceBlock.ParentID = block.ID()

	nbits := fmt.Sprintf("%08x", BigToCompact(BlockIdToBigInt(p.persist.Target)))

	var buf bytes.Buffer
	WriteUint64(&buf, uint64(p.sourceBlock.Timestamp.Unix()))
	ntime := hex.EncodeToString(buf.Bytes())

	WriteJSON(w, ConsensusNotify{
		Target:    p.persist.Target,
		Height:    chainIndex.Height + 1,
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
