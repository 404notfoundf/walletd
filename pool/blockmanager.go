package pool

import (
	"fmt"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"time"
)

func (p *Pool) buildBlockForWork(isForce bool) types.Block {
	if !isForce && time.Now().Before(p.sourceBlock.Timestamp.Add(10*time.Second)) {
		return p.sourceBlock
	}

	state := p.cm.TipState()
	transactions := p.cm.PoolTransactions()

	block := types.Block{
		Timestamp:    types.CurrentTimestamp(),
		Transactions: transactions,
	}

	payoutVal := p.calculateBlockPayout(state, &block)

	block.MinerPayouts = []types.SiacoinOutput{{
		Address: p.setting.wallet,
		Value:   payoutVal,
	}}

	return block
}

func (p *Pool) processHeightChange() {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.cm.TipState()
	if p.persist.GetBlockHeight() != state.Index.Height {
		p.log.Info("height update", zap.Uint64("chain manager height", state.Index.Height), zap.Uint64("persist height", p.persist.GetBlockHeight()))

		p.persist.SetBlockHeight(state.Index.Height)
		p.persist.SetTarget(state.ChildTarget)

		block, _ := p.cm.Block(state.Index.ID)

		p.sourceBlock.ParentID = block.ParentID
		p.sourceBlock.Timestamp = MedianTimestamp(state)

		if IsSynced(state) {
			p.sourceBlock = p.buildBlockForWork(true)
		}
	}
}

func (p *Pool) manageSubmitBlock(b types.Block) error {
	p.log.Info(fmt.Sprintf("manage submit block called on block id: %s, block has %d v1 txs, %d v2 txs\n", b.ID(), len(b.Transactions), len(b.V2Transactions())))

	if err := p.cm.AddBlocks([]types.Block{b}); err != nil {
		p.log.Error(fmt.Sprintf("chain manager failed to add block, err: %s", err.Error()))
		return err
	}

	// broadcast block
	if b.V2 == nil {
		p.s.BroadcastHeader(b.Header())
	} else {
		p.s.BroadcastV2BlockOutline(gateway.OutlineBlock(b, p.cm.PoolTransactions(), p.cm.V2PoolTransactions()))
	}

	return nil
}

func (p *Pool) calculateBlockPayout(state consensus.State, block *types.Block) types.Currency {
	if !isAllowV2Transaction(state) {
		return CalculateSubsidy(state, block.Transactions, nil)
	}

	v2Transactions := p.cm.V2PoolTransactions()
	payoutVal := CalculateSubsidy(state, block.Transactions, v2Transactions)

	if len(v2Transactions) > 0 {
		block.V2 = ConstructV2BlockData(state, block.Transactions, v2Transactions, p.setting.wallet)
	}

	return payoutVal
}
