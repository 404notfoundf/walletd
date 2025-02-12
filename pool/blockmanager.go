package pool

import (
	"fmt"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"time"
)

func (p *Pool) buildBlockForWork(isForce bool) types.Block {
	if !isForce && time.Now().Before(p.sourceBlock.Timestamp.Add(10*time.Second)) {
		return p.sourceBlock
	}

	// build block
	var block types.Block

	block.Timestamp = types.CurrentTimestamp()

	tipState := p.cm.TipState()

	block.Transactions = p.cm.PoolTransactions()

	v2Transactions := p.cm.V2PoolTransactions()

	payoutVal := CalculateSubsidy(tipState, block.Transactions, v2Transactions)

	block.MinerPayouts = []types.SiacoinOutput{{Address: p.setting.wallet, Value: payoutVal}}

	if len(v2Transactions) > 0 {
		block.V2 = ConstructV2BlockData(tipState, block.Transactions, v2Transactions, p.setting.wallet)
	}

	return block
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
