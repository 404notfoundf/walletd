package pool

import (
	"go.sia.tech/core/types"
	"sync"
)

type persistence struct {
	mu          sync.RWMutex
	BlockHeight uint64
	Target      types.BlockID
}

func (p *persistence) GetBlockHeight() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.BlockHeight
}

func (p *persistence) SetBlockHeight(blockHeight uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.BlockHeight = blockHeight
}

func (p *persistence) GetTarget() types.BlockID {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Target
}

func (p *persistence) SetTarget(target types.BlockID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Target = target
}

/*
func (p *Pool) setPoolSettings(initConfig PoolConfig) error {
	p.log.Debug("set pool settings called")

	poolWallet, err := types.ParseAddress(initConfig.Wallet)
	if err != nil {
		return fmt.Errorf("failed to parse address to wallet, err: %s", err.Error())
	}
	interSettings := PoolInternalSettings{
		name:   initConfig.Name,
		wallet: poolWallet,
	}

	p.persist.SetSettings(interSettings)
	return nil
}

func (ps *persistence) SetSettings(settings PoolInternalSettings) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.Settings = settings
}

func (ps *persistence) GetSettings() PoolInternalSettings {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.Settings
}
*/
