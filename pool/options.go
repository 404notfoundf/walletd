package pool

import "go.uber.org/zap"

// An Option configures a wallet Manager.
type Option func(*Pool)

// WithLogger sets the logger used by the manager.
func WithLogger(log *zap.Logger) Option {
	return func(p *Pool) {
		p.log = log
	}
}

// WithSyncBatchSize sets the number of blocks to batch when scanning
// the blockchain. The default is 64. Increasing this value can
// improve performance at the cost of memory usage.
func WithSyncBatchSize(size int) Option {
	return func(p *Pool) {
		p.setting.syncBatchSize = size
	}
}

// WithPoolConfig sets pool config
func WithPoolConfig(config PoolConfig) Option {
	return func(p *Pool) {
		p.setting.wallet = MustParseAddress(config.Wallet)
		p.setting.port = config.Port
		p.setting.name = config.Name
		p.setting.syncBatchSize = config.BatchSize
	}
}
