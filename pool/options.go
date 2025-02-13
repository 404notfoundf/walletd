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

// WithPoolConfig sets pool config
func WithPoolConfig(config PoolConfig) Option {
	return func(p *Pool) {
		p.setting.wallet = MustParseAddress(config.Wallet)
		p.setting.port = config.Port
		p.setting.name = config.Name
	}
}
