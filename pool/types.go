package pool

import (
	"crypto"
	"errors"
	"go.sia.tech/core/types"
)

const (
	// MajorVersion is the significant version of the pool module
	MajorVersion = 0
	// MinorVersion is the minor version of the pool module
	MinorVersion = 3
)

var (
	PrefixNonSia = types.NewSpecifier("NonSia")

	ErrNilChainManager = errors.New("pool cannot use a nil chain manager")

	ErrNilSyncer = errors.New("pool cannot use a nil syncer")
)

type (
	PoolConfig struct {
		Name   string `yaml:"name"`
		Wallet string `yaml:"wallet"`
		Port   string `yaml:"port"`
		// PoolLogDir string
	}

	PoolInternalSettings struct {
		name   string
		wallet types.Address
		port   string
	}

	ConsensusNotify struct {
		Target    types.BlockID `json:"target"`
		Height    uint64        `json:"height"`
		Block     types.Block   `json:"block"`
		Coinbase1 string        `json:"coinbase_1"`
		Coinbase2 string        `json:"coinbase_2"`
		Merkle    []string      `json:"merkle"`
		Nbits     string        `json:"nbits"`
		Ntime     string        `json:"ntime"`
		Error     string        `json:"error"`
	}

	HandleSubmitResponse struct {
		Message string `json:"message"`
	}

	Template struct {
		Block          types.Block `json:"block"`
		MerkleRoot     crypto.Hash `json:"merkle_root"`
		ParentId       string      `json:"parent_id"`
		CoinB1         string      `json:"coinbase_1"`
		CoinB2         string      `json:"coinbase_2"`
		MerkleBranches []string    `json:"merkle"`
		Nbits          string      `json:"nbits"`
		Ntimes         string      `json:"ntime"`
	}

	Error struct {
		Message string `json:"message"`
	}
)
