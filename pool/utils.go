package pool

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/siad/crypto"
	"io"
	"math"
	"math/big"
	"sort"
	"time"
	"unsafe"
)

// BigToCompact converts a whole number N to a compact representation using
// an unsigned 32-bit number.  The compact representation only provides 23 bits
// of precision, so values larger than (2^23 - 1) only encode the most
// significant digits of the number.  See CompactToBig for details.
func BigToCompact(n *big.Int) uint32 {
	// No need to do any work if it's zero.
	if n.Sign() == 0 {
		return 0
	}

	// Since the base for the exponent is 256, the exponent can be treated
	// as the number of bytes.  So, shift the number right or left
	// accordingly.  This is equivalent to:
	// mantissa = mantissa / 256^(exponent-3)
	var mantissa uint32
	exponent := uint(len(n.Bytes()))
	if exponent <= 3 {
		mantissa = uint32(n.Bits()[0])
		mantissa <<= 8 * (3 - exponent)
	} else {
		// Use a copy to avoid modifying the caller's original number.
		tn := new(big.Int).Set(n)
		mantissa = uint32(tn.Rsh(tn, 8*(exponent-3)).Bits()[0])
	}

	// When the mantissa already has the sign bit set, the number is too
	// large to fit into the available 23-bits, so divide the number by 256
	// and increment the exponent accordingly.
	if mantissa&0x00800000 != 0 {
		mantissa >>= 8
		exponent++
	}

	// Pack the exponent, sign bit, and mantissa into an unsigned 32-bit
	// int and return it.
	compact := uint32(exponent<<24) | mantissa
	if n.Sign() < 0 {
		compact |= 0x00800000
	}
	return compact
}

// BlockIdToBigInt block id type transfer to big.Int
func BlockIdToBigInt(id types.BlockID) *big.Int {
	return new(big.Int).SetBytes(id[:])
}

// EncUint64 encodes a uint64 as a slice of 8 bytes.
func EncUint64(i uint64) (b []byte) {
	b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return
}

func WriteUint64(w io.Writer, u uint64) error {
	_, err := w.Write(EncUint64(u))
	return err
}

func CalculateSubsidy(cs consensus.State, transactions []types.Transaction, v2Transactions []types.V2Transaction) types.Currency {
	subsidy := cs.BlockReward()
	// v1 transactions
	for _, txn := range transactions {
		subsidy = subsidy.Add(txn.TotalFees())
	}

	// v2 transaction
	for _, txn := range v2Transactions {
		subsidy = subsidy.Add(txn.MinerFee)
	}
	return subsidy
}

func ConstructV2BlockData(cs consensus.State, transactions []types.Transaction, v2Transactions []types.V2Transaction, minerAddress types.Address) *types.V2BlockData {
	blockData := &types.V2BlockData{
		Height:       cs.Index.Height,
		Transactions: v2Transactions,
	}
	blockData.Commitment = cs.Commitment(cs.TransactionsCommitment(transactions, v2Transactions), minerAddress)
	return blockData
}

// MustParseAddress must parse string to types.Address
func MustParseAddress(s string) types.Address {
	addr, err := types.ParseAddress(s)
	if err != nil {
		panic(err)
	}
	return addr
}

func MarshalSiaArbDataNoSignatures(t types.Transaction, w io.Writer) {
	encoder := types.NewEncoder(w)
	t.EncodeTo(encoder)
	encoder.WriteUint64(uint64(len(t.SiacoinInputs)))
	encoder.WriteUint64(uint64(len(t.SiacoinOutputs)))
	encoder.WriteUint64(uint64(len(t.FileContracts)))
	encoder.WriteUint64(uint64(len(t.FileContractRevisions)))
	encoder.WriteUint64(uint64(len(t.StorageProofs)))
	encoder.WriteUint64(uint64(len(t.SiafundInputs)))
	encoder.WriteUint64(uint64(len(t.SiafundOutputs)))
	encoder.WriteUint64(uint64(len(t.MinerFees)))

	encoder.WriteUint64(uint64(len(t.ArbitraryData)))
	for i := range t.ArbitraryData {
		encoder.WriteBytes(t.ArbitraryData[i])
	}
	encoder.Flush()
}

func IsSynced(state consensus.State) bool {
	if state.Index.Height == 0 {
		return false
	}

	return time.Now().After(MedianTimestamp(state))
}

func lastBlockLessThanTwoHourAgo(preTimestamp [11]time.Time) bool {
	lastBlockTime := preTimestamp[0]
	if lastBlockTime.Add(time.Hour * 2).Before(time.Now()) {
		return false
	}
	return true
}

func MedianTimestamp(state consensus.State) time.Time {
	prevCopy := state.PrevTimestamps

	num := int(math.Min(float64(state.Index.Height+1), float64(len(state.PrevTimestamps))))

	ts := prevCopy[:num]
	sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
	if len(ts)%2 != 0 {
		return ts[len(ts)/2]
	}
	l, r := ts[len(ts)/2-1], ts[len(ts)/2]
	return l.Add(r.Sub(l) / 2)
}

// ReadMerkleBranches returns the merkle branches of a block, as used in stratum
// mining.
func ReadMerkleBranches(block types.Block) []string {
	mbranch := crypto.NewTree()
	var buf bytes.Buffer
	encoder := types.NewEncoder(&buf)
	for _, payout := range block.MinerPayouts {
		types.V1SiacoinOutput(payout).EncodeTo(encoder)
		encoder.Flush()
		mbranch.Push(buf.Bytes())
		buf.Reset()
	}

	for _, txn := range block.Transactions {
		txn.EncodeTo(encoder)
		encoder.Flush()
		mbranch.Push(buf.Bytes())
		buf.Reset()
	}

	for _, txn := range block.V2Transactions() {
		txn.EncodeTo(encoder)
		encoder.Flush()
		mbranch.Push(buf.Bytes())
		buf.Reset()
	}

	//
	// This whole approach needs to be revisited.  I basically am cheating to look
	// inside the merkle tree struct to determine if the head is a leaf or not
	//

	//SubTree here is redefined to read the sum field
	type SubTree struct {
		height int
		sum    [32]byte
	}

	// Tree here is redefined from
	// NebulousLabs\merkletree@v0.0.0-20200118113624-07fbf710afc4\merkletree-blake\tree.go
	// to access the merkle branches stored in the stack
	type Tree struct {
		stack        []SubTree
		currentIndex uint64
		proofIndex   uint64
		proofBase    []byte
		proofSet     [][32]byte
		proofTree    bool
		cachedTree   bool
	}

	tr := *(*Tree)(unsafe.Pointer(mbranch))

	var merkle []string

	for i := len(tr.stack) - 1; i >= 0; i-- {
		merkle = append(merkle, hex.EncodeToString(tr.stack[i].sum[:]))
	}
	return merkle
}
