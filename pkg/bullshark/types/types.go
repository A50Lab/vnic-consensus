package types

import (
	"time"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

// BlockHeight represents the height of a block in the blockchain
type BlockHeight int64

// Anchor represents an anchor certificate that determines ordering
type Anchor struct {
	Certificate   *types.Certificate
	Round         types.Round
	Height        BlockHeight
	AnchorIndex   int  // Index of this anchor in the round
	Selected      bool // Whether this anchor was selected for ordering
	SelectionTime time.Time
}

// OrderedCertificate represents a certificate with its ordering information
type OrderedCertificate struct {
	Certificate *types.Certificate
	Height      BlockHeight
	Position    int // Position within the block
	AnchorRound types.Round
}

// OrderedBatch represents a batch with its ordering information
type OrderedBatch struct {
	Batch    *types.Batch
	Height   BlockHeight
	Position int // Position within the block
}

// BullsharkBlock represents a finalized block in Bullshark consensus
type BullsharkBlock struct {
	Height            BlockHeight
	PreviousBlockHash types.Hash
	BlockHash         types.Hash
	Timestamp         time.Time

	// Ordered content
	OrderedCertificates []*OrderedCertificate
	OrderedBatches      []*OrderedBatch
	OrderedTransactions []*types.Transaction

	// Consensus information
	AnchorRound     types.Round
	SelectedAnchors []*Anchor

	// Block metadata
	TransactionCount int
	BatchCount       int
	CertificateCount int
}

// NewBullsharkBlock creates a new Bullshark block
func NewBullsharkBlock(
	height BlockHeight,
	previousBlockHash types.Hash,
	anchorRound types.Round,
	anchors []*Anchor,
) *BullsharkBlock {
	timestamp := time.Now()

	// Create a deterministic block hash from block metadata
	// This will be updated with transaction data when the block is finalized
	blockHash := computeBlockHash(height, previousBlockHash, timestamp, anchorRound)

	return &BullsharkBlock{
		Height:              height,
		PreviousBlockHash:   previousBlockHash,
		BlockHash:           blockHash,
		Timestamp:           timestamp,
		OrderedCertificates: make([]*OrderedCertificate, 0),
		OrderedBatches:      make([]*OrderedBatch, 0),
		OrderedTransactions: make([]*types.Transaction, 0),
		AnchorRound:         anchorRound,
		SelectedAnchors:     anchors,
		TransactionCount:    0,
		BatchCount:          0,
		CertificateCount:    0,
	}
}

// computeBlockHash computes a deterministic block hash
func computeBlockHash(height BlockHeight, previousBlockHash types.Hash, timestamp time.Time, round types.Round) types.Hash {
	// Create deterministic hash from block components
	hashData := make([]byte, 0)

	// Add height (8 bytes)
	heightBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		heightBytes[i] = byte(height >> (8 * (7 - i)))
	}
	hashData = append(hashData, heightBytes...)

	// Add previous block hash
	hashData = append(hashData, previousBlockHash.Bytes()...)

	// Add timestamp (deterministic format)
	timestampBytes := []byte(timestamp.UTC().Format("2006-01-02T15:04:05.000000000Z"))
	hashData = append(hashData, timestampBytes...)

	// Add round
	roundBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		roundBytes[i] = byte(round >> (8 * (7 - i)))
	}
	hashData = append(hashData, roundBytes...)

	return types.NewHash(hashData)
}

// AddOrderedCertificate adds an ordered certificate to the block
func (b *BullsharkBlock) AddOrderedCertificate(cert *types.Certificate, position int) {
	orderedCert := &OrderedCertificate{
		Certificate: cert,
		Height:      b.Height,
		Position:    position,
		AnchorRound: b.AnchorRound,
	}
	b.OrderedCertificates = append(b.OrderedCertificates, orderedCert)
	b.CertificateCount++
}

// AddOrderedBatch adds an ordered batch to the block
func (b *BullsharkBlock) AddOrderedBatch(batch *types.Batch, position int) {
	orderedBatch := &OrderedBatch{
		Batch:    batch,
		Height:   b.Height,
		Position: position,
	}
	b.OrderedBatches = append(b.OrderedBatches, orderedBatch)
	b.BatchCount++

	// Add transactions from the batch
	for _, tx := range batch.Transactions {
		b.OrderedTransactions = append(b.OrderedTransactions, tx)
		b.TransactionCount++
	}
}

// GetTransactions returns all transactions in the block in order
func (b *BullsharkBlock) GetTransactions() []*types.Transaction {
	return b.OrderedTransactions
}

// Validate validates the block structure
func (b *BullsharkBlock) Validate() error {
	// Basic validation checks
	if b.Height < 0 {
		return ErrInvalidBlockHeight
	}

	if len(b.OrderedTransactions) != b.TransactionCount {
		return ErrInconsistentTransactionCount
	}

	if len(b.OrderedBatches) != b.BatchCount {
		return ErrInconsistentBatchCount
	}

	if len(b.OrderedCertificates) != b.CertificateCount {
		return ErrInconsistentCertificateCount
	}

	return nil
}

// UpdateBlockHashWithTransactions updates the block hash to include transaction data
func (b *BullsharkBlock) UpdateBlockHashWithTransactions(txs [][]byte) {
	// Compute transaction list hash
	txListHash := computeTransactionListHash(txs)

	// Recompute block hash including transaction data
	hashData := make([]byte, 0)

	// Add original block components
	heightBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		heightBytes[i] = byte(b.Height >> (8 * (7 - i)))
	}
	hashData = append(hashData, heightBytes...)
	hashData = append(hashData, b.PreviousBlockHash.Bytes()...)

	timestampBytes := []byte(b.Timestamp.UTC().Format("2006-01-02T15:04:05.000000000Z"))
	hashData = append(hashData, timestampBytes...)

	roundBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		roundBytes[i] = byte(b.AnchorRound >> (8 * (7 - i)))
	}
	hashData = append(hashData, roundBytes...)

	// Add transaction list hash
	hashData = append(hashData, txListHash.Bytes()...)

	// Update block hash
	b.BlockHash = types.NewHash(hashData)
}

// computeTransactionListHash computes a hash of the transaction list
func computeTransactionListHash(txs [][]byte) types.Hash {
	if len(txs) == 0 {
		return types.NewHash([]byte("empty_tx_list"))
	}

	// Concatenate all transaction data
	allTxData := make([]byte, 0)
	for _, tx := range txs {
		// Add transaction length (4 bytes) + transaction data
		txLen := len(tx)
		lenBytes := make([]byte, 4)
		for i := 0; i < 4; i++ {
			lenBytes[i] = byte(txLen >> (8 * (3 - i)))
		}
		allTxData = append(allTxData, lenBytes...)
		allTxData = append(allTxData, tx...)
	}

	return types.NewHash(allTxData)
}

// OrderingRule defines how certificates should be ordered
type OrderingRule int

const (
	OrderByTimestamp OrderingRule = iota
	OrderByHash
	OrderByAuthor
	OrderByRound
)

// OrderingStrategy defines the strategy for ordering certificates and batches
type OrderingStrategy struct {
	PrimaryRule   OrderingRule
	SecondaryRule OrderingRule
	Deterministic bool // Whether ordering should be deterministic
}

// DefaultOrderingStrategy returns the default ordering strategy for Bullshark
func DefaultOrderingStrategy() *OrderingStrategy {
	return &OrderingStrategy{
		PrimaryRule:   OrderByRound,
		SecondaryRule: OrderByHash,
		Deterministic: true,
	}
}

// AnchorSelectionStrategy defines how anchors are selected in each round
type AnchorSelectionStrategy struct {
	MaxAnchorsPerRound int
	SelectionRule      OrderingRule
	RequireQuorum      bool
	QuorumThreshold    float64 // Fraction of total validators required
}

// DefaultAnchorSelectionStrategy returns the default anchor selection strategy
func DefaultAnchorSelectionStrategy() *AnchorSelectionStrategy {
	return &AnchorSelectionStrategy{
		MaxAnchorsPerRound: 1, // Single anchor per round for simplicity
		SelectionRule:      OrderByTimestamp,
		RequireQuorum:      false, // For single node setup
		QuorumThreshold:    0.67,
	}
}

// OrderingResult represents the result of ordering certificates for a block
type OrderingResult struct {
	Block               *BullsharkBlock
	OrderedCertificates []*OrderedCertificate
	OrderedBatches      []*OrderedBatch
	OrderedTransactions []*types.Transaction
	AnchorRound         types.Round
	ProcessingTime      time.Duration
}

// ConsensusState represents the current state of Bullshark consensus
type ConsensusState struct {
	CurrentHeight    BlockHeight
	LastBlockHash    types.Hash
	LastBlock        *BullsharkBlock
	CurrentRound     types.Round
	PendingAnchors   map[types.Round][]*Anchor
	CommittedBlocks  map[BlockHeight]*BullsharkBlock
	OrderingStrategy *OrderingStrategy
	AnchorStrategy   *AnchorSelectionStrategy
}

// NewConsensusState creates a new consensus state
func NewConsensusState() *ConsensusState {
	genesisHash := types.NewHash([]byte("genesis"))

	return &ConsensusState{
		CurrentHeight:    0,
		LastBlockHash:    genesisHash,
		LastBlock:        nil,
		CurrentRound:     0,
		PendingAnchors:   make(map[types.Round][]*Anchor),
		CommittedBlocks:  make(map[BlockHeight]*BullsharkBlock),
		OrderingStrategy: DefaultOrderingStrategy(),
		AnchorStrategy:   DefaultAnchorSelectionStrategy(),
	}
}

// UpdateState updates the consensus state with a new block
func (cs *ConsensusState) UpdateState(block *BullsharkBlock) {
	cs.CurrentHeight = block.Height + 1
	cs.LastBlockHash = block.BlockHash
	cs.LastBlock = block
	cs.CommittedBlocks[block.Height] = block

	// Update current round based on the block's anchor round
	if block.AnchorRound > cs.CurrentRound {
		cs.CurrentRound = block.AnchorRound
	}
}

// GetBlock retrieves a block by height
func (cs *ConsensusState) GetBlock(height BlockHeight) (*BullsharkBlock, bool) {
	block, exists := cs.CommittedBlocks[height]
	return block, exists
}

// AddPendingAnchor adds a pending anchor for a round
func (cs *ConsensusState) AddPendingAnchor(anchor *Anchor) {
	if cs.PendingAnchors[anchor.Round] == nil {
		cs.PendingAnchors[anchor.Round] = make([]*Anchor, 0)
	}
	cs.PendingAnchors[anchor.Round] = append(cs.PendingAnchors[anchor.Round], anchor)
}

// GetPendingAnchors returns pending anchors for a round
func (cs *ConsensusState) GetPendingAnchors(round types.Round) []*Anchor {
	return cs.PendingAnchors[round]
}

// RemovePendingAnchors removes pending anchors for a round (after they're processed)
func (cs *ConsensusState) RemovePendingAnchors(round types.Round) {
	delete(cs.PendingAnchors, round)
}

// Error definitions
type BullsharkError struct {
	Code    int
	Message string
}

func (e *BullsharkError) Error() string {
	return e.Message
}

var (
	ErrInvalidBlockHeight           = &BullsharkError{1001, "invalid block height"}
	ErrInconsistentTransactionCount = &BullsharkError{1002, "inconsistent transaction count"}
	ErrInconsistentBatchCount       = &BullsharkError{1003, "inconsistent batch count"}
	ErrInconsistentCertificateCount = &BullsharkError{1004, "inconsistent certificate count"}
	ErrNoAnchorsSelected            = &BullsharkError{1005, "no anchors selected for round"}
	ErrInvalidAnchorSelection       = &BullsharkError{1006, "invalid anchor selection"}
	ErrOrderingFailed               = &BullsharkError{1007, "failed to order certificates"}
	ErrBlockCreationFailed          = &BullsharkError{1008, "failed to create block"}
	ErrInsufficientCertificates     = &BullsharkError{1009, "insufficient certificates for block creation"}
)
