package core

import (
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	bullsharktypes "github.com/vietchain/vniccss/pkg/bullshark/types"
	"github.com/vietchain/vniccss/pkg/narwhal/core"
	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

// Engine implements the Bullshark ordering algorithm
type Engine struct {
	logger   *zap.Logger
	mempool  *core.DAGMempool
	strategy *bullsharktypes.OrderingStrategy
}

// NewEngine creates a new ordering engine
func NewEngine(
	mempool *core.DAGMempool,
	strategy *bullsharktypes.OrderingStrategy,
	logger *zap.Logger,
) *Engine {
	if strategy == nil {
		strategy = bullsharktypes.DefaultOrderingStrategy()
	}

	return &Engine{
		logger:   logger,
		mempool:  mempool,
		strategy: strategy,
	}
}

// OrderCertificates orders certificates based on anchor certificates
func (e *Engine) OrderCertificates(
	anchors []*bullsharktypes.Anchor,
	round types.Round,
) ([]*bullsharktypes.OrderedCertificate, error) {
	if len(anchors) == 0 {
		return nil, bullsharktypes.ErrNoAnchorsSelected
	}

	// Validate all anchor certificates before ordering
	for i, anchor := range anchors {
		if err := e.validateAnchor(anchor); err != nil {
			return nil, fmt.Errorf("invalid anchor at index %d: %w", i, err)
		}
	}

	startTime := time.Now()
	e.logger.Debug("Starting certificate ordering",
		zap.Uint64("round", uint64(round)),
		zap.Int("anchor_count", len(anchors)),
	)

	// Get all certificates for the round and previous rounds that need ordering
	certificates := e.getCertificatesForOrdering(round)
	if len(certificates) == 0 {
		e.logger.Debug("No certificates to order",
			zap.Uint64("round", uint64(round)),
		)
		return nil, nil
	}

	// Perform topological ordering based on DAG structure and anchor selection
	orderedCerts, err := e.performTopologicalOrdering(certificates, anchors, round)
	if err != nil {
		return nil, fmt.Errorf("failed to perform topological ordering: %w", err)
	}

	processingTime := time.Since(startTime)
	e.logger.Info("Completed certificate ordering",
		zap.Uint64("round", uint64(round)),
		zap.Int("ordered_count", len(orderedCerts)),
		zap.Duration("processing_time", processingTime),
	)

	return orderedCerts, nil
}

// OrderBatches orders batches from the ordered certificates
func (e *Engine) OrderBatches(
	orderedCertificates []*bullsharktypes.OrderedCertificate,
	height bullsharktypes.BlockHeight,
) ([]*bullsharktypes.OrderedBatch, error) {
	e.logger.Debug("Ordering batches from certificates",
		zap.Int64("height", int64(height)),
		zap.Int("certificate_count", len(orderedCertificates)),
	)

	orderedBatches := make([]*bullsharktypes.OrderedBatch, 0)
	position := 0

	// Extract batches from certificates in order
	for _, orderedCert := range orderedCertificates {
		cert := orderedCert.Certificate

		// Get all batches referenced in this certificate's payload
		for _, batchHash := range cert.Header.Payload {
			batch, exists := e.mempool.GetBatch(batchHash)
			if !exists {
				e.logger.Warn("Batch not found in mempool",
					zap.String("batch_hash", batchHash.String()),
					zap.String("cert_hash", cert.Hash.String()),
				)
				continue
			}

			orderedBatch := &bullsharktypes.OrderedBatch{
				Batch:    batch,
				Height:   height,
				Position: position,
			}
			orderedBatches = append(orderedBatches, orderedBatch)
			position++
		}
	}

	// Apply additional ordering to batches if needed
	if e.strategy.Deterministic {
		orderedBatches = e.sortBatchesDeterministically(orderedBatches)
	}

	e.logger.Debug("Completed batch ordering",
		zap.Int("ordered_batch_count", len(orderedBatches)),
	)

	return orderedBatches, nil
}

// CreateOrderingResult creates a complete ordering result
func (e *Engine) CreateOrderingResult(
	anchors []*bullsharktypes.Anchor,
	round types.Round,
	height bullsharktypes.BlockHeight,
	previousBlockHash types.Hash,
) (*bullsharktypes.OrderingResult, error) {
	startTime := time.Now()

	// Order certificates
	orderedCerts, err := e.OrderCertificates(anchors, round)
	if err != nil {
		return nil, fmt.Errorf("failed to order certificates: %w", err)
	}

	// Order batches
	orderedBatches, err := e.OrderBatches(orderedCerts, height)
	if err != nil {
		return nil, fmt.Errorf("failed to order batches: %w", err)
	}

	// Extract all transactions in order
	orderedTransactions := e.extractOrderedTransactions(orderedBatches)

	// Create the block
	block := bullsharktypes.NewBullsharkBlock(height, previousBlockHash, round, anchors)

	// Add ordered content to block
	for i, orderedCert := range orderedCerts {
		block.AddOrderedCertificate(orderedCert.Certificate, i)
	}

	for i, orderedBatch := range orderedBatches {
		block.AddOrderedBatch(orderedBatch.Batch, i)
	}

	processingTime := time.Since(startTime)

	result := &bullsharktypes.OrderingResult{
		Block:               block,
		OrderedCertificates: orderedCerts,
		OrderedBatches:      orderedBatches,
		OrderedTransactions: orderedTransactions,
		AnchorRound:         round,
		ProcessingTime:      processingTime,
	}

	e.logger.Info("Created ordering result",
		zap.Int64("height", int64(height)),
		zap.Uint64("round", uint64(round)),
		zap.Int("certificates", len(orderedCerts)),
		zap.Int("batches", len(orderedBatches)),
		zap.Int("transactions", len(orderedTransactions)),
		zap.Duration("processing_time", processingTime),
	)

	return result, nil
}

// getCertificatesForOrdering gets certificates that need to be ordered
func (e *Engine) getCertificatesForOrdering(round types.Round) []*types.Certificate {
	// For simplicity, we'll order certificates from the current round
	// In a full implementation, we might include certificates from previous rounds
	// that haven't been ordered yet
	certificates := e.mempool.GetCertificatesByRound(round)

	e.logger.Debug("Retrieved certificates for ordering",
		zap.Uint64("round", uint64(round)),
		zap.Int("certificate_count", len(certificates)),
	)

	return certificates
}

// performTopologicalOrdering performs topological ordering based on DAG structure
func (e *Engine) performTopologicalOrdering(
	certificates []*types.Certificate,
	anchors []*bullsharktypes.Anchor,
	round types.Round,
) ([]*bullsharktypes.OrderedCertificate, error) {
	// Create anchor map for quick lookup
	anchorMap := make(map[types.Hash]*bullsharktypes.Anchor)
	for _, anchor := range anchors {
		anchorMap[anchor.Certificate.Hash] = anchor
	}

	// Sort certificates according to ordering strategy
	sortedCerts := e.sortCertificates(certificates, anchorMap)

	// Create ordered certificates
	orderedCerts := make([]*bullsharktypes.OrderedCertificate, 0, len(sortedCerts))
	for i, cert := range sortedCerts {
		orderedCert := &bullsharktypes.OrderedCertificate{
			Certificate: cert,
			Height:      0, // Will be set by caller
			Position:    i,
			AnchorRound: round,
		}
		orderedCerts = append(orderedCerts, orderedCert)
	}

	return orderedCerts, nil
}

// sortCertificates sorts certificates according to the ordering strategy
func (e *Engine) sortCertificates(
	certificates []*types.Certificate,
	anchorMap map[types.Hash]*bullsharktypes.Anchor,
) []*types.Certificate {
	sorted := make([]*types.Certificate, len(certificates))
	copy(sorted, certificates)

	// Primary sorting by the primary rule
	e.applySortingRule(sorted, e.strategy.PrimaryRule, anchorMap)

	// If we need deterministic ordering, apply secondary rule for ties
	if e.strategy.Deterministic {
		// For simplicity, we'll use a stable sort with secondary rule
		// In a full implementation, we'd handle ties more carefully
		e.applySortingRule(sorted, e.strategy.SecondaryRule, anchorMap)
	}

	return sorted
}

// applySortingRule applies a specific sorting rule to certificates
func (e *Engine) applySortingRule(
	certificates []*types.Certificate,
	rule bullsharktypes.OrderingRule,
	anchorMap map[types.Hash]*bullsharktypes.Anchor,
) {
	switch rule {
	case bullsharktypes.OrderByTimestamp:
		sort.SliceStable(certificates, func(i, j int) bool {
			return certificates[i].Header.Timestamp.Before(certificates[j].Header.Timestamp)
		})
	case bullsharktypes.OrderByHash:
		sort.SliceStable(certificates, func(i, j int) bool {
			return certificates[i].Hash.String() < certificates[j].Hash.String()
		})
	case bullsharktypes.OrderByAuthor:
		sort.SliceStable(certificates, func(i, j int) bool {
			return certificates[i].Header.Author < certificates[j].Header.Author
		})
	case bullsharktypes.OrderByRound:
		sort.SliceStable(certificates, func(i, j int) bool {
			return certificates[i].Header.Round < certificates[j].Header.Round
		})
	}

	// Prioritize anchor certificates
	sort.SliceStable(certificates, func(i, j int) bool {
		isAnchorI := anchorMap[certificates[i].Hash] != nil
		isAnchorJ := anchorMap[certificates[j].Hash] != nil

		// Anchors come first
		if isAnchorI && !isAnchorJ {
			return true
		}
		if !isAnchorI && isAnchorJ {
			return false
		}

		// If both are anchors, sort by anchor index
		if isAnchorI && isAnchorJ {
			anchorI := anchorMap[certificates[i].Hash]
			anchorJ := anchorMap[certificates[j].Hash]
			return anchorI.AnchorIndex < anchorJ.AnchorIndex
		}

		// Neither is anchor, maintain existing order
		return false
	})
}

// sortBatchesDeterministically sorts batches for deterministic ordering
func (e *Engine) sortBatchesDeterministically(batches []*bullsharktypes.OrderedBatch) []*bullsharktypes.OrderedBatch {
	sorted := make([]*bullsharktypes.OrderedBatch, len(batches))
	copy(sorted, batches)

	// Sort by batch ID for deterministic ordering
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Batch.ID.String() < sorted[j].Batch.ID.String()
	})

	// Update positions after sorting
	for i, batch := range sorted {
		batch.Position = i
	}

	return sorted
}

// extractOrderedTransactions extracts all transactions from ordered batches
func (e *Engine) extractOrderedTransactions(orderedBatches []*bullsharktypes.OrderedBatch) []*types.Transaction {
	transactions := make([]*types.Transaction, 0)

	for _, orderedBatch := range orderedBatches {
		transactions = append(transactions, orderedBatch.Batch.Transactions...)
	}

	e.logger.Debug("Extracted transactions from batches",
		zap.Int("transaction_count", len(transactions)),
		zap.Int("batch_count", len(orderedBatches)),
	)

	return transactions
}

// GetOrderingStrategy returns the current ordering strategy
func (e *Engine) GetOrderingStrategy() *bullsharktypes.OrderingStrategy {
	return e.strategy
}

// UpdateOrderingStrategy updates the ordering strategy
func (e *Engine) UpdateOrderingStrategy(strategy *bullsharktypes.OrderingStrategy) {
	e.strategy = strategy
	e.logger.Info("Updated ordering strategy",
		zap.Bool("deterministic", strategy.Deterministic),
	)
}

// validateAnchor validates an anchor certificate before using it for ordering
func (e *Engine) validateAnchor(anchor *bullsharktypes.Anchor) error {
	if anchor == nil {
		return fmt.Errorf("anchor is nil")
	}

	if anchor.Certificate == nil {
		return fmt.Errorf("anchor certificate is nil")
	}

	cert := anchor.Certificate

	// Validate certificate structure
	if cert.Header == nil {
		return fmt.Errorf("certificate header is nil")
	}

	// Validate certificate hash
	expectedHash := cert.Header.Hash()
	if cert.Hash != expectedHash {
		return fmt.Errorf("certificate hash mismatch: expected %s, got %s",
			expectedHash.String(), cert.Hash.String())
	}

	// Validate signature exists
	if len(cert.Signature) == 0 {
		return fmt.Errorf("certificate has no signature")
	}

	// Validate round consistency
	if cert.Header.Round != anchor.Round {
		return fmt.Errorf("anchor round mismatch: certificate round %d, anchor round %d",
			cert.Header.Round, anchor.Round)
	}

	// Validate anchor is marked as selected
	if !anchor.Selected {
		return fmt.Errorf("anchor is not marked as selected")
	}

	// Verify the certificate exists in mempool
	if _, exists := e.mempool.GetCertificate(cert.Hash); !exists {
		return fmt.Errorf("anchor certificate not found in mempool: %s", cert.Hash.String())
	}

	e.logger.Debug("Anchor validation passed",
		zap.String("cert_hash", cert.Hash.String()),
		zap.Uint64("round", uint64(anchor.Round)),
		zap.Int("anchor_index", anchor.AnchorIndex),
	)

	return nil
}

// GetStats returns statistics about the ordering engine
func (e *Engine) GetStats() map[string]interface{} {
	currentRound := e.mempool.GetCurrentRound()
	certificates := e.getCertificatesForOrdering(currentRound)

	return map[string]interface{}{
		"current_round":   uint64(currentRound),
		"available_certs": len(certificates),
		"deterministic":   e.strategy.Deterministic,
		"primary_rule":    int(e.strategy.PrimaryRule),
		"secondary_rule":  int(e.strategy.SecondaryRule),
	}
}
