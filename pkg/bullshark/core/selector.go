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

// Selector handles the selection of anchor certificates for ordering
type Selector struct {
	logger   *zap.Logger
	mempool  *core.DAGMempool
	strategy *bullsharktypes.AnchorSelectionStrategy
}

// NewSelector creates a new anchor selector
func NewSelector(
	mempool *core.DAGMempool,
	strategy *bullsharktypes.AnchorSelectionStrategy,
	logger *zap.Logger,
) *Selector {
	if strategy == nil {
		strategy = bullsharktypes.DefaultAnchorSelectionStrategy()
	}

	return &Selector{
		logger:   logger,
		mempool:  mempool,
		strategy: strategy,
	}
}

// NewSelectorWithShoal creates a new anchor selector with Shoal framework integration
func NewSelectorWithShoal(
	mempool *core.DAGMempool,
	strategy *bullsharktypes.AnchorSelectionStrategy,
	logger *zap.Logger,
) *Selector {
	if strategy == nil {
		strategy = bullsharktypes.DefaultAnchorSelectionStrategy()
	}

	return &Selector{
		logger:   logger,
		mempool:  mempool,
		strategy: strategy,
	}
}

// SelectAnchors selects anchor certificates for a given round
func (s *Selector) SelectAnchors(round types.Round) ([]*bullsharktypes.Anchor, error) {
	start := time.Now()
	s.logger.Debug("Selecting anchors for round",
		zap.Uint64("round", uint64(round)),
		zap.Int("max_anchors", s.strategy.MaxAnchorsPerRound),
	)

	// Get all certificates for the round
	certificates := s.mempool.GetCertificatesByRound(round)
	if len(certificates) == 0 {
		s.logger.Debug("No certificates available for anchor selection",
			zap.Uint64("round", uint64(round)),
		)
		return nil, nil
	}

	s.logger.Debug("Found certificates for anchor selection",
		zap.Uint64("round", uint64(round)),
		zap.Int("certificate_count", len(certificates)),
	)

	var sortedCerts []*types.Certificate

	sortedCerts = s.sortCertificatesForSelection(certificates)

	// Select anchors based on strategy
	selectedCount := s.strategy.MaxAnchorsPerRound
	if selectedCount > len(sortedCerts) {
		selectedCount = len(sortedCerts)
	}

	anchors := make([]*bullsharktypes.Anchor, 0, selectedCount)
	for i := 0; i < selectedCount; i++ {
		anchor := &bullsharktypes.Anchor{
			Certificate:   sortedCerts[i],
			Round:         round,
			Height:        0, // Will be set when block is created
			AnchorIndex:   i,
			Selected:      true,
			SelectionTime: time.Now(),
		}
		anchors = append(anchors, anchor)
	}

	s.logger.Info("Selected anchors for round",
		zap.Uint64("round", uint64(round)),
		zap.Int("selected_count", len(anchors)),
		zap.Duration("selection_time", time.Since(start)),
	)

	return anchors, nil
}

// SelectAnchorsForHeight selects anchors for a specific block height
func (s *Selector) SelectAnchorsForHeight(
	height bullsharktypes.BlockHeight,
	round types.Round,
) ([]*bullsharktypes.Anchor, error) {
	anchors, err := s.SelectAnchors(round)
	if err != nil {
		return nil, err
	}

	// Update height information for selected anchors
	for _, anchor := range anchors {
		anchor.Height = height
	}

	return anchors, nil
}

// CanCreateBlock determines if enough anchors are available to create a block
func (s *Selector) CanCreateBlock(round types.Round) bool {
	// Get available certificates for this round
	certificates := s.mempool.GetCertificatesByRound(round)

	// Filter for valid anchor candidates
	validCandidates := 0
	for _, cert := range certificates {
		if s.isValidAnchorCandidate(cert) {
			validCandidates++
		}
	}

	// If we require a quorum and don't have enough valid certificates, return false
	quorumThreshold := int(s.strategy.QuorumThreshold) // Convert to int for comparison
	if s.strategy.RequireQuorum && validCandidates < quorumThreshold {
		s.logger.Debug("Insufficient valid certificates for quorum",
			zap.Uint64("round", uint64(round)),
			zap.Int("valid_candidates", validCandidates),
			zap.Int("quorum_threshold", quorumThreshold),
		)
		return false
	}

	// Always allow block creation for continuous progress, even with empty blocks
	// This ensures the system doesn't stall
	return true
}

// GetAnchorCandidates returns all potential anchor candidates for a round
func (s *Selector) GetAnchorCandidates(round types.Round) []*types.Certificate {
	certificates := s.mempool.GetCertificatesByRound(round)

	// Filter certificates that are valid anchor candidates
	candidates := make([]*types.Certificate, 0)
	for _, cert := range certificates {
		if s.isValidAnchorCandidate(cert) {
			candidates = append(candidates, cert)
		}
	}

	return candidates
}

// WaitForAnchors waits for sufficient anchors to be available for a round
func (s *Selector) WaitForAnchors(round types.Round, timeout time.Duration) ([]*bullsharktypes.Anchor, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if s.CanCreateBlock(round) {
			return s.SelectAnchors(round)
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("timeout waiting for anchors in round %d", round)
}

// sortCertificatesForSelection sorts certificates according to the selection rule
func (s *Selector) sortCertificatesForSelection(certs []*types.Certificate) []*types.Certificate {
	sorted := make([]*types.Certificate, len(certs))
	copy(sorted, certs)

	switch s.strategy.SelectionRule {
	case bullsharktypes.OrderByTimestamp:
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Header.Timestamp.Before(sorted[j].Header.Timestamp)
		})
	case bullsharktypes.OrderByHash:
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Hash.String() < sorted[j].Hash.String()
		})
	case bullsharktypes.OrderByAuthor:
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Header.Author < sorted[j].Header.Author
		})
	case bullsharktypes.OrderByRound:
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Header.Round < sorted[j].Header.Round
		})
	default:
		// Default to timestamp ordering
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Header.Timestamp.Before(sorted[j].Header.Timestamp)
		})
	}

	return sorted
}

// isValidAnchorCandidate checks if a certificate can be used as an anchor
func (s *Selector) isValidAnchorCandidate(cert *types.Certificate) bool {
	// Basic validation - certificate should have proper structure
	if cert == nil || cert.Header == nil {
		return false
	}

	// Check if certificate has proper signatures
	if len(cert.Signature) == 0 {
		return false
	}

	// For single node setup, any valid certificate can be an anchor
	// In multi-node setup, we would check additional consensus requirements

	return true
}

// GetSelectionStrategy returns the current selection strategy
func (s *Selector) GetSelectionStrategy() *bullsharktypes.AnchorSelectionStrategy {
	return s.strategy
}

// UpdateSelectionStrategy updates the selection strategy
func (s *Selector) UpdateSelectionStrategy(strategy *bullsharktypes.AnchorSelectionStrategy) {
	s.strategy = strategy
	s.logger.Info("Updated anchor selection strategy",
		zap.Int("max_anchors", strategy.MaxAnchorsPerRound),
		zap.Bool("require_quorum", strategy.RequireQuorum),
	)
}

// GetStats returns statistics about anchor selection
func (s *Selector) GetStats() map[string]interface{} {
	currentRound := s.mempool.GetCurrentRound()
	candidates := s.GetAnchorCandidates(currentRound)

	return map[string]interface{}{
		"current_round":         uint64(currentRound),
		"anchor_candidates":     len(candidates),
		"max_anchors_per_round": s.strategy.MaxAnchorsPerRound,
		"require_quorum":        s.strategy.RequireQuorum,
		"quorum_threshold":      s.strategy.QuorumThreshold,
		"can_create_block":      s.CanCreateBlock(currentRound),
	}
}
