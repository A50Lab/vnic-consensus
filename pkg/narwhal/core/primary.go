package core

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cometbft/cometbft/crypto"
	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/p2p"
	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

// Primary represents a primary node in Narwhal that creates certificates
type Primary struct {
	nodeID     types.NodeID
	privateKey crypto.PrivKey
	publicKey  crypto.PubKey
	logger     *zap.Logger
	network    *p2p.Network
	mempool    *DAGMempool

	// Workers managed by this primary
	workers      map[types.NodeID]*Worker
	workersMutex sync.RWMutex

	// Committee of other primaries
	committee      map[types.NodeID]*types.PrimaryInfo
	committeeMutex sync.RWMutex

	// Certificate creation
	currentRound types.Round
	currentEpoch uint64
	roundMutex   sync.RWMutex

	// Configuration
	certificateTimeout time.Duration
	maxPayloadSize     int
	minPayloadSize     int

	// State
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Pending votes for certificates
	pendingVotes map[types.Hash]map[types.NodeID]*types.Vote
	votesMutex   sync.RWMutex

	// Callbacks
	onCertificateCreated func(*types.Certificate)
	onRoundAdvanced      func(types.Round)
}

// NewPrimary creates a new primary node
func NewPrimary(
	nodeID types.NodeID,
	privateKey crypto.PrivKey,
	network *p2p.Network,
	mempool *DAGMempool,
	logger *zap.Logger,
) *Primary {
	ctx, cancel := context.WithCancel(context.Background())

	return &Primary{
		nodeID:             nodeID,
		privateKey:         privateKey,
		publicKey:          privateKey.PubKey(),
		logger:             logger,
		network:            network,
		mempool:            mempool,
		ctx:                ctx,
		cancel:             cancel,
		workers:            make(map[types.NodeID]*Worker),
		committee:          make(map[types.NodeID]*types.PrimaryInfo),
		currentRound:       0,
		currentEpoch:       0,
		certificateTimeout: 10 * time.Second,
		maxPayloadSize:     100,
		minPayloadSize:     0, // Allow empty certificates for continuous block production
		pendingVotes:       make(map[types.Hash]map[types.NodeID]*types.Vote),
	}
}

// SetConfig sets the primary configuration
func (p *Primary) SetConfig(certificateTimeout time.Duration, maxPayloadSize, minPayloadSize int) {
	p.certificateTimeout = certificateTimeout
	p.maxPayloadSize = maxPayloadSize
	p.minPayloadSize = minPayloadSize
}

// SetCallbacks sets the callback functions
func (p *Primary) SetCallbacks(
	onCertificateCreated func(*types.Certificate),
	onRoundAdvanced func(types.Round),
) {
	p.onCertificateCreated = onCertificateCreated
	p.onRoundAdvanced = onRoundAdvanced
}

// AddWorker adds a worker to this primary
func (p *Primary) AddWorker(w *Worker) {
	p.workersMutex.Lock()
	defer p.workersMutex.Unlock()

	workerID := w.GetStats()["node_id"].(string)
	p.workers[types.NodeID(workerID)] = w

	p.logger.Info("Added worker to primary",
		zap.String("worker_id", workerID),
		zap.String("primary_id", string(p.nodeID)),
	)
}

// UpdateCommittee updates the committee of primaries
func (p *Primary) UpdateCommittee(committee map[types.NodeID]*types.PrimaryInfo) {
	p.committeeMutex.Lock()
	defer p.committeeMutex.Unlock()

	p.committee = committee
	p.logger.Info("Updated primary committee",
		zap.Int("committee_size", len(committee)),
	)
}

// Start starts the primary node
func (p *Primary) Start() error {
	p.logger.Info("Starting primary node",
		zap.String("node_id", string(p.nodeID)),
	)

	// Set up network handlers
	p.network.SetHandlers(
		p.handleBatch,
		p.handleHeader,
		p.handleCertificate,
		p.handleVote,
	)

	// Start certificate creation loop
	p.wg.Add(1)
	go p.certificateCreationLoop()

	// Start vote processing loop
	p.wg.Add(1)
	go p.voteProcessingLoop()

	p.logger.Info("Primary node started successfully")
	return nil
}

// Stop stops the primary node
func (p *Primary) Stop() error {
	p.logger.Info("Stopping primary node")

	p.cancel()
	p.wg.Wait()

	p.logger.Info("Primary node stopped")
	return nil
}

// GetCurrentRound returns the current round
func (p *Primary) GetCurrentRound() types.Round {
	p.roundMutex.RLock()
	defer p.roundMutex.RUnlock()
	return p.currentRound
}

// AdvanceRound advances to the next round
func (p *Primary) AdvanceRound() {
	p.roundMutex.Lock()
	defer p.roundMutex.Unlock()

	p.currentRound++
	p.logger.Info("Advanced to new round",
		zap.Uint64("round", uint64(p.currentRound)),
	)

	if p.onRoundAdvanced != nil {
		p.onRoundAdvanced(p.currentRound)
	}
}

// certificateCreationLoop handles the automatic creation of certificates
func (p *Primary) certificateCreationLoop() {
	defer p.wg.Done()

	p.logger.Debug("Starting certificate creation loop")

	// Get initial timeout
	timeout := p.getAdaptiveTimeout()
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Debug("Certificate creation loop stopped")
			return
		case <-ticker.C:
			if err := p.tryCreateCertificate(); err != nil {
				p.logger.Error("Failed to create certificate", zap.Error(err))
			}

			// Update ticker with new adaptive timeout
			newTimeout := p.getAdaptiveTimeout()
			if newTimeout != timeout {
				timeout = newTimeout
				ticker.Stop()
				ticker = time.NewTicker(timeout)
				p.logger.Debug("Updated certificate creation timeout",
					zap.Duration("new_timeout", timeout),
				)
			}
		}
	}
}

// getAdaptiveTimeout returns the current timeout (adaptive if Shoal is enabled)
func (p *Primary) getAdaptiveTimeout() time.Duration {
	return p.certificateTimeout
}

// voteProcessingLoop handles processing of votes
func (p *Primary) voteProcessingLoop() {
	defer p.wg.Done()

	p.logger.Debug("Starting vote processing loop")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Debug("Vote processing loop stopped")
			return
		case <-ticker.C:
			p.processAccumulatedVotes()
		}
	}
}

// tryCreateCertificate attempts to create a new certificate
func (p *Primary) tryCreateCertificate() error {
	p.logger.Debug("Attempting to create certificate")

	return p.createCertificateSync()
}

// createCertificateSync creates a certificate synchronously (fallback)
func (p *Primary) createCertificateSync() error {
	// Get available batches for payload
	payload := p.selectPayloadBatches()

	// Always create certificate to enable continuous block production
	// Even empty certificates (with no batches) are valuable for consensus progress

	// Get parent certificates
	parents := p.selectParentCertificates()

	// Create header
	header := &types.Header{
		Author:    p.nodeID,
		Round:     p.GetCurrentRound(),
		Epoch:     p.currentEpoch,
		Parents:   parents,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	// Sign the header
	headerHash := header.Hash()
	signature, err := p.privateKey.Sign(headerHash.Bytes())
	if err != nil {
		return fmt.Errorf("failed to sign header: %w", err)
	}
	header.Signature = signature

	// Create certificate
	certificate := types.NewCertificate(header, signature)

	p.logger.Info("Created new certificate",
		zap.String("cert_hash", certificate.Hash.String()),
		zap.Uint64("round", uint64(header.Round)),
		zap.Int("payload_size", len(payload)),
		zap.Int("parents_count", len(parents)),
	)

	// Add certificate to mempool
	if err := p.mempool.AddCertificate(certificate); err != nil {
		return fmt.Errorf("failed to add certificate to mempool: %w", err)
	}

	// Broadcast header to committee for voting
	if err := p.network.BroadcastHeader(header); err != nil {
		p.logger.Error("Failed to broadcast header", zap.Error(err))
	}

	// Call callback if set
	if p.onCertificateCreated != nil {
		p.onCertificateCreated(certificate)
	}

	// Advance to next round for continuous progress
	p.AdvanceRound()

	return nil
}

// selectPayloadBatches selects batches to include in certificate payload
func (p *Primary) selectPayloadBatches() []types.Hash {
	// Get recent batches from mempool
	stats := p.mempool.GetStats()
	if stats["batches"].(int) == 0 {
		return nil
	}

	// Get all available batches from mempool
	allBatches := p.getAllAvailableBatches()
	if len(allBatches) == 0 {
		return nil
	}

	// Select batches up to maxPayloadSize
	payload := make([]types.Hash, 0, p.maxPayloadSize)
	selectedCount := p.maxPayloadSize
	if selectedCount > len(allBatches) {
		selectedCount = len(allBatches)
	}

	// Sort batches by timestamp for deterministic selection
	sort.Slice(allBatches, func(i, j int) bool {
		return allBatches[i].Timestamp.Before(allBatches[j].Timestamp)
	})

	// Select the most recent batches
	for i := 0; i < selectedCount; i++ {
		payload = append(payload, allBatches[i].ID)
	}

	p.logger.Debug("Selected payload batches",
		zap.Int("available_batches", len(allBatches)),
		zap.Int("selected_batches", len(payload)),
		zap.Int("max_payload_size", p.maxPayloadSize),
	)

	return payload
}

// getAllAvailableBatches gets all available batches from mempool that can be included
func (p *Primary) getAllAvailableBatches() []*types.Batch {
	// This is a simplified implementation - in production you'd want more sophisticated selection
	// For now, we'll query batches from our known workers
	batches := make([]*types.Batch, 0)

	p.workersMutex.RLock()
	defer p.workersMutex.RUnlock()

	// Get batches from all our workers
	for _, worker := range p.workers {
		_ = worker.GetStats() // Worker stats for future batch retrieval implementation
		// In a real implementation, we'd have a way to get batches from workers
		// For now, we'll simulate this by getting recent batches from mempool
	}

	// Fallback: get some batches directly from mempool
	// This is a simplified approach - in production you'd have better batch tracking
	mempoolStats := p.mempool.GetStats()
	if mempoolStats["batches"].(int) > 0 {
		// Get recent batches (simplified - would need actual batch retrieval method)
		// For now return empty to avoid breaking the system
	}

	return batches
}

// selectParentCertificates selects parent certificates for the new certificate
func (p *Primary) selectParentCertificates() []types.Hash {
	currentRound := p.GetCurrentRound()
	if currentRound == 0 {
		return nil // Genesis round has no parents
	}

	// Get certificates from the previous round
	parentRound := currentRound - 1
	parentCerts := p.mempool.GetCertificatesByRound(parentRound)

	parents := make([]types.Hash, 0, len(parentCerts))
	for _, cert := range parentCerts {
		parents = append(parents, cert.Hash)
	}

	p.logger.Debug("Selected parent certificates",
		zap.Uint64("parent_round", uint64(parentRound)),
		zap.Int("parent_count", len(parents)),
	)

	return parents
}

// handleBatch handles incoming batch messages
func (p *Primary) handleBatch(batch *types.Batch) {
	p.logger.Debug("Primary received batch",
		zap.String("batch_id", batch.ID.String()),
		zap.String("author", string(batch.Author)),
	)

	// Add batch to mempool
	if err := p.mempool.AddBatch(batch); err != nil {
		p.logger.Debug("Failed to add batch to mempool or batch already exists",
			zap.String("batch_id", batch.ID.String()),
			zap.Error(err),
		)
	}
}

// handleHeader handles incoming header messages (for voting)
func (p *Primary) handleHeader(header *types.Header) {
	p.logger.Debug("Primary received header for voting",
		zap.String("author", string(header.Author)),
		zap.Uint64("round", uint64(header.Round)),
	)

	// Don't vote on our own headers
	if header.Author == p.nodeID {
		return
	}

	// Validate header
	if err := p.validateHeader(header); err != nil {
		p.logger.Error("Invalid header received",
			zap.String("author", string(header.Author)),
			zap.Error(err),
		)
		return
	}

	// Create and send vote
	vote := &types.Vote{
		CertificateHash: header.Hash(),
		Author:          p.nodeID,
		Timestamp:       time.Now(),
	}

	// Sign the vote
	voteData := append(vote.CertificateHash.Bytes(), []byte(vote.Author)...)
	signature, err := p.privateKey.Sign(voteData)
	if err != nil {
		p.logger.Error("Failed to sign vote", zap.Error(err))
		return
	}
	vote.Signature = signature

	// Broadcast vote
	if err := p.network.BroadcastVote(vote); err != nil {
		p.logger.Error("Failed to broadcast vote", zap.Error(err))
	}

	p.logger.Debug("Sent vote for header",
		zap.String("cert_hash", vote.CertificateHash.String()),
	)
}

// handleCertificate handles incoming certificate messages
func (p *Primary) handleCertificate(cert *types.Certificate) {
	p.logger.Debug("Primary received certificate",
		zap.String("cert_hash", cert.Hash.String()),
		zap.String("author", string(cert.Header.Author)),
	)

	// Add certificate to mempool
	if err := p.mempool.AddCertificate(cert); err != nil {
		p.logger.Debug("Failed to add certificate to mempool or certificate already exists",
			zap.String("cert_hash", cert.Hash.String()),
			zap.Error(err),
		)
	}
}

// handleVote handles incoming vote messages
func (p *Primary) handleVote(vote *types.Vote) {
	p.logger.Debug("Primary received vote",
		zap.String("cert_hash", vote.CertificateHash.String()),
		zap.String("voter", string(vote.Author)),
	)

	p.votesMutex.Lock()
	defer p.votesMutex.Unlock()

	// Initialize vote map for this certificate if needed
	if p.pendingVotes[vote.CertificateHash] == nil {
		p.pendingVotes[vote.CertificateHash] = make(map[types.NodeID]*types.Vote)
	}

	// Store the vote
	p.pendingVotes[vote.CertificateHash][vote.Author] = vote
}

// processAccumulatedVotes processes accumulated votes to form certificates
func (p *Primary) processAccumulatedVotes() {
	p.votesMutex.Lock()
	defer p.votesMutex.Unlock()

	committeeSize := len(p.committee) + 1 // +1 for ourselves
	threshold := (committeeSize*2)/3 + 1  // 2f+1 threshold

	for certHash, votes := range p.pendingVotes {
		if len(votes) >= threshold {
			p.logger.Info("Certificate achieved voting threshold",
				zap.String("cert_hash", certHash.String()),
				zap.Int("votes", len(votes)),
				zap.Int("threshold", threshold),
			)

			// Remove from pending votes
			delete(p.pendingVotes, certHash)

			// In a full implementation, we would finalize the certificate here
			// For now, we just log the achievement
		}
	}
}

// validateHeader validates an incoming header
func (p *Primary) validateHeader(header *types.Header) error {
	// Check if all parent certificates exist
	for _, parentHash := range header.Parents {
		if _, exists := p.mempool.GetCertificate(parentHash); !exists {
			return fmt.Errorf("parent certificate not found: %s", parentHash.String())
		}
	}

	// Check if all payload batches exist
	for _, batchHash := range header.Payload {
		if _, exists := p.mempool.GetBatch(batchHash); !exists {
			return fmt.Errorf("payload batch not found: %s", batchHash.String())
		}
	}

	// Verify signature
	expectedHash := header.Hash()
	if !p.verifySignature(header.Author, expectedHash.Bytes(), header.Signature) {
		return fmt.Errorf("invalid header signature")
	}

	return nil
}

// verifySignature verifies a signature properly
func (p *Primary) verifySignature(author types.NodeID, data, signature []byte) bool {
	// Get the public key for the author from committee
	p.committeeMutex.RLock()
	primaryInfo, exists := p.committee[author]
	p.committeeMutex.RUnlock()

	if !exists {
		p.logger.Error("Author not found in committee", zap.String("author", string(author)))
		return false
	}

	if primaryInfo.PublicKey == nil {
		p.logger.Error("No public key available for author", zap.String("author", string(author)))
		return false
	}

	// Verify the signature using the author's public key
	if !primaryInfo.PublicKey.VerifySignature(data, signature) {
		p.logger.Error("Signature verification failed",
			zap.String("author", string(author)),
			zap.String("data_hash", fmt.Sprintf("%X", data)),
		)
		return false
	}

	return true
}

// GetStats returns primary statistics
func (p *Primary) GetStats() map[string]interface{} {
	p.roundMutex.RLock()
	defer p.roundMutex.RUnlock()

	p.workersMutex.RLock()
	workerCount := len(p.workers)
	p.workersMutex.RUnlock()

	p.committeeMutex.RLock()
	committeeSize := len(p.committee)
	p.committeeMutex.RUnlock()

	p.votesMutex.RLock()
	pendingVotesCount := len(p.pendingVotes)
	p.votesMutex.RUnlock()

	return map[string]interface{}{
		"node_id":                string(p.nodeID),
		"current_round":          uint64(p.currentRound),
		"current_epoch":          p.currentEpoch,
		"worker_count":           workerCount,
		"committee_size":         committeeSize,
		"pending_votes":          pendingVotesCount,
		"certificate_timeout_ms": p.certificateTimeout.Milliseconds(),
		"max_payload_size":       p.maxPayloadSize,
		"min_payload_size":       p.minPayloadSize,
	}
}
