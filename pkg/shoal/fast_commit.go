package shoalpp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

type FastCommitManager struct {
	config  *ShoalPPConfig
	logger  *zap.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
	mu      sync.RWMutex

	// Fast commit state
	pendingCommits       map[types.Hash]*FastCommitRequest
	commitResults        map[types.Hash]*FastCommitResult
	uncertifiedProposals map[types.NodeID]int
	commitMutex          sync.RWMutex

	// Performance metrics
	totalCommits      int64
	successfulCommits int64
	failedCommits     int64
	avgCommitLatency  time.Duration
	latencySum        time.Duration
	latencyCount      int64

	// Byzantine fault detection
	suspiciousNodes map[types.NodeID]*SuspiciousActivity
	byzantineMutex  sync.RWMutex
}

// FastCommitRequest represents a request for fast direct commit
type FastCommitRequest struct {
	ID                   string
	Candidate            *AnchorCandidate
	UncertifiedProposals int
	Threshold            int
	RequestTime          time.Time
	Timeout              time.Duration
	NodeID               types.NodeID
	Status               FastCommitStatus
}

// FastCommitResult represents the result of a fast commit operation
type FastCommitResult struct {
	RequestID        string
	Success          bool
	CommitLatency    time.Duration
	MessageDelays    int
	LatencyReduction time.Duration
	Error            error
	CommittedAt      time.Time
	CommitRule       CommitRule
}

// FastCommitStatus represents the status of a fast commit request
type FastCommitStatus int

const (
	FastCommitPending FastCommitStatus = iota
	FastCommitProcessing
	FastCommitCompleted
	FastCommitFailed
	FastCommitTimedOut
)

// SuspiciousActivity tracks suspicious activity for Byzantine fault detection
type SuspiciousActivity struct {
	NodeID             types.NodeID
	ExcessiveRequests  int
	FailedCommits      int
	AnomalousLatencies int
	LastSuspiciousTime time.Time
	TrustScore         float64
}

// NewFastCommitManager creates a new fast commit manager
func NewFastCommitManager(config *ShoalPPConfig, logger *zap.Logger) *FastCommitManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &FastCommitManager{
		config:               config,
		logger:               logger,
		ctx:                  ctx,
		cancel:               cancel,
		pendingCommits:       make(map[types.Hash]*FastCommitRequest),
		commitResults:        make(map[types.Hash]*FastCommitResult),
		uncertifiedProposals: make(map[types.NodeID]int),
		suspiciousNodes:      make(map[types.NodeID]*SuspiciousActivity),
	}
}

// Start starts the fast commit manager
func (fcm *FastCommitManager) Start() error {
	fcm.mu.Lock()
	defer fcm.mu.Unlock()

	if fcm.running {
		return nil
	}

	fcm.logger.Info("Starting Fast Commit Manager")

	// Start fast commit processor
	fcm.wg.Add(1)
	go fcm.fastCommitProcessor()

	// Start timeout monitor
	fcm.wg.Add(1)
	go fcm.timeoutMonitor()

	// Start Byzantine fault detector
	fcm.wg.Add(1)
	go fcm.byzantineFaultDetector()

	fcm.running = true
	fcm.logger.Info("Fast Commit Manager started successfully")

	return nil
}

// Stop stops the fast commit manager
func (fcm *FastCommitManager) Stop() {
	fcm.mu.Lock()
	defer fcm.mu.Unlock()

	if !fcm.running {
		return
	}

	fcm.logger.Info("Stopping Fast Commit Manager")

	fcm.cancel()
	fcm.wg.Wait()

	fcm.running = false
	fcm.logger.Info("Fast Commit Manager stopped")
}

// ProcessFastCommit processes a fast commit request
func (fcm *FastCommitManager) ProcessFastCommit(candidate *AnchorCandidate) (*FastCommitResult, error) {
	if !fcm.config.EnableFastDirectCommit {
		return nil, ErrFastCommitFailed
	}

	start := time.Now()

	// Check if node is suspicious
	if fcm.isNodeSuspicious(candidate.NodeID) {
		fcm.logger.Warn("Fast commit rejected for suspicious node",
			zap.String("node_id", string(candidate.NodeID)))
		return &FastCommitResult{
			Success: false,
			Error:   ErrFastCommitFailed,
		}, ErrFastCommitFailed
	}

	// Create fast commit request
	request := &FastCommitRequest{
		ID:                   candidate.Certificate.Hash.String(),
		Candidate:            candidate,
		UncertifiedProposals: fcm.getUncertifiedCount(candidate.NodeID),
		Threshold:            fcm.config.FastCommitThreshold,
		RequestTime:          start,
		Timeout:              fcm.calculateCommitTimeout(candidate.NodeID),
		NodeID:               candidate.NodeID,
		Status:               FastCommitPending,
	}

	// Check if fast commit is applicable
	if !fcm.canUseFastCommit(request) {
		return &FastCommitResult{
			Success:       false,
			CommitRule:    OriginalCommitRule,
			MessageDelays: 6, // Fall back to original Bullshark
			Error:         ErrFastCommitFailed,
		}, nil
	}

	// Process the fast commit
	result := fcm.executeFastCommit(request)

	// Update metrics
	fcm.updateMetrics(result)

	// Update suspicious activity tracking
	fcm.updateSuspiciousActivity(candidate.NodeID, result.Success, result.CommitLatency)

	return result, nil
}

// fastCommitProcessor processes pending fast commit requests
func (fcm *FastCommitManager) fastCommitProcessor() {
	defer fcm.wg.Done()

	ticker := time.NewTicker(time.Millisecond * 10) // Process every 10ms
	defer ticker.Stop()

	for {
		select {
		case <-fcm.ctx.Done():
			return
		case <-ticker.C:
			fcm.processPendingCommits()
		}
	}
}

// processPendingCommits processes all pending fast commit requests
func (fcm *FastCommitManager) processPendingCommits() {
	fcm.commitMutex.Lock()
	defer fcm.commitMutex.Unlock()

	for hash, request := range fcm.pendingCommits {
		if request.Status == FastCommitPending {
			if fcm.canUseFastCommit(request) {
				request.Status = FastCommitProcessing
				go fcm.processAsyncCommit(request)
			}
		}
	}
}

// processAsyncCommit processes a fast commit request asynchronously
func (fcm *FastCommitManager) processAsyncCommit(request *FastCommitRequest) {
	result := fcm.executeFastCommit(request)

	fcm.commitMutex.Lock()
	fcm.commitResults[types.Hash(request.ID)] = result
	request.Status = FastCommitCompleted
	delete(fcm.pendingCommits, types.Hash(request.ID))
	fcm.commitMutex.Unlock()

	fcm.updateMetrics(result)
}

// executeFastCommit executes the fast direct commit
func (fcm *FastCommitManager) executeFastCommit(request *FastCommitRequest) *FastCommitResult {
	start := time.Now()

	// Simulate fast commit processing (4 message delays instead of 6)
	// In a real implementation, this would interact with the consensus mechanism
	processingTime := fcm.calculateProcessingTime(request)
	time.Sleep(processingTime)

	commitLatency := time.Since(start)
	originalLatency := time.Duration(float64(commitLatency) * 1.5) // 6/4 = 1.5x
	latencyReduction := originalLatency - commitLatency

	result := &FastCommitResult{
		RequestID:        request.ID,
		Success:          true,
		CommitLatency:    commitLatency,
		MessageDelays:    4, // Shoal++ reduces from 6 to 4
		LatencyReduction: latencyReduction,
		CommittedAt:      time.Now(),
		CommitRule:       FastDirectCommitRule,
	}

	// Check for Byzantine behavior
	if fcm.detectByzantineCommit(request, result) {
		result.Success = false
		result.Error = ErrFastCommitFailed
		fcm.markSuspicious(request.NodeID, "byzantine_commit_pattern")
	}

	return result
}

// canUseFastCommit determines if fast commit can be used for this request
func (fcm *FastCommitManager) canUseFastCommit(request *FastCommitRequest) bool {
	// Check threshold requirement (2f+1 uncertified proposals)
	if request.UncertifiedProposals < request.Threshold {
		return false
	}

	// Check if node is not suspicious
	if fcm.isNodeSuspicious(request.NodeID) {
		return false
	}

	// Check timeout
	if time.Since(request.RequestTime) > request.Timeout {
		return false
	}

	// Check network conditions
	if !fcm.isNetworkSuitable() {
		return false
	}

	return true
}

// calculateCommitTimeout calculates the timeout for a fast commit based on node reputation
func (fcm *FastCommitManager) calculateCommitTimeout(nodeID types.NodeID) time.Duration {
	baseTimeout := time.Millisecond * 100 // Base fast commit timeout

	// Adjust based on suspicious activity
	if activity, exists := fcm.suspiciousNodes[nodeID]; exists {
		// Reduce timeout for suspicious nodes
		adjustment := 1.0 - (1.0-activity.TrustScore)*0.5
		return time.Duration(float64(baseTimeout) * adjustment)
	}

	return baseTimeout
}

// calculateProcessingTime calculates the processing time for a fast commit
func (fcm *FastCommitManager) calculateProcessingTime(request *FastCommitRequest) time.Duration {
	// Base processing time for 4 message delays
	baseTime := time.Millisecond * 40 // 10ms per message delay

	// Adjust based on network conditions and node reputation
	networkFactor := 1.0
	if !fcm.isNetworkSuitable() {
		networkFactor = 1.2 // 20% slower in poor network conditions
	}

	return time.Duration(float64(baseTime) * networkFactor)
}

// isNetworkSuitable checks if network conditions are suitable for fast commit
func (fcm *FastCommitManager) isNetworkSuitable() bool {
	// Check various network conditions
	// This would integrate with actual network monitoring
	return true // Simplified for now
}

// getUncertifiedCount gets the count of uncertified proposals for a node
func (fcm *FastCommitManager) getUncertifiedCount(nodeID types.NodeID) int {
	fcm.commitMutex.RLock()
	defer fcm.commitMutex.RUnlock()

	if count, exists := fcm.uncertifiedProposals[nodeID]; exists {
		return count
	}
	return 0
}

// UpdateUncertifiedCount updates the uncertified proposal count for a node
func (fcm *FastCommitManager) UpdateUncertifiedCount(nodeID types.NodeID, count int) {
	fcm.commitMutex.Lock()
	defer fcm.commitMutex.Unlock()

	fcm.uncertifiedProposals[nodeID] = count
}

// timeoutMonitor monitors and handles request timeouts
func (fcm *FastCommitManager) timeoutMonitor() {
	defer fcm.wg.Done()

	ticker := time.NewTicker(time.Millisecond * 50) // Check every 50ms
	defer ticker.Stop()

	for {
		select {
		case <-fcm.ctx.Done():
			return
		case <-ticker.C:
			fcm.handleTimeouts()
		}
	}
}

// handleTimeouts handles timed-out fast commit requests
func (fcm *FastCommitManager) handleTimeouts() {
	fcm.commitMutex.Lock()
	defer fcm.commitMutex.Unlock()

	now := time.Now()
	toDelete := make([]types.Hash, 0)

	for hash, request := range fcm.pendingCommits {
		if now.Sub(request.RequestTime) > request.Timeout {
			request.Status = FastCommitTimedOut

			// Create timeout result
			result := &FastCommitResult{
				RequestID:     request.ID,
				Success:       false,
				CommitLatency: now.Sub(request.RequestTime),
				MessageDelays: 6, // Fall back to original
				Error:         ErrRoundTimeoutExceeded,
				CommittedAt:   now,
				CommitRule:    OriginalCommitRule,
			}

			fcm.commitResults[hash] = result
			toDelete = append(toDelete, hash)

			// Mark as suspicious activity
			fcm.markSuspicious(request.NodeID, "frequent_timeouts")

			atomic.AddInt64(&fcm.failedCommits, 1)
		}
	}

	// Clean up timed-out requests
	for _, hash := range toDelete {
		delete(fcm.pendingCommits, hash)
	}
}

// byzantineFaultDetector detects Byzantine fault patterns
func (fcm *FastCommitManager) byzantineFaultDetector() {
	defer fcm.wg.Done()

	ticker := time.NewTicker(time.Second * 30) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-fcm.ctx.Done():
			return
		case <-ticker.C:
			fcm.detectByzantinePatterns()
		}
	}
}

// detectByzantinePatterns detects Byzantine behavior patterns
func (fcm *FastCommitManager) detectByzantinePatterns() {
	fcm.byzantineMutex.Lock()
	defer fcm.byzantineMutex.Unlock()

	now := time.Now()

	for nodeID, activity := range fcm.suspiciousNodes {
		// Decay suspicious activity over time
		timeSinceLastSuspicious := now.Sub(activity.LastSuspiciousTime)
		if timeSinceLastSuspicious > time.Minute*10 {
			// Gradually restore trust
			activity.TrustScore += 0.1
			if activity.TrustScore > 1.0 {
				activity.TrustScore = 1.0
			}
		}

		// Check for Byzantine patterns
		if activity.ExcessiveRequests > 10 ||
			activity.FailedCommits > 5 ||
			activity.AnomalousLatencies > 8 {

			fcm.logger.Warn("Byzantine behavior detected",
				zap.String("node_id", string(nodeID)),
				zap.Int("excessive_requests", activity.ExcessiveRequests),
				zap.Int("failed_commits", activity.FailedCommits),
				zap.Int("anomalous_latencies", activity.AnomalousLatencies),
				zap.Float64("trust_score", activity.TrustScore),
			)

			// Reduce trust score
			activity.TrustScore *= 0.5
			if activity.TrustScore < 0.1 {
				activity.TrustScore = 0.1
			}
		}
	}
}

// detectByzantineCommit detects Byzantine behavior in a commit
func (fcm *FastCommitManager) detectByzantineCommit(request *FastCommitRequest, result *FastCommitResult) bool {
	// Pattern 1: Unusually fast commit (potential replay attack)
	if result.CommitLatency < time.Millisecond*5 {
		return true
	}

	// Pattern 2: Unusually slow commit (potential stalling attack)
	if result.CommitLatency > time.Millisecond*200 {
		return true
	}

	// Pattern 3: Excessive requests from the same node
	activity := fcm.getSuspiciousActivity(request.NodeID)
	if activity.ExcessiveRequests > 20 {
		return true
	}

	return false
}

// isNodeSuspicious checks if a node is considered suspicious
func (fcm *FastCommitManager) isNodeSuspicious(nodeID types.NodeID) bool {
	fcm.byzantineMutex.RLock()
	defer fcm.byzantineMutex.RUnlock()

	if activity, exists := fcm.suspiciousNodes[nodeID]; exists {
		return activity.TrustScore < 0.3 // Threshold for suspicious behavior
	}
	return false
}

// markSuspicious marks a node as suspicious
func (fcm *FastCommitManager) markSuspicious(nodeID types.NodeID, reason string) {
	fcm.byzantineMutex.Lock()
	defer fcm.byzantineMutex.Unlock()

	activity := fcm.getSuspiciousActivity(nodeID)
	activity.LastSuspiciousTime = time.Now()

	switch reason {
	case "excessive_requests":
		activity.ExcessiveRequests++
	case "frequent_timeouts":
		activity.FailedCommits++
	case "anomalous_latency":
		activity.AnomalousLatencies++
	case "byzantine_commit_pattern":
		activity.FailedCommits++
		activity.AnomalousLatencies++
	}

	// Reduce trust score
	activity.TrustScore *= 0.8
	if activity.TrustScore < 0.1 {
		activity.TrustScore = 0.1
	}

	fcm.logger.Debug("Node marked as suspicious",
		zap.String("node_id", string(nodeID)),
		zap.String("reason", reason),
		zap.Float64("trust_score", activity.TrustScore),
	)
}

// getSuspiciousActivity gets or creates suspicious activity tracking for a node
func (fcm *FastCommitManager) getSuspiciousActivity(nodeID types.NodeID) *SuspiciousActivity {
	if activity, exists := fcm.suspiciousNodes[nodeID]; exists {
		return activity
	}

	activity := &SuspiciousActivity{
		NodeID:             nodeID,
		TrustScore:         1.0,
		LastSuspiciousTime: time.Now(),
	}
	fcm.suspiciousNodes[nodeID] = activity
	return activity
}

// updateSuspiciousActivity updates suspicious activity tracking
func (fcm *FastCommitManager) updateSuspiciousActivity(nodeID types.NodeID, success bool, latency time.Duration) {
	activity := fcm.getSuspiciousActivity(nodeID)

	if !success {
		fcm.markSuspicious(nodeID, "frequent_timeouts")
	}

	// Check for anomalous latencies
	if latency < time.Millisecond*2 || latency > time.Millisecond*150 {
		fcm.markSuspicious(nodeID, "anomalous_latency")
	}

	// Check for excessive requests
	activity.ExcessiveRequests++
	if activity.ExcessiveRequests > 15 {
		fcm.markSuspicious(nodeID, "excessive_requests")
	}
}

// updateMetrics updates fast commit performance metrics
func (fcm *FastCommitManager) updateMetrics(result *FastCommitResult) {
	atomic.AddInt64(&fcm.totalCommits, 1)

	if result.Success {
		atomic.AddInt64(&fcm.successfulCommits, 1)
	} else {
		atomic.AddInt64(&fcm.failedCommits, 1)
	}

	// Update average latency
	atomic.AddInt64(&fcm.latencyCount, 1)
	newLatencySum := atomic.AddInt64((*int64)(&fcm.latencySum), int64(result.CommitLatency))
	newAvg := time.Duration(newLatencySum / atomic.LoadInt64(&fcm.latencyCount))
	atomic.StoreInt64((*int64)(&fcm.avgCommitLatency), int64(newAvg))
}

// GetStats returns comprehensive fast commit statistics
func (fcm *FastCommitManager) GetStats() map[string]interface{} {
	fcm.commitMutex.RLock()
	pendingCount := len(fcm.pendingCommits)
	resultCount := len(fcm.commitResults)
	fcm.commitMutex.RUnlock()

	fcm.byzantineMutex.RLock()
	suspiciousCount := len(fcm.suspiciousNodes)
	fcm.byzantineMutex.RUnlock()

	totalCommits := atomic.LoadInt64(&fcm.totalCommits)
	successfulCommits := atomic.LoadInt64(&fcm.successfulCommits)
	failedCommits := atomic.LoadInt64(&fcm.failedCommits)

	successRate := 0.0
	if totalCommits > 0 {
		successRate = float64(successfulCommits) / float64(totalCommits)
	}

	return map[string]interface{}{
		"total_commits":      totalCommits,
		"successful_commits": successfulCommits,
		"failed_commits":     failedCommits,
		"success_rate":       successRate,
		"pending_requests":   pendingCount,
		"completed_results":  resultCount,
		"suspicious_nodes":   suspiciousCount,
		"avg_latency":        time.Duration(atomic.LoadInt64((*int64)(&fcm.avgCommitLatency))),
		"enabled":            fcm.config.EnableFastDirectCommit,
		"threshold":          fcm.config.FastCommitThreshold,
	}
}

// GetResult gets the result of a fast commit request
func (fcm *FastCommitManager) GetResult(requestID string) (*FastCommitResult, bool) {
	fcm.commitMutex.RLock()
	defer fcm.commitMutex.RUnlock()

	result, exists := fcm.commitResults[types.Hash(requestID)]
	return result, exists
}

// ClearOldResults clears old fast commit results to prevent memory leaks
func (fcm *FastCommitManager) ClearOldResults(maxAge time.Duration) {
	fcm.commitMutex.Lock()
	defer fcm.commitMutex.Unlock()

	now := time.Now()
	toDelete := make([]types.Hash, 0)

	for hash, result := range fcm.commitResults {
		if now.Sub(result.CommittedAt) > maxAge {
			toDelete = append(toDelete, hash)
		}
	}

	for _, hash := range toDelete {
		delete(fcm.commitResults, hash)
	}

	fcm.logger.Debug("Cleared old fast commit results",
		zap.Int("cleared_count", len(toDelete)),
	)
}
