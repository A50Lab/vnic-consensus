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

type FastCommitStatus int

const (
	FastCommitPending FastCommitStatus = iota
	FastCommitProcessing
	FastCommitCompleted
	FastCommitFailed
	FastCommitTimedOut
)

type SuspiciousActivity struct {
	NodeID             types.NodeID
	ExcessiveRequests  int
	FailedCommits      int
	AnomalousLatencies int
	LastSuspiciousTime time.Time
	TrustScore         float64
}

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

func (fcm *FastCommitManager) Start() error {
	fcm.mu.Lock()
	defer fcm.mu.Unlock()

	if fcm.running {
		return nil
	}

	fcm.logger.Info("Starting Fast Commit Manager")

	fcm.wg.Add(1)
	go fcm.fastCommitProcessor()

	fcm.wg.Add(1)
	go fcm.timeoutMonitor()

	fcm.wg.Add(1)
	go fcm.byzantineFaultDetector()

	fcm.running = true
	fcm.logger.Info("Fast Commit Manager started successfully")

	return nil
}

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

func (fcm *FastCommitManager) ProcessFastCommit(candidate *AnchorCandidate) (*FastCommitResult, error) {
	if !fcm.config.EnableFastDirectCommit {
		return nil, ErrFastCommitFailed
	}

	start := time.Now()

	if fcm.isNodeSuspicious(candidate.NodeID) {
		fcm.logger.Warn("Fast commit rejected for suspicious node",
			zap.String("node_id", string(candidate.NodeID)))
		return &FastCommitResult{
			Success: false,
			Error:   ErrFastCommitFailed,
		}, ErrFastCommitFailed
	}

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

	if !fcm.canUseFastCommit(request) {
		return &FastCommitResult{
			Success:       false,
			CommitRule:    OriginalCommitRule,
			MessageDelays: 6, // Fall back to original Bullshark
			Error:         ErrFastCommitFailed,
		}, nil
	}

	result := fcm.executeFastCommit(request)

	fcm.updateMetrics(result)

	fcm.updateSuspiciousActivity(candidate.NodeID, result.Success, result.CommitLatency)

	return result, nil
}

func (fcm *FastCommitManager) fastCommitProcessor() {
	defer fcm.wg.Done()

	ticker := time.NewTicker(time.Millisecond * 10)
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

func (fcm *FastCommitManager) processAsyncCommit(request *FastCommitRequest) {
	result := fcm.executeFastCommit(request)

	fcm.commitMutex.Lock()
	fcm.commitResults[types.Hash(request.ID)] = result
	request.Status = FastCommitCompleted
	delete(fcm.pendingCommits, types.Hash(request.ID))
	fcm.commitMutex.Unlock()

	fcm.updateMetrics(result)
}

func (fcm *FastCommitManager) executeFastCommit(request *FastCommitRequest) *FastCommitResult {
	start := time.Now()

	processingTime := fcm.calculateProcessingTime(request)
	time.Sleep(processingTime)

	commitLatency := time.Since(start)
	originalLatency := time.Duration(float64(commitLatency) * 1.5)
	latencyReduction := originalLatency - commitLatency

	result := &FastCommitResult{
		RequestID:        request.ID,
		Success:          true,
		CommitLatency:    commitLatency,
		MessageDelays:    4,
		LatencyReduction: latencyReduction,
		CommittedAt:      time.Now(),
		CommitRule:       FastDirectCommitRule,
	}

	if fcm.detectByzantineCommit(request, result) {
		result.Success = false
		result.Error = ErrFastCommitFailed
		fcm.markSuspicious(request.NodeID, "byzantine_commit_pattern")
	}

	return result
}

func (fcm *FastCommitManager) canUseFastCommit(request *FastCommitRequest) bool {
	if request.UncertifiedProposals < request.Threshold {
		return false
	}

	if fcm.isNodeSuspicious(request.NodeID) {
		return false
	}

	if time.Since(request.RequestTime) > request.Timeout {
		return false
	}

	if !fcm.isNetworkSuitable() {
		return false
	}

	return true
}

func (fcm *FastCommitManager) calculateCommitTimeout(nodeID types.NodeID) time.Duration {
	baseTimeout := time.Millisecond * 100

	if activity, exists := fcm.suspiciousNodes[nodeID]; exists {
		adjustment := 1.0 - (1.0-activity.TrustScore)*0.5
		return time.Duration(float64(baseTimeout) * adjustment)
	}

	return baseTimeout
}

func (fcm *FastCommitManager) calculateProcessingTime(request *FastCommitRequest) time.Duration {
	baseTime := time.Millisecond * 40

	networkFactor := 1.0
	if !fcm.isNetworkSuitable() {
		networkFactor = 1.2
	}

	return time.Duration(float64(baseTime) * networkFactor)
}

func (fcm *FastCommitManager) isNetworkSuitable() bool {
	return true // Simplified for now
}

func (fcm *FastCommitManager) getUncertifiedCount(nodeID types.NodeID) int {
	fcm.commitMutex.RLock()
	defer fcm.commitMutex.RUnlock()

	if count, exists := fcm.uncertifiedProposals[nodeID]; exists {
		return count
	}
	return 0
}

func (fcm *FastCommitManager) UpdateUncertifiedCount(nodeID types.NodeID, count int) {
	fcm.commitMutex.Lock()
	defer fcm.commitMutex.Unlock()

	fcm.uncertifiedProposals[nodeID] = count
}

func (fcm *FastCommitManager) timeoutMonitor() {
	defer fcm.wg.Done()

	ticker := time.NewTicker(time.Millisecond * 50)
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

func (fcm *FastCommitManager) handleTimeouts() {
	fcm.commitMutex.Lock()
	defer fcm.commitMutex.Unlock()

	now := time.Now()
	toDelete := make([]types.Hash, 0)

	for hash, request := range fcm.pendingCommits {
		if now.Sub(request.RequestTime) > request.Timeout {
			request.Status = FastCommitTimedOut

			result := &FastCommitResult{
				RequestID:     request.ID,
				Success:       false,
				CommitLatency: now.Sub(request.RequestTime),
				MessageDelays: 6,
				Error:         ErrRoundTimeoutExceeded,
				CommittedAt:   now,
				CommitRule:    OriginalCommitRule,
			}

			fcm.commitResults[hash] = result
			toDelete = append(toDelete, hash)

			fcm.markSuspicious(request.NodeID, "frequent_timeouts")

			atomic.AddInt64(&fcm.failedCommits, 1)
		}
	}

	for _, hash := range toDelete {
		delete(fcm.pendingCommits, hash)
	}
}

func (fcm *FastCommitManager) byzantineFaultDetector() {
	defer fcm.wg.Done()

	ticker := time.NewTicker(time.Second * 30)
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

func (fcm *FastCommitManager) detectByzantinePatterns() {
	fcm.byzantineMutex.Lock()
	defer fcm.byzantineMutex.Unlock()

	now := time.Now()

	for nodeID, activity := range fcm.suspiciousNodes {
		timeSinceLastSuspicious := now.Sub(activity.LastSuspiciousTime)
		if timeSinceLastSuspicious > time.Minute*10 {
			activity.TrustScore += 0.1
			if activity.TrustScore > 1.0 {
				activity.TrustScore = 1.0
			}
		}

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

			activity.TrustScore *= 0.5
			if activity.TrustScore < 0.1 {
				activity.TrustScore = 0.1
			}
		}
	}
}

func (fcm *FastCommitManager) detectByzantineCommit(request *FastCommitRequest, result *FastCommitResult) bool {
	if result.CommitLatency < time.Millisecond*5 {
		return true
	}

	if result.CommitLatency > time.Millisecond*200 {
		return true
	}

	activity := fcm.getSuspiciousActivity(request.NodeID)
	if activity.ExcessiveRequests > 20 {
		return true
	}

	return false
}

func (fcm *FastCommitManager) isNodeSuspicious(nodeID types.NodeID) bool {
	fcm.byzantineMutex.RLock()
	defer fcm.byzantineMutex.RUnlock()

	if activity, exists := fcm.suspiciousNodes[nodeID]; exists {
		return activity.TrustScore < 0.3
	}
	return false
}

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

func (fcm *FastCommitManager) updateSuspiciousActivity(nodeID types.NodeID, success bool, latency time.Duration) {
	activity := fcm.getSuspiciousActivity(nodeID)

	if !success {
		fcm.markSuspicious(nodeID, "frequent_timeouts")
	}

	if latency < time.Millisecond*2 || latency > time.Millisecond*150 {
		fcm.markSuspicious(nodeID, "anomalous_latency")
	}

	activity.ExcessiveRequests++
	if activity.ExcessiveRequests > 15 {
		fcm.markSuspicious(nodeID, "excessive_requests")
	}
}

func (fcm *FastCommitManager) updateMetrics(result *FastCommitResult) {
	atomic.AddInt64(&fcm.totalCommits, 1)

	if result.Success {
		atomic.AddInt64(&fcm.successfulCommits, 1)
	} else {
		atomic.AddInt64(&fcm.failedCommits, 1)
	}

	atomic.AddInt64(&fcm.latencyCount, 1)
	newLatencySum := atomic.AddInt64((*int64)(&fcm.latencySum), int64(result.CommitLatency))
	newAvg := time.Duration(newLatencySum / atomic.LoadInt64(&fcm.latencyCount))
	atomic.StoreInt64((*int64)(&fcm.avgCommitLatency), int64(newAvg))
}

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

func (fcm *FastCommitManager) GetResult(requestID string) (*FastCommitResult, bool) {
	fcm.commitMutex.RLock()
	defer fcm.commitMutex.RUnlock()

	result, exists := fcm.commitResults[types.Hash(requestID)]
	return result, exists
}

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
