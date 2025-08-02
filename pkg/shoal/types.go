package shoalpp

import (
	"sort"
	"sync"
	"time"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

type ShoalPPConfig struct {
	EnableLeaderReputation bool
	ReputationWindow       time.Duration
	ReputationDecayRate    float64
	MinReputationScore     float64

	EnableAdaptiveTimeouts bool
	BaseTimeout            time.Duration
	MaxTimeout             time.Duration
	MinTimeout             time.Duration
	TimeoutAdjustmentRate  float64

	EnablePipelining bool
	PipelineDepth    int
	MaxConcurrentOps int

	EnableMetrics   bool
	MetricsInterval time.Duration

	EnablePrevalentResponsiveness bool
	ResponsivenessThreshold       time.Duration

	EnableFastDirectCommit  bool
	EnableDynamicAnchorFreq bool
	EnableParallelDAGs      bool
	ParallelDAGInstances    int
	DAGStaggerDelay         time.Duration
	RoundTimeoutEnabled     bool
	RoundTimeout            time.Duration
	AnchorSkippingEnabled   bool
	AnchorSkippingThreshold time.Duration
	FastCommitThreshold     int
}

func DefaultShoalPPConfig() *ShoalPPConfig {
	return &ShoalPPConfig{
		EnableLeaderReputation:        true,
		ReputationWindow:              time.Minute * 10,
		ReputationDecayRate:           0.1,
		MinReputationScore:            0.1,
		EnableAdaptiveTimeouts:        true,
		BaseTimeout:                   time.Second * 10,
		MaxTimeout:                    time.Second * 30,
		MinTimeout:                    time.Second * 1,
		TimeoutAdjustmentRate:         0.2,
		EnablePipelining:              true,
		PipelineDepth:                 3,
		MaxConcurrentOps:              10,
		EnableMetrics:                 true,
		MetricsInterval:               time.Second * 5,
		EnablePrevalentResponsiveness: true,
		ResponsivenessThreshold:       time.Millisecond * 100,
		EnableFastDirectCommit:        true,
		EnableDynamicAnchorFreq:       true,
		EnableParallelDAGs:            true,
		ParallelDAGInstances:          3,
		DAGStaggerDelay:               time.Millisecond * 10,
		RoundTimeoutEnabled:           true,
		RoundTimeout:                  time.Millisecond * 50,
		AnchorSkippingEnabled:         true,
		AnchorSkippingThreshold:       time.Millisecond * 200,
		FastCommitThreshold:           5,
	}
}

type LeaderReputation struct {
	NodeID     types.NodeID
	Score      float64
	LastUpdate time.Time

	AvgLatency   time.Duration
	SuccessRate  float64
	MessageCount int64
	FailureCount int64

	ConnectivityScore float64
	BandwidthScore    float64

	WindowStart time.Time
	WindowEnd   time.Time

	FastCommitCount        int64
	DynamicAnchorCount     int64
	ParallelDAGUtilization float64
	RoundTimeoutCount      int64
	AnchorSkipCount        int64

	mu sync.RWMutex
}

// NewLeaderReputation creates a new leader reputation tracker
func NewLeaderReputation(nodeID types.NodeID) *LeaderReputation {
	now := time.Now()
	return &LeaderReputation{
		NodeID:                 nodeID,
		Score:                  1.0, // Start with perfect score
		LastUpdate:             now,
		AvgLatency:             0,
		SuccessRate:            1.0,
		MessageCount:           0,
		FailureCount:           0,
		ConnectivityScore:      1.0,
		BandwidthScore:         1.0,
		WindowStart:            now,
		WindowEnd:              now.Add(time.Minute * 10),
		FastCommitCount:        0,
		DynamicAnchorCount:     0,
		ParallelDAGUtilization: 0.0,
		RoundTimeoutCount:      0,
		AnchorSkipCount:        0,
	}
}

func (lr *LeaderReputation) UpdateScore(latency time.Duration, success bool) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	now := time.Now()

	lr.MessageCount++
	if !success {
		lr.FailureCount++
	}

	lr.SuccessRate = 1.0 - (float64(lr.FailureCount) / float64(lr.MessageCount))

	alpha := 0.1
	if lr.AvgLatency == 0 {
		lr.AvgLatency = latency
	} else {
		lr.AvgLatency = time.Duration(float64(lr.AvgLatency)*(1-alpha) + float64(latency)*alpha)
	}

	latencyScore := calculateLatencyScore(latency)
	connectivityScore := lr.ConnectivityScore
	bandwidthScore := lr.BandwidthScore

	fastCommitBonus := lr.calculateFastCommitBonus()
	dynamicAnchorBonus := lr.calculateDynamicAnchorBonus()
	parallelDAGBonus := lr.calculateParallelDAGBonus()

	byzantinePenalty := lr.calculateEnhancedByzantinePenalty()

	baseScore := (lr.SuccessRate*0.35 + latencyScore*0.25 + connectivityScore*0.15 + bandwidthScore*0.1 +
		fastCommitBonus*0.05 + dynamicAnchorBonus*0.05 + parallelDAGBonus*0.05)
	lr.Score = baseScore * (1.0 - byzantinePenalty)

	if lr.Score < 0.0 {
		lr.Score = 0.0
	}

	lr.LastUpdate = now
}

func (lr *LeaderReputation) calculateFastCommitBonus() float64 {
	if lr.MessageCount == 0 {
		return 0.0
	}

	fastCommitRatio := float64(lr.FastCommitCount) / float64(lr.MessageCount)
	return fastCommitRatio
}

func (lr *LeaderReputation) calculateDynamicAnchorBonus() float64 {
	if lr.MessageCount == 0 {
		return 0.0
	}

	dynamicAnchorRatio := float64(lr.DynamicAnchorCount) / float64(lr.MessageCount)
	return dynamicAnchorRatio * 0.8
}

func (lr *LeaderReputation) calculateParallelDAGBonus() float64 {
	return lr.ParallelDAGUtilization
}

func (lr *LeaderReputation) calculateEnhancedByzantinePenalty() float64 {
	if lr.SuccessRate < 0.5 && lr.MessageCount > 10 {
		return 0.8
	}

	if lr.MessageCount > 20 {
		recentLatencyVariance := float64(lr.AvgLatency) * 0.1
		if recentLatencyVariance < float64(time.Millisecond) {
			return 0.3
		}
	}

	maxAcceptableLatency := lr.AvgLatency * 10
	if lr.AvgLatency > maxAcceptableLatency && lr.MessageCount > 5 {
		return 0.5
	}

	if lr.MessageCount > 0 {
		timeoutRatio := float64(lr.RoundTimeoutCount) / float64(lr.MessageCount)
		if timeoutRatio > 0.3 {
			return 0.6
		}
	}

	if lr.MessageCount > 0 {
		skipRatio := float64(lr.AnchorSkipCount) / float64(lr.MessageCount)
		if skipRatio > 0.4 {
			return 0.4
		}
	}

	if lr.MessageCount > 10 && lr.ParallelDAGUtilization < 0.3 {
		return 0.2
	}

	return 0.0
}

func (lr *LeaderReputation) UpdateFastCommitMetrics(fastCommitUsed bool) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if fastCommitUsed {
		lr.FastCommitCount++
	}
}

func (lr *LeaderReputation) UpdateDynamicAnchorMetrics(dynamicAnchorUsed bool) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if dynamicAnchorUsed {
		lr.DynamicAnchorCount++
	}
}

func (lr *LeaderReputation) UpdateParallelDAGMetrics(utilization float64) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	alpha := 0.1
	if lr.ParallelDAGUtilization == 0.0 {
		lr.ParallelDAGUtilization = utilization
	} else {
		lr.ParallelDAGUtilization = lr.ParallelDAGUtilization*(1-alpha) + utilization*alpha
	}
}

func (lr *LeaderReputation) UpdateRoundTimeoutMetrics(timeoutOccurred bool) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if timeoutOccurred {
		lr.RoundTimeoutCount++
	}
}

func (lr *LeaderReputation) UpdateAnchorSkipMetrics(skipOccurred bool) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if skipOccurred {
		lr.AnchorSkipCount++
	}
}

func calculateLatencyScore(latency time.Duration) float64 {
	latencyMs := float64(latency.Nanoseconds()) / 1000000.0

	if latencyMs <= 0.5 {
		return 1.0
	}

	if latencyMs <= 10.0 {
		return 1.0 - 0.1*(latencyMs-0.5)/9.5
	} else if latencyMs <= 50.0 {
		progress := (latencyMs - 10.0) / 40.0
		return 0.9 * (1.0 - progress*progress)
	} else if latencyMs <= 200.0 {
		progress := (latencyMs - 50.0) / 150.0
		return 0.6 * (1.0 - progress)
	} else {
		return 0.05 / (1.0 + latencyMs/200.0)
	}
}

func (lr *LeaderReputation) GetScore() float64 {
	lr.mu.RLock()
	defer lr.mu.RUnlock()
	return lr.Score
}

func (lr *LeaderReputation) GetShoalPPMetrics() map[string]interface{} {
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	return map[string]interface{}{
		"fast_commit_count":        lr.FastCommitCount,
		"dynamic_anchor_count":     lr.DynamicAnchorCount,
		"parallel_dag_utilization": lr.ParallelDAGUtilization,
		"round_timeout_count":      lr.RoundTimeoutCount,
		"anchor_skip_count":        lr.AnchorSkipCount,
		"fast_commit_ratio":        float64(lr.FastCommitCount) / float64(lr.MessageCount),
		"dynamic_anchor_ratio":     float64(lr.DynamicAnchorCount) / float64(lr.MessageCount),
		"round_timeout_ratio":      float64(lr.RoundTimeoutCount) / float64(lr.MessageCount),
		"anchor_skip_ratio":        float64(lr.AnchorSkipCount) / float64(lr.MessageCount),
	}
}

type ReputationManager struct {
	reputations map[types.NodeID]*LeaderReputation
	config      *ShoalPPConfig
	mu          sync.RWMutex
}

func NewReputationManager(config *ShoalPPConfig) *ReputationManager {
	return &ReputationManager{
		reputations: make(map[types.NodeID]*LeaderReputation),
		config:      config,
	}
}

func (rm *ReputationManager) GetReputation(nodeID types.NodeID) *LeaderReputation {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rep, exists := rm.reputations[nodeID]; exists {
		return rep
	}

	rep := NewLeaderReputation(nodeID)
	rm.reputations[nodeID] = rep
	return rep
}

func (rm *ReputationManager) UpdateReputation(nodeID types.NodeID, latency time.Duration, success bool) {
	rep := rm.GetReputation(nodeID)
	rep.UpdateScore(latency, success)
}

func (rm *ReputationManager) UpdateShoalPPMetrics(nodeID types.NodeID,
	fastCommit, dynamicAnchor, roundTimeout, anchorSkip bool, parallelDAGUtil float64) {
	rep := rm.GetReputation(nodeID)
	rep.UpdateFastCommitMetrics(fastCommit)
	rep.UpdateDynamicAnchorMetrics(dynamicAnchor)
	rep.UpdateRoundTimeoutMetrics(roundTimeout)
	rep.UpdateAnchorSkipMetrics(anchorSkip)
	rep.UpdateParallelDAGMetrics(parallelDAGUtil)
}

func (rm *ReputationManager) GetBestLeaders(count int) []types.NodeID {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	type nodeScore struct {
		nodeID types.NodeID
		score  float64
	}

	var leaders []nodeScore
	for nodeID, rep := range rm.reputations {
		if rep.GetScore() >= rm.config.MinReputationScore {
			leaders = append(leaders, nodeScore{nodeID, rep.GetScore()})
		}
	}

	// Sort by score (descending)
	sort.Slice(leaders, func(i, j int) bool {
		return leaders[i].score > leaders[j].score
	})

	// Return top leaders
	result := make([]types.NodeID, 0, count)
	for i := 0; i < len(leaders) && i < count; i++ {
		result = append(result, leaders[i].nodeID)
	}

	return result
}

// DecayReputations applies decay to all reputations
func (rm *ReputationManager) DecayReputations() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	now := time.Now()

	for _, rep := range rm.reputations {
		rep.mu.Lock()
		if now.Sub(rep.LastUpdate) > rm.config.ReputationWindow {
			// Apply decay
			rep.Score *= (1.0 - rm.config.ReputationDecayRate)
			if rep.Score < rm.config.MinReputationScore {
				rep.Score = rm.config.MinReputationScore
			}
			rep.LastUpdate = now
		}
		rep.mu.Unlock()
	}
}

func (rm *ReputationManager) GetShoalPPStats() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	stats := map[string]interface{}{
		"total_nodes": len(rm.reputations),
	}

	var totalFastCommits, totalDynamicAnchors, totalRoundTimeouts, totalAnchorSkips int64
	var totalParallelDAGUtil float64
	count := 0

	for _, rep := range rm.reputations {
		metrics := rep.GetShoalPPMetrics()
		totalFastCommits += metrics["fast_commit_count"].(int64)
		totalDynamicAnchors += metrics["dynamic_anchor_count"].(int64)
		totalRoundTimeouts += metrics["round_timeout_count"].(int64)
		totalAnchorSkips += metrics["anchor_skip_count"].(int64)
		totalParallelDAGUtil += metrics["parallel_dag_utilization"].(float64)
		count++
	}

	if count > 0 {
		stats["avg_parallel_dag_utilization"] = totalParallelDAGUtil / float64(count)
	}

	stats["total_fast_commits"] = totalFastCommits
	stats["total_dynamic_anchors"] = totalDynamicAnchors
	stats["total_round_timeouts"] = totalRoundTimeouts
	stats["total_anchor_skips"] = totalAnchorSkips

	return stats
}

type CommitRule int

const (
	OriginalCommitRule CommitRule = iota
	FastDirectCommitRule
)

type AnchorCandidate struct {
	NodeID               types.NodeID
	Round                types.Round
	Certificate          *types.Certificate
	Timestamp            time.Time
	CommitRule           CommitRule
	UncertifiedProposals int
	DynamicPriority      float64
	SkipReason           string
	ParallelDAGInstance  int
}

func NewAnchorCandidate(nodeID types.NodeID, round types.Round, cert *types.Certificate) *AnchorCandidate {
	return &AnchorCandidate{
		NodeID:               nodeID,
		Round:                round,
		Certificate:          cert,
		Timestamp:            time.Now(),
		CommitRule:           OriginalCommitRule,
		UncertifiedProposals: 0,
		DynamicPriority:      1.0,
		SkipReason:           "",
		ParallelDAGInstance:  0,
	}
}

func (ac *AnchorCandidate) CanUseFastCommit(threshold int) bool {
	return ac.UncertifiedProposals >= threshold
}

func (ac *AnchorCandidate) ShouldSkip(skipThreshold time.Duration) bool {
	return time.Since(ac.Timestamp) > skipThreshold
}

var (
	ErrPipelineFull         = &ShoalPPError{3001, "pipeline is full"}
	ErrInvalidConfiguration = &ShoalPPError{3002, "invalid shoalpp configuration"}
	ErrReputationNotFound   = &ShoalPPError{3003, "reputation not found for node"}
	ErrTimeoutAdjustment    = &ShoalPPError{3004, "failed to adjust timeout"}
	ErrPipelineOperation    = &ShoalPPError{3005, "pipeline operation failed"}
	ErrMetricsCollection    = &ShoalPPError{3006, "metrics collection failed"}
	ErrFastCommitFailed     = &ShoalPPError{3007, "fast direct commit failed"}
	ErrDynamicAnchorFailed  = &ShoalPPError{3008, "dynamic anchor frequency adjustment failed"}
	ErrParallelDAGFailed    = &ShoalPPError{3009, "parallel DAG operation failed"}
	ErrRoundTimeoutExceeded = &ShoalPPError{3010, "round timeout exceeded"}
	ErrAnchorSkipFailed     = &ShoalPPError{3011, "anchor candidate skipping failed"}
)

type ShoalPPError struct {
	Code    int
	Message string
}

func (e *ShoalPPError) Error() string {
	return e.Message
}
