package shoal

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

// ShoalConfig contains configuration for the Shoal framework
type ShoalConfig struct {
	// Leader reputation settings
	EnableLeaderReputation bool
	ReputationWindow       time.Duration
	ReputationDecayRate    float64
	MinReputationScore     float64

	// Adaptive timeout settings
	EnableAdaptiveTimeouts bool
	BaseTimeout            time.Duration
	MaxTimeout             time.Duration
	MinTimeout             time.Duration
	TimeoutAdjustmentRate  float64

	// Pipelining settings
	EnablePipelining bool
	PipelineDepth    int
	MaxConcurrentOps int

	// Performance monitoring
	EnableMetrics   bool
	MetricsInterval time.Duration

	// Prevalent responsiveness
	EnablePrevalentResponsiveness bool
	ResponsivenessThreshold       time.Duration
}

// DefaultShoalConfig returns the default Shoal configuration
func DefaultShoalConfig() *ShoalConfig {
	return &ShoalConfig{
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
	}
}

// LeaderReputation tracks the reputation of a leader node
type LeaderReputation struct {
	NodeID     types.NodeID
	Score      float64
	LastUpdate time.Time

	// Performance metrics
	AvgLatency   time.Duration
	SuccessRate  float64
	MessageCount int64
	FailureCount int64

	// Network metrics
	ConnectivityScore float64
	BandwidthScore    float64

	// Temporal metrics
	WindowStart time.Time
	WindowEnd   time.Time

	mu sync.RWMutex
}

// NewLeaderReputation creates a new leader reputation tracker
func NewLeaderReputation(nodeID types.NodeID) *LeaderReputation {
	now := time.Now()
	return &LeaderReputation{
		NodeID:            nodeID,
		Score:             1.0, // Start with perfect score
		LastUpdate:        now,
		AvgLatency:        0,
		SuccessRate:       1.0,
		MessageCount:      0,
		FailureCount:      0,
		ConnectivityScore: 1.0,
		BandwidthScore:    1.0,
		WindowStart:       now,
		WindowEnd:         now.Add(time.Minute * 10),
	}
}

// UpdateScore updates the reputation score based on recent performance
func (lr *LeaderReputation) UpdateScore(latency time.Duration, success bool) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	now := time.Now()

	// Update message count
	lr.MessageCount++
	if !success {
		lr.FailureCount++
	}

	// Update success rate
	lr.SuccessRate = 1.0 - (float64(lr.FailureCount) / float64(lr.MessageCount))

	// Update average latency (exponential moving average)
	alpha := 0.1
	if lr.AvgLatency == 0 {
		lr.AvgLatency = latency
	} else {
		lr.AvgLatency = time.Duration(float64(lr.AvgLatency)*(1-alpha) + float64(latency)*alpha)
	}

	// Calculate composite score
	latencyScore := calculateLatencyScore(latency)
	connectivityScore := lr.ConnectivityScore
	bandwidthScore := lr.BandwidthScore

	// Byzantine fault detection: severe penalty for suspicious patterns
	byzantinePenalty := lr.calculateByzantinePenalty()

	// Weighted combination of scores with Byzantine penalty
	baseScore := (lr.SuccessRate*0.4 + latencyScore*0.3 + connectivityScore*0.2 + bandwidthScore*0.1)
	lr.Score = baseScore * (1.0 - byzantinePenalty)

	// Ensure score doesn't go negative
	if lr.Score < 0.0 {
		lr.Score = 0.0
	}

	lr.LastUpdate = now
}

// calculateByzantinePenalty detects and penalizes Byzantine behavior patterns
func (lr *LeaderReputation) calculateByzantinePenalty() float64 {
	// Pattern 1: Extremely high failure rate (>50%)
	if lr.SuccessRate < 0.5 && lr.MessageCount > 10 {
		return 0.8 // 80% penalty
	}

	// Pattern 2: Suspiciously consistent latency (possible replay attack)
	if lr.MessageCount > 20 {
		// If latency is too consistent, it might be artificial
		recentLatencyVariance := float64(lr.AvgLatency) * 0.1 // Expected 10% variance
		if recentLatencyVariance < float64(time.Millisecond) {
			return 0.3 // 30% penalty for suspicious consistency
		}
	}

	// Pattern 3: Extreme latency spikes (>10x average)
	maxAcceptableLatency := lr.AvgLatency * 10
	if lr.AvgLatency > maxAcceptableLatency && lr.MessageCount > 5 {
		return 0.5 // 50% penalty for extreme latency
	}

	return 0.0 // No Byzantine behavior detected
}

// calculateLatencyScore converts latency to a score between 0 and 1
func calculateLatencyScore(latency time.Duration) float64 {
	// Convert latency to score: lower latency = higher score
	latencyMs := float64(latency.Nanoseconds()) / 1000000.0

	// Exceptional performance (sub-millisecond)
	if latencyMs <= 1.0 {
		return 1.0
	}

	// Exponential decay for better discrimination
	// Good: 1-100ms, Acceptable: 100ms-1s, Poor: >1s
	if latencyMs <= 100.0 {
		// Linear decay from 1.0 to 0.8 for 1-100ms
		return 1.0 - 0.2*(latencyMs-1.0)/99.0
	} else if latencyMs <= 1000.0 {
		// Exponential decay from 0.8 to 0.3 for 100ms-1s
		progress := (latencyMs - 100.0) / 900.0
		return 0.8 * (1.0 - progress*progress)
	} else {
		// Harsh penalty for >1s latency
		return 0.1 / (1.0 + latencyMs/1000.0)
	}
}

// GetScore returns the current reputation score
func (lr *LeaderReputation) GetScore() float64 {
	lr.mu.RLock()
	defer lr.mu.RUnlock()
	return lr.Score
}

// ReputationManager manages leader reputations for the network
type ReputationManager struct {
	reputations map[types.NodeID]*LeaderReputation
	config      *ShoalConfig
	mu          sync.RWMutex
}

// NewReputationManager creates a new reputation manager
func NewReputationManager(config *ShoalConfig) *ReputationManager {
	return &ReputationManager{
		reputations: make(map[types.NodeID]*LeaderReputation),
		config:      config,
	}
}

// GetReputation gets or creates a reputation for a node
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

// UpdateReputation updates the reputation for a node
func (rm *ReputationManager) UpdateReputation(nodeID types.NodeID, latency time.Duration, success bool) {
	rep := rm.GetReputation(nodeID)
	rep.UpdateScore(latency, success)
}

// GetBestLeaders returns the best leaders sorted by reputation score
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

	// Sort by score (descending) - O(n log n)
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

// AdaptiveTimeout manages adaptive timeout values
type AdaptiveTimeout struct {
	baseTimeout    time.Duration
	currentTimeout time.Duration
	maxTimeout     time.Duration
	minTimeout     time.Duration
	adjustmentRate float64

	// Statistics for adjustment
	recentLatencies []time.Duration
	avgLatency      time.Duration

	mu sync.RWMutex
}

// NewAdaptiveTimeout creates a new adaptive timeout manager
func NewAdaptiveTimeout(config *ShoalConfig) *AdaptiveTimeout {
	return &AdaptiveTimeout{
		baseTimeout:     config.BaseTimeout,
		currentTimeout:  config.BaseTimeout,
		maxTimeout:      config.MaxTimeout,
		minTimeout:      config.MinTimeout,
		adjustmentRate:  config.TimeoutAdjustmentRate,
		recentLatencies: make([]time.Duration, 0, 10), // Keep last 10 latencies
	}
}

// UpdateLatency updates the timeout based on observed latency
func (at *AdaptiveTimeout) UpdateLatency(latency time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	if at.avgLatency > 0 && latency > at.avgLatency*10 {
		return
	}

	at.recentLatencies = append(at.recentLatencies, latency)
	if len(at.recentLatencies) > 10 {
		at.recentLatencies = at.recentLatencies[1:]
	}

	total := time.Duration(0)
	for _, l := range at.recentLatencies {
		total += l
	}
	at.avgLatency = total / time.Duration(len(at.recentLatencies))

	p95Latency := at.calculateP95()

	targetTimeout := time.Duration(float64(p95Latency) * 1.5)

	diff := targetTimeout - at.currentTimeout

	// Faster adjustment when timeout is too low, slower when too high
	adjustmentRate := at.adjustmentRate
	if targetTimeout > at.currentTimeout {
		adjustmentRate *= 1.5 // Faster increase
	} else {
		adjustmentRate *= 0.7 // Slower decrease
	}

	adjustment := time.Duration(float64(diff) * adjustmentRate)
	at.currentTimeout += adjustment

	// Enforce bounds with hysteresis
	if at.currentTimeout > at.maxTimeout {
		at.currentTimeout = at.maxTimeout
	} else if at.currentTimeout < at.minTimeout {
		at.currentTimeout = at.minTimeout
	}
}

// calculateP95 calculates the 95th percentile latency
func (at *AdaptiveTimeout) calculateP95() time.Duration {
	if len(at.recentLatencies) < 3 {
		return at.avgLatency
	}

	// Create sorted copy
	sorted := make([]time.Duration, len(at.recentLatencies))
	copy(sorted, at.recentLatencies)

	// Simple insertion sort for small arrays
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		j := i - 1
		for j >= 0 && sorted[j] > key {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}

	// Calculate P95 index
	p95Index := int(float64(len(sorted)) * 0.95)
	if p95Index >= len(sorted) {
		p95Index = len(sorted) - 1
	}

	return sorted[p95Index]
}

// GetTimeout returns the current adaptive timeout
func (at *AdaptiveTimeout) GetTimeout() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return at.currentTimeout
}

// GetAverageLatency returns the current average latency
func (at *AdaptiveTimeout) GetAverageLatency() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return at.avgLatency
}

// PipelineManager manages pipelining of operations
type PipelineManager struct {
	config          *ShoalConfig
	activePipelines map[string]*Pipeline
	mu              sync.RWMutex
}

// Pipeline represents a processing pipeline
type Pipeline struct {
	ID         string
	Operations chan PipelineOperation
	Results    chan PipelineResult
	Workers    int
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	// Performance optimization
	resultPool sync.Pool // Reuse PipelineResult objects
	metrics    struct {
		processedOps int64
		avgDuration  time.Duration
		mu           sync.RWMutex
	}
}

// PipelineOperation represents an operation in the pipeline
type PipelineOperation struct {
	ID       string
	Type     string
	Data     interface{}
	Callback func(result PipelineResult)
}

// PipelineResult represents the result of a pipeline operation
type PipelineResult struct {
	OperationID string
	Success     bool
	Data        interface{}
	Error       error
	Duration    time.Duration
}

// NewPipelineManager creates a new pipeline manager
func NewPipelineManager(config *ShoalConfig) *PipelineManager {
	return &PipelineManager{
		config:          config,
		activePipelines: make(map[string]*Pipeline),
	}
}

// CreatePipeline creates a new processing pipeline
func (pm *PipelineManager) CreatePipeline(id string, workers int) *Pipeline {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	pipeline := &Pipeline{
		ID:         id,
		Operations: make(chan PipelineOperation, pm.config.PipelineDepth),
		Results:    make(chan PipelineResult, pm.config.PipelineDepth),
		Workers:    workers,
		ctx:        ctx,
		cancel:     cancel,
		resultPool: sync.Pool{
			New: func() interface{} {
				return &PipelineResult{}
			},
		},
	}

	// Start workers
	for i := 0; i < workers; i++ {
		pipeline.wg.Add(1)
		go pipeline.worker()
	}

	pm.activePipelines[id] = pipeline
	return pipeline
}

// worker processes operations in the pipeline
func (p *Pipeline) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case op := <-p.Operations:
			start := time.Now()

			// Get result from pool to reduce GC pressure
			result := p.resultPool.Get().(*PipelineResult)
			result.OperationID = op.ID
			result.Success = true
			result.Data = op.Data
			result.Error = nil
			result.Duration = time.Since(start)

			// Update pipeline metrics
			p.updateMetrics(result.Duration)

			// Send result
			select {
			case p.Results <- *result:
			case <-p.ctx.Done():
				p.resultPool.Put(result) // Return to pool on cancellation
				return
			}

			// Call callback if provided
			if op.Callback != nil {
				op.Callback(*result)
			}

			// Return result to pool
			p.resultPool.Put(result)
		}
	}
}

// updateMetrics updates pipeline performance metrics
func (p *Pipeline) updateMetrics(duration time.Duration) {
	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()

	p.metrics.processedOps++

	// Exponential moving average for duration
	alpha := 0.1
	if p.metrics.avgDuration == 0 {
		p.metrics.avgDuration = duration
	} else {
		p.metrics.avgDuration = time.Duration(
			float64(p.metrics.avgDuration)*(1-alpha) + float64(duration)*alpha,
		)
	}
}

// SubmitOperation submits an operation to the pipeline
func (p *Pipeline) SubmitOperation(op PipelineOperation) error {
	select {
	case p.Operations <- op:
		return nil
	case <-p.ctx.Done():
		return context.Canceled
	default:
		return ErrPipelineFull
	}
}

// Stop stops the pipeline
func (p *Pipeline) Stop() {
	p.cancel()
	close(p.Operations)
	p.wg.Wait()
	close(p.Results)
}

// GetPipeline gets a pipeline by ID
func (pm *PipelineManager) GetPipeline(id string) (*Pipeline, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	pipeline, exists := pm.activePipelines[id]
	return pipeline, exists
}

// StopPipeline stops a pipeline
func (pm *PipelineManager) StopPipeline(id string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pipeline, exists := pm.activePipelines[id]; exists {
		pipeline.Stop()
		delete(pm.activePipelines, id)
	}
}

// StopAll stops all pipelines
func (pm *PipelineManager) StopAll() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for id, pipeline := range pm.activePipelines {
		pipeline.Stop()
		delete(pm.activePipelines, id)
	}
}

// PerformanceMetrics tracks performance metrics
type PerformanceMetrics struct {
	// Latency metrics
	AvgLatency time.Duration
	MinLatency time.Duration
	MaxLatency time.Duration
	LatencyP95 time.Duration
	LatencyP99 time.Duration

	// Throughput metrics
	MessagesPerSecond float64
	BytesPerSecond    float64

	// Error metrics
	ErrorRate  float64
	ErrorCount int64
	TotalCount int64

	// Resource metrics
	CPUUsage    float64
	MemoryUsage int64

	// Consensus metrics
	BlockLatency        time.Duration
	CertificateLatency  time.Duration
	AnchorSelectionTime time.Duration

	// Shoal-specific metrics
	ReputationScores        map[types.NodeID]float64
	AdaptiveTimeoutValue    time.Duration
	PipelineUtilization     float64
	PrevalentResponsiveness bool

	LastUpdate time.Time
	mu         sync.RWMutex
}

// NewPerformanceMetrics creates a new performance metrics tracker
func NewPerformanceMetrics() *PerformanceMetrics {
	return &PerformanceMetrics{
		MinLatency:       time.Hour, // Start with high value
		ReputationScores: make(map[types.NodeID]float64),
		LastUpdate:       time.Now(),
	}
}

// UpdateLatency updates latency metrics
func (pm *PerformanceMetrics) UpdateLatency(latency time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Update basic metrics
	if latency > pm.MaxLatency {
		pm.MaxLatency = latency
	}
	if latency < pm.MinLatency {
		pm.MinLatency = latency
	}

	// Update average (exponential moving average)
	alpha := 0.1
	if pm.AvgLatency == 0 {
		pm.AvgLatency = latency
	} else {
		pm.AvgLatency = time.Duration(float64(pm.AvgLatency)*(1-alpha) + float64(latency)*alpha)
	}

	pm.LastUpdate = time.Now()
}

// UpdateThroughput updates throughput metrics
func (pm *PerformanceMetrics) UpdateThroughput(messages int64, bytes int64, duration time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	seconds := duration.Seconds()
	if seconds > 0 {
		pm.MessagesPerSecond = float64(messages) / seconds
		pm.BytesPerSecond = float64(bytes) / seconds
	}

	pm.LastUpdate = time.Now()
}

// UpdateError updates error metrics
func (pm *PerformanceMetrics) UpdateError(isError bool) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.TotalCount++
	if isError {
		pm.ErrorCount++
	}

	pm.ErrorRate = float64(pm.ErrorCount) / float64(pm.TotalCount)
	pm.LastUpdate = time.Now()
}

// GetSnapshot returns a snapshot of current metrics
func (pm *PerformanceMetrics) GetSnapshot() PerformanceMetrics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Create a copy of the metrics
	snapshot := *pm
	snapshot.ReputationScores = make(map[types.NodeID]float64)
	for k, v := range pm.ReputationScores {
		snapshot.ReputationScores[k] = v
	}

	return snapshot
}

// Error definitions for Shoal
var (
	ErrPipelineFull         = &ShoalError{2001, "pipeline is full"}
	ErrInvalidConfiguration = &ShoalError{2002, "invalid shoal configuration"}
	ErrReputationNotFound   = &ShoalError{2003, "reputation not found for node"}
	ErrTimeoutAdjustment    = &ShoalError{2004, "failed to adjust timeout"}
	ErrPipelineOperation    = &ShoalError{2005, "pipeline operation failed"}
	ErrMetricsCollection    = &ShoalError{2006, "metrics collection failed"}
)

// ShoalError represents errors in the Shoal framework
type ShoalError struct {
	Code    int
	Message string
}

func (e *ShoalError) Error() string {
	return e.Message
}
