package shoalpp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

type DynamicAnchorManager struct {
	config  *ShoalPPConfig
	logger  *zap.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
	mu      sync.RWMutex

	anchorSchedule map[types.Round]*AnchorScheduleEntry
	activeAnchors  map[types.NodeID]*ActiveAnchor
	roundTimeouts  map[types.Round]*RoundTimeout
	scheduleMutex  sync.RWMutex

	baseFrequency      float64
	currentFrequency   float64
	frequencyHistory   []FrequencyMeasurement
	adaptiveController *AdaptiveFrequencyController

	totalAnchors       int64
	dynamicAnchors     int64
	timeoutOccurrences int64
	frequencyGain      float64
	avgResponseTime    time.Duration
}

type AnchorScheduleEntry struct {
	Round               types.Round
	ScheduledAnchors    []types.NodeID
	ActualAnchors       []types.NodeID
	Timestamp           time.Time
	Timeout             time.Duration
	Status              AnchorScheduleStatus
	FrequencyMultiplier float64
}

type ActiveAnchor struct {
	NodeID         types.NodeID
	Round          types.Round
	ActivationTime time.Time
	LastActivity   time.Time
	ResponseTime   time.Duration
	IsResponsive   bool
	Priority       float64
	SkipCount      int
}

type AnchorScheduleStatus int

const (
	SchedulePending AnchorScheduleStatus = iota
	ScheduleActive
	ScheduleCompleted
	ScheduleTimedOut
	ScheduleSkipped
)

type FrequencyMeasurement struct {
	Timestamp  time.Time
	Frequency  float64
	Throughput float64
	Latency    time.Duration
	ErrorRate  float64
}

type AdaptiveFrequencyController struct {
	targetThroughput   float64
	targetLatency      time.Duration
	maxFrequency       float64
	minFrequency       float64
	adjustmentRate     float64
	stabilityThreshold time.Duration

	kp, ki, kd     float64
	errorIntegral  float64
	lastError      float64
	lastAdjustment time.Time
}

type DynamicAnchorResult struct {
	NodeID               types.NodeID
	Round                types.Round
	Success              bool
	ResponseTime         time.Duration
	FrequencyImprovement float64
	ThroughputGain       float64
	LatencyReduction     time.Duration
	Error                error
}

func NewDynamicAnchorManager(config *ShoalPPConfig, logger *zap.Logger) *DynamicAnchorManager {
	ctx, cancel := context.WithCancel(context.Background())

	controller := &AdaptiveFrequencyController{
		targetThroughput:   1000.0,
		targetLatency:      time.Millisecond * 50,
		maxFrequency:       5.0,
		minFrequency:       0.2,
		adjustmentRate:     0.1,
		stabilityThreshold: time.Second * 30,
		kp:                 0.5,
		ki:                 0.1,
		kd:                 0.05,
		lastAdjustment:     time.Now(),
	}

	return &DynamicAnchorManager{
		config:             config,
		logger:             logger,
		ctx:                ctx,
		cancel:             cancel,
		anchorSchedule:     make(map[types.Round]*AnchorScheduleEntry),
		activeAnchors:      make(map[types.NodeID]*ActiveAnchor),
		roundTimeouts:      make(map[types.Round]*RoundTimeout),
		baseFrequency:      1.0,
		currentFrequency:   1.0,
		frequencyHistory:   make([]FrequencyMeasurement, 0, 100),
		adaptiveController: controller,
	}
}

func (dam *DynamicAnchorManager) Start() error {
	dam.mu.Lock()
	defer dam.mu.Unlock()

	if dam.running {
		return nil
	}

	dam.logger.Info("Starting Dynamic Anchor Manager")

	dam.wg.Add(1)
	go dam.dynamicAnchorProcessor()

	dam.wg.Add(1)
	go dam.roundTimeoutMonitor()

	dam.wg.Add(1)
	go dam.frequencyController()

	dam.wg.Add(1)
	go dam.performanceMonitor()

	dam.running = true
	dam.logger.Info("Dynamic Anchor Manager started successfully")

	return nil
}

func (dam *DynamicAnchorManager) Stop() {
	dam.mu.Lock()
	defer dam.mu.Unlock()

	if !dam.running {
		return
	}

	dam.logger.Info("Stopping Dynamic Anchor Manager")

	dam.cancel()
	dam.wg.Wait()

	dam.running = false
	dam.logger.Info("Dynamic Anchor Manager stopped")
}

func (dam *DynamicAnchorManager) ProcessDynamicAnchor(candidate *AnchorCandidate) (*DynamicAnchorResult, error) {
	if !dam.config.EnableDynamicAnchorFreq {
		return nil, ErrDynamicAnchorFailed
	}

	start := time.Now()

	shouldInclude := dam.shouldIncludeAnchor(candidate)
	if !shouldInclude {
		return &DynamicAnchorResult{
			NodeID:  candidate.NodeID,
			Round:   candidate.Round,
			Success: false,
			Error:   ErrDynamicAnchorFailed,
		}, nil
	}

	result := dam.executeDynamicAnchor(candidate, start)

	dam.updateMetrics(result)

	return result, nil
}

func (dam *DynamicAnchorManager) dynamicAnchorProcessor() {
	defer dam.wg.Done()

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-dam.ctx.Done():
			return
		case <-ticker.C:
			dam.processScheduledAnchors()
			dam.adjustAnchorFrequency()
		}
	}
}

func (dam *DynamicAnchorManager) processScheduledAnchors() {
	dam.scheduleMutex.Lock()
	defer dam.scheduleMutex.Unlock()

	now := time.Now()

	for round, entry := range dam.anchorSchedule {
		if entry.Status == SchedulePending {
			if now.Sub(entry.Timestamp) >= time.Duration(0) {
				entry.Status = ScheduleActive
				dam.activateAnchorSchedule(entry)
			}
		} else if entry.Status == ScheduleActive {
			if now.Sub(entry.Timestamp) > entry.Timeout {
				entry.Status = ScheduleTimedOut
				dam.handleScheduleTimeout(entry)
			}
		}
	}
}

func (dam *DynamicAnchorManager) activateAnchorSchedule(entry *AnchorScheduleEntry) {
	dam.logger.Debug("Activating anchor schedule",
		zap.Uint64("round", uint64(entry.Round)),
		zap.Int("scheduled_anchors", len(entry.ScheduledAnchors)),
		zap.Float64("frequency_multiplier", entry.FrequencyMultiplier),
	)

	for _, nodeID := range entry.ScheduledAnchors {
		anchor := &ActiveAnchor{
			NodeID:         nodeID,
			Round:          entry.Round,
			ActivationTime: time.Now(),
			LastActivity:   time.Now(),
			IsResponsive:   true,
			Priority:       1.0,
		}
		dam.activeAnchors[nodeID] = anchor
	}
}

func (dam *DynamicAnchorManager) handleScheduleTimeout(entry *AnchorScheduleEntry) {
	dam.logger.Warn("Anchor schedule timed out",
		zap.Uint64("round", uint64(entry.Round)),
		zap.Duration("timeout", entry.Timeout),
	)

	atomic.AddInt64(&dam.timeoutOccurrences, 1)

	for _, nodeID := range entry.ScheduledAnchors {
		if anchor, exists := dam.activeAnchors[nodeID]; exists {
			anchor.IsResponsive = false
			anchor.SkipCount++
		}
	}
}

func (dam *DynamicAnchorManager) adjustAnchorFrequency() {
	currentMetrics := dam.getCurrentMetrics()

	measurement := FrequencyMeasurement{
		Timestamp:  time.Now(),
		Frequency:  dam.currentFrequency,
		Throughput: currentMetrics.throughput,
		Latency:    currentMetrics.latency,
		ErrorRate:  currentMetrics.errorRate,
	}

	dam.addFrequencyMeasurement(measurement)

	newFrequency := dam.adaptiveController.adjustFrequency(currentMetrics, dam.currentFrequency)

	if newFrequency != dam.currentFrequency {
		dam.logger.Debug("Adjusting anchor frequency",
			zap.Float64("old_frequency", dam.currentFrequency),
			zap.Float64("new_frequency", newFrequency),
			zap.Float64("throughput", currentMetrics.throughput),
			zap.Duration("latency", currentMetrics.latency),
		)

		dam.currentFrequency = newFrequency
		dam.frequencyGain = newFrequency / dam.baseFrequency
	}
}

func (dam *DynamicAnchorManager) shouldIncludeAnchor(candidate *AnchorCandidate) bool {
	if dam.config.RoundTimeoutEnabled {
		if dam.isRoundTimedOut(candidate.Round) {
			return false
		}
	}

	scheduleProbability := dam.calculateInclusionProbability(candidate)

	hash := candidate.Certificate.Hash.String()
	hashValue := dam.hashToProbability(hash)

	return hashValue < scheduleProbability
}

func (dam *DynamicAnchorManager) calculateInclusionProbability(candidate *AnchorCandidate) float64 {
	baseProbability := 1.0 / dam.baseFrequency
	adjustedProbability := baseProbability * dam.currentFrequency

	if anchor, exists := dam.activeAnchors[candidate.NodeID]; exists {
		if !anchor.IsResponsive {
			adjustedProbability *= 0.5
		}

		if anchor.SkipCount > 3 {
			adjustedProbability *= 0.3
		}
	}

	if adjustedProbability > 1.0 {
		adjustedProbability = 1.0
	} else if adjustedProbability < 0.0 {
		adjustedProbability = 0.0
	}

	return adjustedProbability
}

func (dam *DynamicAnchorManager) hashToProbability(hash string) float64 {
	sum := 0
	for _, b := range hash {
		sum += int(b)
	}
	return float64(sum%1000) / 1000.0
}

func (dam *DynamicAnchorManager) executeDynamicAnchor(candidate *AnchorCandidate, start time.Time) *DynamicAnchorResult {
	processingTime := dam.calculateDynamicProcessingTime(candidate)
	time.Sleep(processingTime)

	responseTime := time.Since(start)

	frequencyImprovement := dam.frequencyGain
	throughputGain := frequencyImprovement * 1.2
	latencyReduction := time.Duration(float64(responseTime) * (1.0 - (1.0 / frequencyImprovement)))

	result := &DynamicAnchorResult{
		NodeID:               candidate.NodeID,
		Round:                candidate.Round,
		Success:              true,
		ResponseTime:         responseTime,
		FrequencyImprovement: frequencyImprovement,
		ThroughputGain:       throughputGain,
		LatencyReduction:     latencyReduction,
	}

	if anchor, exists := dam.activeAnchors[candidate.NodeID]; exists {
		anchor.LastActivity = time.Now()
		anchor.ResponseTime = responseTime
		anchor.IsResponsive = responseTime < dam.config.ResponsivenessThreshold
	}

	atomic.AddInt64(&dam.dynamicAnchors, 1)

	return result
}

func (dam *DynamicAnchorManager) calculateDynamicProcessingTime(candidate *AnchorCandidate) time.Duration {
	baseTime := time.Millisecond * 30

	frequencyFactor := 1.0 / dam.currentFrequency
	if frequencyFactor < 0.5 {
		frequencyFactor = 0.5
	}

	return time.Duration(float64(baseTime) * frequencyFactor)
}

func (dam *DynamicAnchorManager) roundTimeoutMonitor() {
	defer dam.wg.Done()

	ticker := time.NewTicker(dam.config.RoundTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-dam.ctx.Done():
			return
		case <-ticker.C:
			dam.checkRoundTimeouts()
		}
	}
}

func (dam *DynamicAnchorManager) checkRoundTimeouts() {
	dam.scheduleMutex.Lock()
	defer dam.scheduleMutex.Unlock()

	now := time.Now()
	toDelete := make([]types.Round, 0)

	for round, timeout := range dam.roundTimeouts {
		if now.Sub(timeout.StartTime) > timeout.Timeout {
			timeout.IsExpired = true

			for _, callback := range timeout.Callbacks {
				callback(timeout.RoundID)
			}

			toDelete = append(toDelete, round)

			dam.logger.Debug("Round timeout expired",
				zap.String("round_id", timeout.RoundID),
				zap.Duration("timeout", timeout.Timeout),
			)
		}
	}

	for _, round := range toDelete {
		delete(dam.roundTimeouts, round)
	}
}

func (dam *DynamicAnchorManager) isRoundTimedOut(round types.Round) bool {
	dam.scheduleMutex.RLock()
	defer dam.scheduleMutex.RUnlock()

	if timeout, exists := dam.roundTimeouts[round]; exists {
		return timeout.IsExpired
	}
	return false
}

func (dam *DynamicAnchorManager) SetRoundTimeout(round types.Round, timeout time.Duration, callback func(string)) {
	dam.scheduleMutex.Lock()
	defer dam.scheduleMutex.Unlock()

	roundTimeout := &RoundTimeout{
		RoundID:   string(round),
		Timeout:   timeout,
		StartTime: time.Now(),
		IsExpired: false,
		Callbacks: []func(string){callback},
	}

	dam.roundTimeouts[round] = roundTimeout
}

func (dam *DynamicAnchorManager) frequencyController() {
	defer dam.wg.Done()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-dam.ctx.Done():
			return
		case <-ticker.C:
			dam.runFrequencyControl()
		}
	}
}

func (dam *DynamicAnchorManager) runFrequencyControl() {
}

func (dam *DynamicAnchorManager) performanceMonitor() {
	defer dam.wg.Done()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-dam.ctx.Done():
			return
		case <-ticker.C:
			dam.logPerformanceMetrics()
		}
	}
}

func (dam *DynamicAnchorManager) logPerformanceMetrics() {
	metrics := dam.getCurrentMetrics()

	dam.logger.Debug("Dynamic anchor performance metrics",
		zap.Float64("current_frequency", dam.currentFrequency),
		zap.Float64("frequency_gain", dam.frequencyGain),
		zap.Float64("throughput", metrics.throughput),
		zap.Duration("latency", metrics.latency),
		zap.Float64("error_rate", metrics.errorRate),
		zap.Int64("total_anchors", atomic.LoadInt64(&dam.totalAnchors)),
		zap.Int64("dynamic_anchors", atomic.LoadInt64(&dam.dynamicAnchors)),
	)
}

func (dam *DynamicAnchorManager) getCurrentMetrics() struct {
	throughput float64
	latency    time.Duration
	errorRate  float64
} {
	totalAnchors := atomic.LoadInt64(&dam.totalAnchors)
	timeoutOccurrences := atomic.LoadInt64(&dam.timeoutOccurrences)

	throughput := float64(totalAnchors) / time.Since(time.Now().Add(-time.Minute)).Seconds()
	errorRate := 0.0
	if totalAnchors > 0 {
		errorRate = float64(timeoutOccurrences) / float64(totalAnchors)
	}

	return struct {
		throughput float64
		latency    time.Duration
		errorRate  float64
	}{
		throughput: throughput,
		latency:    dam.avgResponseTime,
		errorRate:  errorRate,
	}
}

func (dam *DynamicAnchorManager) addFrequencyMeasurement(measurement FrequencyMeasurement) {
	dam.frequencyHistory = append(dam.frequencyHistory, measurement)
	if len(dam.frequencyHistory) > 100 {
		dam.frequencyHistory = dam.frequencyHistory[1:]
	}
}

func (afc *AdaptiveFrequencyController) adjustFrequency(metrics struct {
	throughput float64
	latency    time.Duration
	errorRate  float64
}, currentFrequency float64) float64 {

	now := time.Now()
	dt := now.Sub(afc.lastAdjustment).Seconds()

	if dt < 1.0 {
		return currentFrequency
	}

	throughputError := (afc.targetThroughput - metrics.throughput) / afc.targetThroughput
	latencyError := (metrics.latency.Seconds() - afc.targetLatency.Seconds()) / afc.targetLatency.Seconds()

	error := throughputError*0.6 + latencyError*0.4

	proportional := afc.kp * error
	afc.errorIntegral += error * dt
	integral := afc.ki * afc.errorIntegral
	derivative := afc.kd * (error - afc.lastError) / dt

	pidOutput := proportional + integral + derivative

	adjustment := pidOutput * afc.adjustmentRate
	newFrequency := currentFrequency * (1.0 + adjustment)

	if newFrequency > afc.maxFrequency {
		newFrequency = afc.maxFrequency
	} else if newFrequency < afc.minFrequency {
		newFrequency = afc.minFrequency
	}

	afc.lastError = error
	afc.lastAdjustment = now

	return newFrequency
}

func (dam *DynamicAnchorManager) updateMetrics(result *DynamicAnchorResult) {
	atomic.AddInt64(&dam.totalAnchors, 1)

	if result.Success {
		alpha := 0.1
		if dam.avgResponseTime == 0 {
			dam.avgResponseTime = result.ResponseTime
		} else {
			dam.avgResponseTime = time.Duration(
				float64(dam.avgResponseTime)*(1-alpha) + float64(result.ResponseTime)*alpha,
			)
		}
	}
}

func (dam *DynamicAnchorManager) GetStats() map[string]interface{} {
	dam.scheduleMutex.RLock()
	scheduleCount := len(dam.anchorSchedule)
	activeCount := len(dam.activeAnchors)
	timeoutCount := len(dam.roundTimeouts)
	dam.scheduleMutex.RUnlock()

	totalAnchors := atomic.LoadInt64(&dam.totalAnchors)
	dynamicAnchors := atomic.LoadInt64(&dam.dynamicAnchors)
	timeoutOccurrences := atomic.LoadInt64(&dam.timeoutOccurrences)

	utilizationRate := 0.0
	if totalAnchors > 0 {
		utilizationRate = float64(dynamicAnchors) / float64(totalAnchors)
	}

	return map[string]interface{}{
		"total_anchors":         totalAnchors,
		"dynamic_anchors":       dynamicAnchors,
		"timeout_occurrences":   timeoutOccurrences,
		"utilization_rate":      utilizationRate,
		"current_frequency":     dam.currentFrequency,
		"base_frequency":        dam.baseFrequency,
		"frequency_improvement": dam.frequencyGain,
		"scheduled_entries":     scheduleCount,
		"active_anchors":        activeCount,
		"pending_timeouts":      timeoutCount,
		"avg_response_time":     dam.avgResponseTime,
		"enabled":               dam.config.EnableDynamicAnchorFreq,
		"round_timeout_enabled": dam.config.RoundTimeoutEnabled,
		"round_timeout_ms":      dam.config.RoundTimeout.Milliseconds(),
	}
}
