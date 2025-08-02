package shoalpp

import (
	"sync"
	"time"
)

type AdaptiveTimeout struct {
	baseTimeout    time.Duration
	currentTimeout time.Duration
	maxTimeout     time.Duration
	minTimeout     time.Duration
	adjustmentRate float64

	// Statistics for adjustment
	recentLatencies []time.Duration
	avgLatency      time.Duration

	// Shoal++ enhancements
	fastCommitTimeouts    []time.Duration
	dynamicAnchorTimeouts []time.Duration
	parallelDAGTimeouts   []time.Duration
	roundTimeouts         map[string]time.Duration // Track timeouts per round

	// Enhanced statistics
	latencyP50 time.Duration
	latencyP95 time.Duration
	latencyP99 time.Duration

	// Network condition awareness
	networkCongestionFactor float64
	byzantineFaultFactor    float64

	mu sync.RWMutex
}

// NewAdaptiveTimeout creates a new adaptive timeout manager with Shoal++ enhancements
func NewAdaptiveTimeout(config *ShoalPPConfig) *AdaptiveTimeout {
	return &AdaptiveTimeout{
		baseTimeout:             config.BaseTimeout,
		currentTimeout:          config.BaseTimeout,
		maxTimeout:              config.MaxTimeout,
		minTimeout:              config.MinTimeout,
		adjustmentRate:          config.TimeoutAdjustmentRate,
		recentLatencies:         make([]time.Duration, 0, 20), // Increased capacity for better statistics
		fastCommitTimeouts:      make([]time.Duration, 0, 10),
		dynamicAnchorTimeouts:   make([]time.Duration, 0, 10),
		parallelDAGTimeouts:     make([]time.Duration, 0, 10),
		roundTimeouts:           make(map[string]time.Duration),
		networkCongestionFactor: 1.0,
		byzantineFaultFactor:    1.0,
	}
}

// UpdateLatency updates the timeout based on observed latency with Shoal++ enhancements
func (at *AdaptiveTimeout) UpdateLatency(latency time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	// Filter out extreme outliers (more than 50x average)
	if at.avgLatency > 0 && latency > at.avgLatency*50 {
		return
	}

	at.recentLatencies = append(at.recentLatencies, latency)
	if len(at.recentLatencies) > 20 {
		at.recentLatencies = at.recentLatencies[1:]
	}

	// Calculate enhanced statistics
	at.calculateEnhancedStatistics()

	// Use P95 latency for timeout calculation (more robust than P99)
	targetTimeout := time.Duration(float64(at.latencyP95) * 2.0) // 2x P95 for safety margin

	// Apply network condition adjustments
	targetTimeout = time.Duration(float64(targetTimeout) * at.networkCongestionFactor * at.byzantineFaultFactor)

	// Calculate adjustment with enhanced responsiveness
	diff := targetTimeout - at.currentTimeout

	// Faster adjustment when timeout is too low, slower when too high
	adjustmentRate := at.adjustmentRate
	if targetTimeout > at.currentTimeout {
		adjustmentRate *= 2.0 // Faster increase for safety
	} else {
		adjustmentRate *= 0.5 // Slower decrease to avoid oscillation
	}

	adjustment := time.Duration(float64(diff) * adjustmentRate)
	at.currentTimeout += adjustment

	// Enforce bounds with enhanced hysteresis
	if at.currentTimeout > at.maxTimeout {
		at.currentTimeout = at.maxTimeout
	} else if at.currentTimeout < at.minTimeout {
		at.currentTimeout = at.minTimeout
	}
}

// UpdateFastCommitLatency updates latency specifically for fast commit operations
func (at *AdaptiveTimeout) UpdateFastCommitLatency(latency time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.fastCommitTimeouts = append(at.fastCommitTimeouts, latency)
	if len(at.fastCommitTimeouts) > 10 {
		at.fastCommitTimeouts = at.fastCommitTimeouts[1:]
	}
}

// UpdateDynamicAnchorLatency updates latency for dynamic anchor operations
func (at *AdaptiveTimeout) UpdateDynamicAnchorLatency(latency time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.dynamicAnchorTimeouts = append(at.dynamicAnchorTimeouts, latency)
	if len(at.dynamicAnchorTimeouts) > 10 {
		at.dynamicAnchorTimeouts = at.dynamicAnchorTimeouts[1:]
	}
}

// UpdateParallelDAGLatency updates latency for parallel DAG operations
func (at *AdaptiveTimeout) UpdateParallelDAGLatency(latency time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.parallelDAGTimeouts = append(at.parallelDAGTimeouts, latency)
	if len(at.parallelDAGTimeouts) > 10 {
		at.parallelDAGTimeouts = at.parallelDAGTimeouts[1:]
	}
}

// SetRoundTimeout sets a specific timeout for a round
func (at *AdaptiveTimeout) SetRoundTimeout(roundID string, timeout time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.roundTimeouts[roundID] = timeout

	// Clean up old round timeouts (keep only last 50)
	if len(at.roundTimeouts) > 50 {
		// Remove oldest entries
		oldestTime := time.Now().Add(-time.Minute * 10)
		for roundID := range at.roundTimeouts {
			// Simple cleanup based on round ID pattern (assuming timestamp in ID)
			// In a real implementation, you'd have a proper cleanup mechanism
			if len(at.roundTimeouts) <= 50 {
				break
			}
			delete(at.roundTimeouts, roundID)
		}
	}
}

// GetRoundTimeout gets the timeout for a specific round
func (at *AdaptiveTimeout) GetRoundTimeout(roundID string) time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()

	if timeout, exists := at.roundTimeouts[roundID]; exists {
		return timeout
	}
	return at.currentTimeout
}

// UpdateNetworkCongestion updates the network congestion factor
func (at *AdaptiveTimeout) UpdateNetworkCongestion(congestionLevel float64) {
	at.mu.Lock()
	defer at.mu.Unlock()

	// Smooth adjustment of congestion factor
	alpha := 0.1
	at.networkCongestionFactor = at.networkCongestionFactor*(1-alpha) + congestionLevel*alpha

	// Bounds: 0.5x to 3.0x adjustment
	if at.networkCongestionFactor < 0.5 {
		at.networkCongestionFactor = 0.5
	} else if at.networkCongestionFactor > 3.0 {
		at.networkCongestionFactor = 3.0
	}
}

// UpdateByzantineFaultDetection updates the Byzantine fault factor
func (at *AdaptiveTimeout) UpdateByzantineFaultDetection(byzantineLevel float64) {
	at.mu.Lock()
	defer at.mu.Unlock()

	// Smooth adjustment of Byzantine fault factor
	alpha := 0.05 // Slower adjustment for Byzantine detection
	at.byzantineFaultFactor = at.byzantineFaultFactor*(1-alpha) + byzantineLevel*alpha

	// Bounds: 1.0x to 2.0x adjustment (Byzantine behavior requires more conservative timeouts)
	if at.byzantineFaultFactor < 1.0 {
		at.byzantineFaultFactor = 1.0
	} else if at.byzantineFaultFactor > 2.0 {
		at.byzantineFaultFactor = 2.0
	}
}

// calculateEnhancedStatistics calculates enhanced latency statistics
func (at *AdaptiveTimeout) calculateEnhancedStatistics() {
	if len(at.recentLatencies) < 3 {
		at.avgLatency = at.calculateAverage(at.recentLatencies)
		at.latencyP50 = at.avgLatency
		at.latencyP95 = at.avgLatency
		at.latencyP99 = at.avgLatency
		return
	}

	// Calculate average
	at.avgLatency = at.calculateAverage(at.recentLatencies)

	// Calculate percentiles
	sorted := make([]time.Duration, len(at.recentLatencies))
	copy(sorted, at.recentLatencies)
	at.quickSort(sorted, 0, len(sorted)-1)

	at.latencyP50 = at.calculatePercentile(sorted, 0.50)
	at.latencyP95 = at.calculatePercentile(sorted, 0.95)
	at.latencyP99 = at.calculatePercentile(sorted, 0.99)
}

// calculateAverage calculates the average of a slice of durations
func (at *AdaptiveTimeout) calculateAverage(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	total := time.Duration(0)
	for _, d := range durations {
		total += d
	}
	return total / time.Duration(len(durations))
}

// calculatePercentile calculates the percentile of a sorted slice
func (at *AdaptiveTimeout) calculatePercentile(sorted []time.Duration, percentile float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	index := int(float64(len(sorted)) * percentile)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// quickSort implements quicksort for time.Duration slices
func (at *AdaptiveTimeout) quickSort(arr []time.Duration, low, high int) {
	if low < high {
		pi := at.partition(arr, low, high)
		at.quickSort(arr, low, pi-1)
		at.quickSort(arr, pi+1, high)
	}
}

// partition is the partition function for quicksort
func (at *AdaptiveTimeout) partition(arr []time.Duration, low, high int) int {
	pivot := arr[high]
	i := low - 1

	for j := low; j < high; j++ {
		if arr[j] <= pivot {
			i++
			arr[i], arr[j] = arr[j], arr[i]
		}
	}
	arr[i+1], arr[high] = arr[high], arr[i+1]
	return i + 1
}

// GetTimeout returns the current adaptive timeout
func (at *AdaptiveTimeout) GetTimeout() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return at.currentTimeout
}

// GetFastCommitTimeout returns the timeout optimized for fast commit operations
func (at *AdaptiveTimeout) GetFastCommitTimeout() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()

	if len(at.fastCommitTimeouts) > 0 {
		avgFastCommit := at.calculateAverage(at.fastCommitTimeouts)
		// Fast commit should be more aggressive (1.5x average instead of 2x)
		return time.Duration(float64(avgFastCommit) * 1.5)
	}

	// Fallback to standard timeout with reduction for fast commit
	return time.Duration(float64(at.currentTimeout) * 0.75)
}

// GetDynamicAnchorTimeout returns the timeout for dynamic anchor operations
func (at *AdaptiveTimeout) GetDynamicAnchorTimeout() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()

	if len(at.dynamicAnchorTimeouts) > 0 {
		avgDynamicAnchor := at.calculateAverage(at.dynamicAnchorTimeouts)
		return time.Duration(float64(avgDynamicAnchor) * 1.8)
	}

	return at.currentTimeout
}

// GetParallelDAGTimeout returns the timeout for parallel DAG operations
func (at *AdaptiveTimeout) GetParallelDAGTimeout() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()

	if len(at.parallelDAGTimeouts) > 0 {
		avgParallelDAG := at.calculateAverage(at.parallelDAGTimeouts)
		// Parallel DAG operations can be more aggressive due to parallelism
		return time.Duration(float64(avgParallelDAG) * 1.3)
	}

	return time.Duration(float64(at.currentTimeout) * 0.8)
}

// GetAverageLatency returns the current average latency
func (at *AdaptiveTimeout) GetAverageLatency() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return at.avgLatency
}

// GetLatencyStatistics returns comprehensive latency statistics
func (at *AdaptiveTimeout) GetLatencyStatistics() map[string]time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()

	return map[string]time.Duration{
		"average": at.avgLatency,
		"p50":     at.latencyP50,
		"p95":     at.latencyP95,
		"p99":     at.latencyP99,
		"min":     at.getMinLatency(),
		"max":     at.getMaxLatency(),
	}
}

// getMinLatency calculates minimum latency from recent observations
func (at *AdaptiveTimeout) getMinLatency() time.Duration {
	if len(at.recentLatencies) == 0 {
		return 0
	}

	min := at.recentLatencies[0]
	for _, latency := range at.recentLatencies[1:] {
		if latency < min {
			min = latency
		}
	}
	return min
}

// getMaxLatency calculates maximum latency from recent observations
func (at *AdaptiveTimeout) getMaxLatency() time.Duration {
	if len(at.recentLatencies) == 0 {
		return 0
	}

	max := at.recentLatencies[0]
	for _, latency := range at.recentLatencies[1:] {
		if latency > max {
			max = latency
		}
	}
	return max
}

// GetNetworkConditionFactors returns current network condition adjustment factors
func (at *AdaptiveTimeout) GetNetworkConditionFactors() map[string]float64 {
	at.mu.RLock()
	defer at.mu.RUnlock()

	return map[string]float64{
		"congestion_factor":    at.networkCongestionFactor,
		"byzantine_factor":     at.byzantineFaultFactor,
		"effective_multiplier": at.networkCongestionFactor * at.byzantineFaultFactor,
	}
}

// Reset resets the adaptive timeout to its base configuration
func (at *AdaptiveTimeout) Reset() {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.currentTimeout = at.baseTimeout
	at.recentLatencies = at.recentLatencies[:0]
	at.fastCommitTimeouts = at.fastCommitTimeouts[:0]
	at.dynamicAnchorTimeouts = at.dynamicAnchorTimeouts[:0]
	at.parallelDAGTimeouts = at.parallelDAGTimeouts[:0]
	at.roundTimeouts = make(map[string]time.Duration)
	at.networkCongestionFactor = 1.0
	at.byzantineFaultFactor = 1.0
	at.avgLatency = 0
	at.latencyP50 = 0
	at.latencyP95 = 0
	at.latencyP99 = 0
}
