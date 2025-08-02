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

	recentLatencies []time.Duration
	avgLatency      time.Duration

	fastCommitTimeouts    []time.Duration
	dynamicAnchorTimeouts []time.Duration
	parallelDAGTimeouts   []time.Duration
	roundTimeouts         map[string]time.Duration

	latencyP50 time.Duration
	latencyP95 time.Duration
	latencyP99 time.Duration

	networkCongestionFactor float64
	byzantineFaultFactor    float64

	mu sync.RWMutex
}

func NewAdaptiveTimeout(config *ShoalPPConfig) *AdaptiveTimeout {
	return &AdaptiveTimeout{
		baseTimeout:             config.BaseTimeout,
		currentTimeout:          config.BaseTimeout,
		maxTimeout:              config.MaxTimeout,
		minTimeout:              config.MinTimeout,
		adjustmentRate:          config.TimeoutAdjustmentRate,
		recentLatencies:         make([]time.Duration, 0, 20),
		fastCommitTimeouts:      make([]time.Duration, 0, 10),
		dynamicAnchorTimeouts:   make([]time.Duration, 0, 10),
		parallelDAGTimeouts:     make([]time.Duration, 0, 10),
		roundTimeouts:           make(map[string]time.Duration),
		networkCongestionFactor: 1.0,
		byzantineFaultFactor:    1.0,
	}
}

func (at *AdaptiveTimeout) UpdateLatency(latency time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	if at.avgLatency > 0 && latency > at.avgLatency*50 {
		return
	}

	at.recentLatencies = append(at.recentLatencies, latency)
	if len(at.recentLatencies) > 20 {
		at.recentLatencies = at.recentLatencies[1:]
	}

	at.calculateEnhancedStatistics()
	targetTimeout := time.Duration(float64(at.latencyP95) * 2.0)
	targetTimeout = time.Duration(float64(targetTimeout) * at.networkCongestionFactor * at.byzantineFaultFactor)
	diff := targetTimeout - at.currentTimeout
	adjustmentRate := at.adjustmentRate
	if targetTimeout > at.currentTimeout {
		adjustmentRate *= 2.0
	} else {
		adjustmentRate *= 0.5
	}
	adjustment := time.Duration(float64(diff) * adjustmentRate)
	at.currentTimeout += adjustment
	if at.currentTimeout > at.maxTimeout {
		at.currentTimeout = at.maxTimeout
	} else if at.currentTimeout < at.minTimeout {
		at.currentTimeout = at.minTimeout
	}
}

func (at *AdaptiveTimeout) UpdateFastCommitLatency(latency time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.fastCommitTimeouts = append(at.fastCommitTimeouts, latency)
	if len(at.fastCommitTimeouts) > 10 {
		at.fastCommitTimeouts = at.fastCommitTimeouts[1:]
	}
}

func (at *AdaptiveTimeout) UpdateDynamicAnchorLatency(latency time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.dynamicAnchorTimeouts = append(at.dynamicAnchorTimeouts, latency)
	if len(at.dynamicAnchorTimeouts) > 10 {
		at.dynamicAnchorTimeouts = at.dynamicAnchorTimeouts[1:]
	}
}

func (at *AdaptiveTimeout) UpdateParallelDAGLatency(latency time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.parallelDAGTimeouts = append(at.parallelDAGTimeouts, latency)
	if len(at.parallelDAGTimeouts) > 10 {
		at.parallelDAGTimeouts = at.parallelDAGTimeouts[1:]
	}
}

func (at *AdaptiveTimeout) SetRoundTimeout(roundID string, timeout time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.roundTimeouts[roundID] = timeout

	if len(at.roundTimeouts) > 50 {
		oldestTime := time.Now().Add(-time.Minute * 10)
		for roundID := range at.roundTimeouts {
			if len(at.roundTimeouts) <= 50 {
				break
			}
			delete(at.roundTimeouts, roundID)
		}
	}
}

func (at *AdaptiveTimeout) GetRoundTimeout(roundID string) time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()

	if timeout, exists := at.roundTimeouts[roundID]; exists {
		return timeout
	}
	return at.currentTimeout
}

func (at *AdaptiveTimeout) UpdateNetworkCongestion(congestionLevel float64) {
	at.mu.Lock()
	defer at.mu.Unlock()

	alpha := 0.1
	at.networkCongestionFactor = at.networkCongestionFactor*(1-alpha) + congestionLevel*alpha

	if at.networkCongestionFactor < 0.5 {
		at.networkCongestionFactor = 0.5
	} else if at.networkCongestionFactor > 3.0 {
		at.networkCongestionFactor = 3.0
	}
}

func (at *AdaptiveTimeout) UpdateByzantineFaultDetection(byzantineLevel float64) {
	at.mu.Lock()
	defer at.mu.Unlock()

	alpha := 0.05
	at.byzantineFaultFactor = at.byzantineFaultFactor*(1-alpha) + byzantineLevel*alpha

	if at.byzantineFaultFactor < 1.0 {
		at.byzantineFaultFactor = 1.0
	} else if at.byzantineFaultFactor > 2.0 {
		at.byzantineFaultFactor = 2.0
	}
}

func (at *AdaptiveTimeout) calculateEnhancedStatistics() {
	if len(at.recentLatencies) < 3 {
		at.avgLatency = at.calculateAverage(at.recentLatencies)
		at.latencyP50 = at.avgLatency
		at.latencyP95 = at.avgLatency
		at.latencyP99 = at.avgLatency
		return
	}

	at.avgLatency = at.calculateAverage(at.recentLatencies)
	sorted := make([]time.Duration, len(at.recentLatencies))
	copy(sorted, at.recentLatencies)
	at.quickSort(sorted, 0, len(sorted)-1)
	at.latencyP50 = at.calculatePercentile(sorted, 0.50)
	at.latencyP95 = at.calculatePercentile(sorted, 0.95)
	at.latencyP99 = at.calculatePercentile(sorted, 0.99)
}

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

func (at *AdaptiveTimeout) quickSort(arr []time.Duration, low, high int) {
	if low < high {
		pi := at.partition(arr, low, high)
		at.quickSort(arr, low, pi-1)
		at.quickSort(arr, pi+1, high)
	}
}

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

func (at *AdaptiveTimeout) GetTimeout() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return at.currentTimeout
}

func (at *AdaptiveTimeout) GetFastCommitTimeout() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()

	if len(at.fastCommitTimeouts) > 0 {
		avgFastCommit := at.calculateAverage(at.fastCommitTimeouts)
		return time.Duration(float64(avgFastCommit) * 1.5)
	}
	return time.Duration(float64(at.currentTimeout) * 0.75)
}

func (at *AdaptiveTimeout) GetDynamicAnchorTimeout() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()

	if len(at.dynamicAnchorTimeouts) > 0 {
		avgDynamicAnchor := at.calculateAverage(at.dynamicAnchorTimeouts)
		return time.Duration(float64(avgDynamicAnchor) * 1.8)
	}
	return at.currentTimeout
}

func (at *AdaptiveTimeout) GetParallelDAGTimeout() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()

	if len(at.parallelDAGTimeouts) > 0 {
		avgParallelDAG := at.calculateAverage(at.parallelDAGTimeouts)
		return time.Duration(float64(avgParallelDAG) * 1.3)
	}
	return time.Duration(float64(at.currentTimeout) * 0.8)
}

func (at *AdaptiveTimeout) GetAverageLatency() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return at.avgLatency
}

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

func (at *AdaptiveTimeout) GetNetworkConditionFactors() map[string]float64 {
	at.mu.RLock()
	defer at.mu.RUnlock()

	return map[string]float64{
		"congestion_factor":    at.networkCongestionFactor,
		"byzantine_factor":     at.byzantineFaultFactor,
		"effective_multiplier": at.networkCongestionFactor * at.byzantineFaultFactor,
	}
}

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
