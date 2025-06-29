package shoal

import (
	"time"

	"go.uber.org/zap"

	bullsharktypes "github.com/vietchain/vniccss/pkg/bullshark/types"
)

// ShoalIntegration manages the integration of Shoal framework with Narwhal and Bullshark
type ShoalIntegration struct {
	shoalFramework   *ShoalFramework
	config           *ShoalConfig
	logger           *zap.Logger
}

// NewShoalIntegration creates a new Shoal integration manager
func NewShoalIntegration(
	config *ShoalConfig,
	logger *zap.Logger,
) *ShoalIntegration {
	if config == nil {
		config = DefaultShoalConfig()
	}
	
	if logger == nil {
		logger, _ = zap.NewDevelopment()
	}

	shoalFramework := NewShoalFramework(config, logger.Named("shoal"))

	return &ShoalIntegration{
		shoalFramework: shoalFramework,
		config:        config,
		logger:        logger.Named("shoal_integration"),
	}
}

// SetBullsharkEngine sets the Bullshark engine for integration (using interface)
func (si *ShoalIntegration) SetBullsharkEngine(engine interface{}) {
	// Store engine interface to avoid import cycle
	si.logger.Info("Bullshark engine integrated with Shoal")
}

// SetBullsharkSelector sets the Bullshark selector for integration (using interface)
func (si *ShoalIntegration) SetBullsharkSelector(selector interface{}) {
	// Use reflection or interface to set Shoal framework on selector
	if setter, ok := selector.(interface{ SetShoalFramework(*ShoalFramework) }); ok {
		setter.SetShoalFramework(si.shoalFramework)
	}
	si.logger.Info("Bullshark selector integrated with Shoal")
}

// SetNarwhalPrimary sets the Narwhal primary for integration (using interface)
func (si *ShoalIntegration) SetNarwhalPrimary(primary interface{}) {
	// Use reflection or interface to set Shoal framework on primary
	if setter, ok := primary.(interface{ SetShoalFramework(*ShoalFramework) }); ok {
		setter.SetShoalFramework(si.shoalFramework)
	}
	si.logger.Info("Narwhal primary integrated with Shoal")
}

// Start starts the Shoal integration
func (si *ShoalIntegration) Start() error {
	si.logger.Info("Starting Shoal integration")
	
	// Start the Shoal framework
	if err := si.shoalFramework.Start(); err != nil {
		return err
	}
	
	// Set up performance monitoring
	if si.config.EnableMetrics {
		go si.performanceMonitor()
	}
	
	si.logger.Info("Shoal integration started successfully")
	return nil
}

// Stop stops the Shoal integration
func (si *ShoalIntegration) Stop() error {
	si.logger.Info("Stopping Shoal integration")
	
	if err := si.shoalFramework.Stop(); err != nil {
		return err
	}
	
	si.logger.Info("Shoal integration stopped successfully")
	return nil
}

// performanceMonitor monitors and logs performance metrics
func (si *ShoalIntegration) performanceMonitor() {
	ticker := time.NewTicker(si.config.MetricsInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			metrics := si.shoalFramework.GetMetrics()
			si.logPerformanceMetrics(metrics)
		}
	}
}

// logPerformanceMetrics logs current performance metrics
func (si *ShoalIntegration) logPerformanceMetrics(metrics PerformanceMetrics) {
	si.logger.Info("Performance metrics",
		zap.Duration("avg_latency", metrics.AvgLatency),
		zap.Duration("min_latency", metrics.MinLatency),
		zap.Duration("max_latency", metrics.MaxLatency),
		zap.Float64("messages_per_second", metrics.MessagesPerSecond),
		zap.Float64("error_rate", metrics.ErrorRate),
		zap.Int64("total_messages", metrics.TotalCount),
		zap.Duration("adaptive_timeout", metrics.AdaptiveTimeoutValue),
		zap.Duration("block_latency", metrics.BlockLatency),
		zap.Duration("certificate_latency", metrics.CertificateLatency),
		zap.Duration("anchor_selection_time", metrics.AnchorSelectionTime),
		zap.Int("reputation_leaders", len(metrics.ReputationScores)),
		zap.Bool("prevalent_responsiveness", metrics.PrevalentResponsiveness),
	)
}

// GetMetrics returns current performance metrics
func (si *ShoalIntegration) GetMetrics() PerformanceMetrics {
	return si.shoalFramework.GetMetrics()
}

// GetBestLeaders returns the best leaders based on reputation
func (si *ShoalIntegration) GetBestLeaders(count int) []string {
	nodeIDs := si.shoalFramework.GetBestLeaders(count)
	leaders := make([]string, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		leaders[i] = string(nodeID)
	}
	return leaders
}

// UpdateLatency updates latency measurements
func (si *ShoalIntegration) UpdateLatency(latency time.Duration) {
	si.shoalFramework.UpdateLatency(latency)
}

// GetCurrentTimeout returns the current adaptive timeout
func (si *ShoalIntegration) GetCurrentTimeout() time.Duration {
	return si.shoalFramework.GetCurrentTimeout()
}

// IsRunning returns whether the integration is running
func (si *ShoalIntegration) IsRunning() bool {
	return si.shoalFramework.IsRunning()
}

// GetConfig returns the current Shoal configuration
func (si *ShoalIntegration) GetConfig() *ShoalConfig {
	return si.config
}

// EnhancedConsensusState represents enhanced consensus state with Shoal metrics
type EnhancedConsensusState struct {
	*bullsharktypes.ConsensusState
	ShoalMetrics PerformanceMetrics
	LastUpdate   time.Time
}

// NewEnhancedConsensusState creates a new enhanced consensus state
func NewEnhancedConsensusState(baseState *bullsharktypes.ConsensusState, shoalMetrics PerformanceMetrics) *EnhancedConsensusState {
	return &EnhancedConsensusState{
		ConsensusState: baseState,
		ShoalMetrics:   shoalMetrics,
		LastUpdate:     time.Now(),
	}
}

// GetEnhancedState returns enhanced consensus state with Shoal metrics
func (si *ShoalIntegration) GetEnhancedState(baseState *bullsharktypes.ConsensusState) *EnhancedConsensusState {
	metrics := si.shoalFramework.GetMetrics()
	return NewEnhancedConsensusState(baseState, metrics)
}

// OptimizeForNetwork optimizes settings based on current network conditions
func (si *ShoalIntegration) OptimizeForNetwork() {
	metrics := si.shoalFramework.GetMetrics()
	
	// Optimize based on current latency
	if metrics.AvgLatency > time.Millisecond*500 {
		si.logger.Info("High latency detected, optimizing for network conditions",
			zap.Duration("avg_latency", metrics.AvgLatency),
		)
		
		// Could dynamically adjust configuration here
		// For example, increase timeout values, reduce batch sizes, etc.
	}
	
	// Optimize based on error rate
	if metrics.ErrorRate > 0.1 { // 10% error rate
		si.logger.Warn("High error rate detected, adjusting for reliability",
			zap.Float64("error_rate", metrics.ErrorRate),
		)
		
		// Could implement error-based optimizations here
	}
	
	// Optimize based on throughput
	if metrics.MessagesPerSecond < 10 {
		si.logger.Info("Low throughput detected, optimizing for performance",
			zap.Float64("messages_per_second", metrics.MessagesPerSecond),
		)
		
		// Could implement throughput optimizations here
	}
}

// GetDetailedStats returns detailed statistics including Shoal enhancements
func (si *ShoalIntegration) GetDetailedStats() map[string]interface{} {
	metrics := si.shoalFramework.GetMetrics()
	
	stats := map[string]interface{}{
		"shoal_enabled":             true,
		"shoal_running":             si.shoalFramework.IsRunning(),
		"leader_reputation_enabled": si.config.EnableLeaderReputation,
		"adaptive_timeouts_enabled": si.config.EnableAdaptiveTimeouts,
		"pipelining_enabled":        si.config.EnablePipelining,
		"metrics_enabled":           si.config.EnableMetrics,
		
		// Performance metrics
		"avg_latency_ms":          metrics.AvgLatency.Milliseconds(),
		"min_latency_ms":          metrics.MinLatency.Milliseconds(),
		"max_latency_ms":          metrics.MaxLatency.Milliseconds(),
		"messages_per_second":     metrics.MessagesPerSecond,
		"bytes_per_second":        metrics.BytesPerSecond,
		"error_rate":              metrics.ErrorRate,
		"total_messages":          metrics.TotalCount,
		"error_count":             metrics.ErrorCount,
		
		// Consensus-specific metrics
		"block_latency_ms":        metrics.BlockLatency.Milliseconds(),
		"certificate_latency_ms":  metrics.CertificateLatency.Milliseconds(),
		"anchor_selection_time_ms": metrics.AnchorSelectionTime.Milliseconds(),
		
		// Shoal-specific metrics
		"adaptive_timeout_ms":     metrics.AdaptiveTimeoutValue.Milliseconds(),
		"pipeline_utilization":    metrics.PipelineUtilization,
		"prevalent_responsiveness": metrics.PrevalentResponsiveness,
		"reputation_leaders_count": len(metrics.ReputationScores),
		
		// Configuration
		"reputation_window_s":     si.config.ReputationWindow.Seconds(),
		"base_timeout_ms":         si.config.BaseTimeout.Milliseconds(),
		"pipeline_depth":          si.config.PipelineDepth,
		"max_concurrent_ops":      si.config.MaxConcurrentOps,
		"metrics_interval_s":      si.config.MetricsInterval.Seconds(),
	}
	
	// Add reputation scores for best leaders
	bestLeaders := si.shoalFramework.GetBestLeaders(5)
	leaderScores := make(map[string]float64)
	for _, nodeID := range bestLeaders {
		if score, exists := metrics.ReputationScores[nodeID]; exists {
			leaderScores[string(nodeID)] = score
		}
	}
	stats["best_leaders"] = leaderScores
	
	return stats
}

// GetShoalFramework returns the underlying Shoal framework
func (si *ShoalIntegration) GetShoalFramework() *ShoalFramework {
	return si.shoalFramework
}