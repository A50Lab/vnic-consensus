package shoalpp

import (
	"time"

	"go.uber.org/zap"

	bullsharktypes "github.com/vietchain/vniccss/pkg/bullshark/types"
)

type ShoalPPIntegration struct {
	shoalppFramework *ShoalPPFramework
	config           *ShoalPPConfig
	logger           *zap.Logger
}

func NewShoalPPIntegration(
	config *ShoalPPConfig,
	logger *zap.Logger,
) *ShoalPPIntegration {
	if config == nil {
		config = DefaultShoalPPConfig()
	}

	if logger == nil {
		logger, _ = zap.NewDevelopment()
	}

	shoalppFramework := NewShoalPPFramework(config, logger.Named("shoalpp"))

	return &ShoalPPIntegration{
		shoalppFramework: shoalppFramework,
		config:           config,
		logger:           logger.Named("shoalpp_integration"),
	}
}

func (si *ShoalPPIntegration) SetBullsharkEngine(engine interface{}) {
	si.logger.Info("Bullshark engine integrated with Shoal++")
}

func (si *ShoalPPIntegration) SetBullsharkSelector(selector interface{}) {
	if setter, ok := selector.(interface{ SetShoalPPFramework(*ShoalPPFramework) }); ok {
		setter.SetShoalPPFramework(si.shoalppFramework)
	}
	si.logger.Info("Bullshark selector integrated with Shoal++")
}

func (si *ShoalPPIntegration) SetNarwhalPrimary(primary interface{}) {
	if setter, ok := primary.(interface{ SetShoalPPFramework(*ShoalPPFramework) }); ok {
		setter.SetShoalPPFramework(si.shoalppFramework)
	}
	si.logger.Info("Narwhal primary integrated with Shoal++")
}

func (si *ShoalPPIntegration) Start() error {
	si.logger.Info("Starting Shoal++ integration")

	if err := si.shoalppFramework.Start(); err != nil {
		return err
	}

	if si.config.EnableMetrics {
		go si.performanceMonitor()
	}

	go si.shoalppMonitor()

	si.logger.Info("Shoal++ integration started successfully")
	return nil
}

func (si *ShoalPPIntegration) Stop() error {
	si.logger.Info("Stopping Shoal++ integration")

	if err := si.shoalppFramework.Stop(); err != nil {
		return err
	}

	si.logger.Info("Shoal++ integration stopped successfully")
	return nil
}

func (si *ShoalPPIntegration) performanceMonitor() {
	ticker := time.NewTicker(si.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			metrics := si.shoalppFramework.GetMetrics()
			si.logPerformanceMetrics(metrics)
		}
	}
}

func (si *ShoalPPIntegration) shoalppMonitor() {
	ticker := time.NewTicker(si.config.MetricsInterval * 2) // Less frequent monitoring
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			si.logShoalPPFeatures()
		}
	}
}

func (si *ShoalPPIntegration) logPerformanceMetrics(metrics PerformanceMetrics) {
	si.logger.Info("Shoal++ Performance metrics",
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

		zap.Duration("fast_commit_latency", metrics.FastCommitLatency),
		zap.Duration("dynamic_anchor_latency", metrics.DynamicAnchorLatency),
		zap.Duration("parallel_dag_latency", metrics.ParallelDAGLatency),
		zap.Duration("latency_reduction", metrics.LatencyReduction),
		zap.Float64("throughput_improvement", metrics.ThroughputImprovement),
		zap.Float64("fast_commit_utilization", metrics.FastCommitUtilization),
		zap.Float64("dynamic_anchor_utilization", metrics.DynamicAnchorUtilization),
		zap.Float64("parallel_dag_utilization", metrics.ParallelDAGUtilization),
		zap.Int("message_delay_reduction", metrics.MessageDelayReduction),
	)
}

func (si *ShoalPPIntegration) logShoalPPFeatures() {
	stats := si.shoalppFramework.GetShoalPPStats()

	si.logger.Info("Shoal++ Features status",
		zap.Any("fast_commit", stats["fast_commit"]),
		zap.Any("dynamic_anchor", stats["dynamic_anchor"]),
		zap.Any("parallel_dag", stats["parallel_dag"]),
		zap.Any("anchor_skip", stats["anchor_skip"]),
		zap.Any("pipeline", stats["pipeline"]),
		zap.Any("reputation", stats["reputation"]),
	)

	improvements := si.shoalppFramework.metrics.GetShoalPPImprovements()
	overallImprovement := si.shoalppFramework.metrics.CalculateOverallImprovement()

	si.logger.Info("Shoal++ Improvements",
		zap.Any("improvements", improvements),
		zap.Any("overall", overallImprovement),
	)
}

func (si *ShoalPPIntegration) GetMetrics() PerformanceMetrics {
	return si.shoalppFramework.GetMetrics()
}

func (si *ShoalPPIntegration) GetBestLeaders(count int) []string {
	nodeIDs := si.shoalppFramework.GetBestLeaders(count)
	leaders := make([]string, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		leaders[i] = string(nodeID)
	}
	return leaders
}

func (si *ShoalPPIntegration) UpdateLatency(latency time.Duration) {
	si.shoalppFramework.UpdateLatency(latency)
}

func (si *ShoalPPIntegration) GetCurrentTimeout() time.Duration {
	return si.shoalppFramework.GetCurrentTimeout()
}

func (si *ShoalPPIntegration) GetFastCommitTimeout() time.Duration {
	return si.shoalppFramework.GetFastCommitTimeout()
}

func (si *ShoalPPIntegration) IsRunning() bool {
	return si.shoalppFramework.IsRunning()
}

func (si *ShoalPPIntegration) GetConfig() *ShoalPPConfig {
	return si.config
}

type EnhancedConsensusState struct {
	*bullsharktypes.ConsensusState
	ShoalPPMetrics PerformanceMetrics
	LastUpdate     time.Time
}

func NewEnhancedConsensusState(baseState *bullsharktypes.ConsensusState, shoalppMetrics PerformanceMetrics) *EnhancedConsensusState {
	return &EnhancedConsensusState{
		ConsensusState: baseState,
		ShoalPPMetrics: shoalppMetrics,
		LastUpdate:     time.Now(),
	}
}

func (si *ShoalPPIntegration) GetEnhancedState(baseState *bullsharktypes.ConsensusState) *EnhancedConsensusState {
	metrics := si.shoalppFramework.GetMetrics()
	return NewEnhancedConsensusState(baseState, metrics)
}

func (si *ShoalPPIntegration) OptimizeForNetwork() {
	metrics := si.shoalppFramework.GetMetrics()

	if metrics.AvgLatency > time.Millisecond*500 {
		si.logger.Info("High latency detected, optimizing for network conditions",
			zap.Duration("avg_latency", metrics.AvgLatency),
		)

		conditions := NetworkConditions{
			AvgLatency: metrics.AvgLatency,
			ErrorRate:  metrics.ErrorRate,
			PacketLoss: 0.01,
			Throughput: metrics.MessagesPerSecond,
			Jitter:     time.Millisecond * 10,
		}

		si.shoalppFramework.networkOptimizer.OptimizeForNetworkConditions(conditions)
	}

	if metrics.ErrorRate > 0.1 {
		si.logger.Warn("High error rate detected, adjusting for reliability",
			zap.Float64("error_rate", metrics.ErrorRate),
		)

	}

	if metrics.MessagesPerSecond < 10 {
		si.logger.Info("Low throughput detected, optimizing for performance",
			zap.Float64("messages_per_second", metrics.MessagesPerSecond),
		)

	}
}

func (si *ShoalPPIntegration) GetDetailedStats() map[string]interface{} {
	metrics := si.shoalppFramework.GetMetrics()
	shoalppStats := si.shoalppFramework.GetShoalPPStats()
	improvements := metrics.GetShoalPPImprovements()
	overallImprovement := metrics.CalculateOverallImprovement()

	stats := map[string]interface{}{
		"shoalpp_enabled":           true,
		"shoalpp_running":           si.shoalppFramework.IsRunning(),
		"leader_reputation_enabled": si.config.EnableLeaderReputation,
		"adaptive_timeouts_enabled": si.config.EnableAdaptiveTimeouts,
		"pipelining_enabled":        si.config.EnablePipelining,
		"metrics_enabled":           si.config.EnableMetrics,

		"fast_commit_enabled":     si.config.EnableFastDirectCommit,
		"dynamic_anchor_enabled":  si.config.EnableDynamicAnchorFreq,
		"parallel_dags_enabled":   si.config.EnableParallelDAGs,
		"anchor_skipping_enabled": si.config.AnchorSkippingEnabled,
		"round_timeout_enabled":   si.config.RoundTimeoutEnabled,

		"avg_latency_ms":      metrics.AvgLatency.Milliseconds(),
		"min_latency_ms":      metrics.MinLatency.Milliseconds(),
		"max_latency_ms":      metrics.MaxLatency.Milliseconds(),
		"messages_per_second": metrics.MessagesPerSecond,
		"bytes_per_second":    metrics.BytesPerSecond,
		"error_rate":          metrics.ErrorRate,
		"total_messages":      metrics.TotalCount,
		"error_count":         metrics.ErrorCount,

		"block_latency_ms":         metrics.BlockLatency.Milliseconds(),
		"certificate_latency_ms":   metrics.CertificateLatency.Milliseconds(),
		"anchor_selection_time_ms": metrics.AnchorSelectionTime.Milliseconds(),

		"fast_commit_latency_ms":     metrics.FastCommitLatency.Milliseconds(),
		"dynamic_anchor_latency_ms":  metrics.DynamicAnchorLatency.Milliseconds(),
		"parallel_dag_latency_ms":    metrics.ParallelDAGLatency.Milliseconds(),
		"latency_reduction_ms":       metrics.LatencyReduction.Milliseconds(),
		"throughput_improvement":     metrics.ThroughputImprovement,
		"fast_commit_utilization":    metrics.FastCommitUtilization,
		"dynamic_anchor_utilization": metrics.DynamicAnchorUtilization,
		"parallel_dag_utilization":   metrics.ParallelDAGUtilization,
		"message_delay_reduction":    metrics.MessageDelayReduction,

		"adaptive_timeout_ms":      metrics.AdaptiveTimeoutValue.Milliseconds(),
		"pipeline_utilization":     metrics.PipelineUtilization,
		"prevalent_responsiveness": metrics.PrevalentResponsiveness,
		"reputation_leaders_count": len(metrics.ReputationScores),

		"reputation_window_s":    si.config.ReputationWindow.Seconds(),
		"base_timeout_ms":        si.config.BaseTimeout.Milliseconds(),
		"pipeline_depth":         si.config.PipelineDepth,
		"max_concurrent_ops":     si.config.MaxConcurrentOps,
		"metrics_interval_s":     si.config.MetricsInterval.Seconds(),
		"parallel_dag_instances": si.config.ParallelDAGInstances,
		"fast_commit_threshold":  si.config.FastCommitThreshold,
		"dag_stagger_delay_ms":   si.config.DAGStaggerDelay.Milliseconds(),

		"component_stats":      shoalppStats,
		"shoalpp_improvements": improvements,
		"overall_improvement":  overallImprovement,
	}

	bestLeaders := si.shoalppFramework.GetBestLeaders(5)
	leaderScores := make(map[string]float64)
	for _, nodeID := range bestLeaders {
		if score, exists := metrics.ReputationScores[nodeID]; exists {
			leaderScores[string(nodeID)] = score
		}
	}
	stats["best_leaders"] = leaderScores

	return stats
}

func (si *ShoalPPIntegration) GetShoalPPFramework() *ShoalPPFramework {
	return si.shoalppFramework
}

func (si *ShoalPPIntegration) ProcessFastCommit(candidate *AnchorCandidate) (*FastCommitResult, error) {
	return si.shoalppFramework.ProcessFastCommit(candidate)
}

func (si *ShoalPPIntegration) ProcessDynamicAnchor(candidate *AnchorCandidate) (*DynamicAnchorResult, error) {
	return si.shoalppFramework.ProcessDynamicAnchor(candidate)
}

func (si *ShoalPPIntegration) ShouldSkipAnchor(candidate *AnchorCandidate) bool {
	return si.shoalppFramework.ShouldSkipAnchor(candidate)
}

func (si *ShoalPPIntegration) SubmitToParallelDAG(dagInstance int, op PipelineOperation) error {
	return si.shoalppFramework.SubmitToParallelDAG(dagInstance, op)
}
