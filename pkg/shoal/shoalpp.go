package shoalpp

import (
	"context"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

type ShoalPPFramework struct {
	config            *ShoalPPConfig
	reputationManager *ReputationManager
	adaptiveTimeout   *AdaptiveTimeout
	pipelineManager   *PipelineManager
	metrics           *PerformanceMetrics

	fastCommitManager    *FastCommitManager
	dynamicAnchorManager *DynamicAnchorManager
	parallelDAGManager   *ParallelDAGManager
	anchorSkipManager    *AnchorSkipManager
	interleavedOrderer   *InterleavedOrderer

	anchorSelector       *EnhancedAnchorSelector
	certificateProcessor *CertificateProcessor
	networkOptimizer     *NetworkOptimizer

	running bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	logger *zap.Logger

	mu sync.RWMutex
}

func NewShoalPPFramework(config *ShoalPPConfig, logger *zap.Logger) *ShoalPPFramework {
	if config == nil {
		config = DefaultShoalPPConfig()
	}

	if logger == nil {
		logger, _ = zap.NewDevelopment()
	}

	ctx, cancel := context.WithCancel(context.Background())

	framework := &ShoalPPFramework{
		config:            config,
		reputationManager: NewReputationManager(config),
		adaptiveTimeout:   NewAdaptiveTimeout(config),
		pipelineManager:   NewPipelineManager(config),
		metrics:           NewPerformanceMetrics(),
		ctx:               ctx,
		cancel:            cancel,
		logger:            logger,
	}

	framework.fastCommitManager = NewFastCommitManager(config, logger.Named("fast_commit"))
	framework.dynamicAnchorManager = NewDynamicAnchorManager(config, logger.Named("dynamic_anchor"))
	framework.parallelDAGManager = NewParallelDAGManager(config, logger.Named("parallel_dag"))
	framework.anchorSkipManager = NewAnchorSkipManager(config, logger.Named("anchor_skip"))
	framework.interleavedOrderer = NewInterleavedOrderer(config, logger.Named("interleaved_orderer"))

	framework.anchorSelector = NewEnhancedAnchorSelector(framework)
	framework.certificateProcessor = NewCertificateProcessor(framework)
	framework.networkOptimizer = NewNetworkOptimizer(framework)

	return framework
}

func (sf *ShoalPPFramework) Start() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if sf.running {
		return nil
	}

	sf.logger.Info("Starting Shoal++ framework")

	if err := sf.pipelineManager.InitializeShoalPPPipelines(); err != nil {
		return err
	}

	if sf.config.EnableFastDirectCommit {
		if err := sf.fastCommitManager.Start(); err != nil {
			return err
		}
	}

	if sf.config.EnableDynamicAnchorFreq {
		if err := sf.dynamicAnchorManager.Start(); err != nil {
			return err
		}
	}

	if sf.config.EnableParallelDAGs {
		if err := sf.parallelDAGManager.Start(); err != nil {
			return err
		}
	}

	if sf.config.AnchorSkippingEnabled {
		if err := sf.anchorSkipManager.Start(); err != nil {
			return err
		}
	}

	if err := sf.interleavedOrderer.Start(); err != nil {
		return err
	}

	if sf.config.EnableMetrics {
		sf.wg.Add(1)
		go sf.metricsCollector()
	}

	if sf.config.EnableLeaderReputation {
		sf.wg.Add(1)
		go sf.reputationUpdater()
	}

	sf.wg.Add(1)
	go sf.shoalPPMonitor()

	if sf.config.EnablePipelining {
		sf.pipelineManager.CreatePipeline("certificate_processing", 3)
		sf.pipelineManager.CreatePipeline("anchor_selection", 2)
		sf.pipelineManager.CreatePipeline("network_optimization", 2)
	}

	sf.running = true
	sf.logger.Info("Shoal++ framework started successfully")

	return nil
}

func (sf *ShoalPPFramework) Stop() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if !sf.running {
		return nil
	}

	sf.logger.Info("Stopping Shoal++ framework")

	sf.cancel()

	if sf.fastCommitManager != nil {
		sf.fastCommitManager.Stop()
	}

	if sf.dynamicAnchorManager != nil {
		sf.dynamicAnchorManager.Stop()
	}

	if sf.parallelDAGManager != nil {
		sf.parallelDAGManager.Stop()
	}

	if sf.anchorSkipManager != nil {
		sf.anchorSkipManager.Stop()
	}

	if sf.interleavedOrderer != nil {
		sf.interleavedOrderer.Stop()
	}

	sf.pipelineManager.StopAll()

	sf.wg.Wait()

	sf.running = false
	sf.logger.Info("Shoal++ framework stopped successfully")

	return nil
}

// shoalPPMonitor monitors Shoal++ specific metrics and performance
func (sf *ShoalPPFramework) shoalPPMonitor() {
	defer sf.wg.Done()

	ticker := time.NewTicker(sf.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sf.ctx.Done():
			return
		case <-ticker.C:
			sf.collectShoalPPMetrics()
		}
	}
}

// collectShoalPPMetrics collects Shoal++ specific performance metrics
func (sf *ShoalPPFramework) collectShoalPPMetrics() {
	// Collect fast commit metrics
	if sf.fastCommitManager != nil {
		fastCommitStats := sf.fastCommitManager.GetStats()
		sf.logger.Debug("Fast commit metrics",
			zap.Int64("total_fast_commits", fastCommitStats["total_commits"].(int64)),
			zap.Float64("success_rate", fastCommitStats["success_rate"].(float64)),
			zap.Duration("avg_latency", fastCommitStats["avg_latency"].(time.Duration)),
		)
	}

	// Collect dynamic anchor metrics
	if sf.dynamicAnchorManager != nil {
		dynamicStats := sf.dynamicAnchorManager.GetStats()
		sf.logger.Debug("Dynamic anchor metrics",
			zap.Int64("total_dynamic_anchors", dynamicStats["total_anchors"].(int64)),
			zap.Float64("frequency_improvement", dynamicStats["frequency_improvement"].(float64)),
		)
	}

	// Collect parallel DAG metrics
	if sf.parallelDAGManager != nil {
		parallelStats := sf.parallelDAGManager.GetStats()
		sf.logger.Debug("Parallel DAG metrics",
			zap.Int("active_instances", parallelStats["active_instances"].(int)),
			zap.Float64("utilization", parallelStats["utilization"].(float64)),
			zap.Float64("throughput_gain", parallelStats["throughput_gain"].(float64)),
		)
	}

	// Update overall metrics
	sf.updateOverallMetrics()
}

// updateOverallMetrics updates the overall Shoal++ performance metrics
func (sf *ShoalPPFramework) updateOverallMetrics() {
	// Update reputation scores in metrics
	bestLeaders := sf.reputationManager.GetBestLeaders(10)
	reputationScores := make(map[types.NodeID]float64)

	for _, nodeID := range bestLeaders {
		rep := sf.reputationManager.GetReputation(nodeID)
		reputationScores[nodeID] = rep.GetScore()
	}

	sf.metrics.mu.Lock()
	sf.metrics.ReputationScores = reputationScores
	sf.metrics.AdaptiveTimeoutValue = sf.adaptiveTimeout.GetTimeout()
	sf.metrics.LastUpdate = time.Now()

	// Add Shoal++ specific metrics
	if sf.fastCommitManager != nil {
		stats := sf.fastCommitManager.GetStats()
		sf.metrics.BlockLatency = stats["avg_latency"].(time.Duration)
	}

	if sf.parallelDAGManager != nil {
		stats := sf.parallelDAGManager.GetStats()
		sf.metrics.PipelineUtilization = stats["utilization"].(float64)
	}

	sf.metrics.mu.Unlock()
}

// metricsCollector collects and updates performance metrics
func (sf *ShoalPPFramework) metricsCollector() {
	defer sf.wg.Done()

	ticker := time.NewTicker(sf.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sf.ctx.Done():
			return
		case <-ticker.C:
			sf.collectMetrics()
		}
	}
}

// collectMetrics collects current performance metrics (enhanced from original Shoal)
func (sf *ShoalPPFramework) collectMetrics() {
	// Update reputation scores in metrics
	bestLeaders := sf.reputationManager.GetBestLeaders(10)
	reputationScores := make(map[types.NodeID]float64)

	for _, nodeID := range bestLeaders {
		rep := sf.reputationManager.GetReputation(nodeID)
		reputationScores[nodeID] = rep.GetScore()
	}

	sf.metrics.mu.Lock()
	sf.metrics.ReputationScores = reputationScores
	sf.metrics.AdaptiveTimeoutValue = sf.adaptiveTimeout.GetTimeout()
	sf.metrics.LastUpdate = time.Now()
	sf.metrics.mu.Unlock()

	// Log enhanced metrics
	if sf.logger != nil {
		sf.logger.Debug("Enhanced metrics updated",
			zap.Duration("avg_latency", sf.metrics.AvgLatency),
			zap.Float64("messages_per_second", sf.metrics.MessagesPerSecond),
			zap.Float64("error_rate", sf.metrics.ErrorRate),
			zap.Duration("adaptive_timeout", sf.metrics.AdaptiveTimeoutValue),
			zap.Int("best_leaders_count", len(bestLeaders)),
			zap.Float64("pipeline_utilization", sf.metrics.PipelineUtilization),
		)
	}
}

// reputationUpdater updates leader reputations and applies decay
func (sf *ShoalPPFramework) reputationUpdater() {
	defer sf.wg.Done()

	ticker := time.NewTicker(time.Minute) // Update every minute
	defer ticker.Stop()

	for {
		select {
		case <-sf.ctx.Done():
			return
		case <-ticker.C:
			sf.reputationManager.DecayReputations()
		}
	}
}

// GetMetrics returns current performance metrics with Shoal++ enhancements
func (sf *ShoalPPFramework) GetMetrics() PerformanceMetrics {
	return sf.metrics.GetSnapshot()
}

// GetBestLeaders returns the best leaders based on reputation
func (sf *ShoalPPFramework) GetBestLeaders(count int) []types.NodeID {
	if !sf.config.EnableLeaderReputation {
		return nil
	}
	return sf.reputationManager.GetBestLeaders(count)
}

// UpdateLatency updates latency measurements for adaptive timeouts with Shoal++ enhancements
func (sf *ShoalPPFramework) UpdateLatency(latency time.Duration) {
	if sf.config.EnableAdaptiveTimeouts {
		sf.adaptiveTimeout.UpdateLatency(latency)
	}
	sf.metrics.UpdateLatency(latency)
}

// UpdateReputation updates the reputation for a node with Shoal++ metrics
func (sf *ShoalPPFramework) UpdateReputation(nodeID types.NodeID, latency time.Duration, success bool) {
	if sf.config.EnableLeaderReputation {
		sf.reputationManager.UpdateReputation(nodeID, latency, success)
	}
}

// UpdateShoalPPMetrics updates Shoal++ specific metrics for a node
func (sf *ShoalPPFramework) UpdateShoalPPMetrics(nodeID types.NodeID,
	fastCommit, dynamicAnchor, roundTimeout, anchorSkip bool, parallelDAGUtil float64) {
	if sf.config.EnableLeaderReputation {
		sf.reputationManager.UpdateShoalPPMetrics(nodeID, fastCommit, dynamicAnchor,
			roundTimeout, anchorSkip, parallelDAGUtil)
	}
}

// GetCurrentTimeout returns the current adaptive timeout value
func (sf *ShoalPPFramework) GetCurrentTimeout() time.Duration {
	if sf.config.EnableAdaptiveTimeouts {
		return sf.adaptiveTimeout.GetTimeout()
	}
	return sf.config.BaseTimeout
}

// GetFastCommitTimeout returns the timeout optimized for fast commit operations
func (sf *ShoalPPFramework) GetFastCommitTimeout() time.Duration {
	if sf.config.EnableAdaptiveTimeouts {
		return sf.adaptiveTimeout.GetFastCommitTimeout()
	}
	return time.Duration(float64(sf.config.BaseTimeout) * 0.75)
}

// ProcessFastCommit processes a fast commit request
func (sf *ShoalPPFramework) ProcessFastCommit(candidate *AnchorCandidate) (*FastCommitResult, error) {
	if !sf.config.EnableFastDirectCommit || sf.fastCommitManager == nil {
		return nil, ErrFastCommitFailed
	}

	return sf.fastCommitManager.ProcessFastCommit(candidate)
}

// ProcessDynamicAnchor processes a dynamic anchor frequency adjustment
func (sf *ShoalPPFramework) ProcessDynamicAnchor(candidate *AnchorCandidate) (*DynamicAnchorResult, error) {
	if !sf.config.EnableDynamicAnchorFreq || sf.dynamicAnchorManager == nil {
		return nil, ErrDynamicAnchorFailed
	}

	return sf.dynamicAnchorManager.ProcessDynamicAnchor(candidate)
}

// SubmitToParallelDAG submits an operation to a parallel DAG instance
func (sf *ShoalPPFramework) SubmitToParallelDAG(dagInstance int, op PipelineOperation) error {
	if !sf.config.EnableParallelDAGs || sf.parallelDAGManager == nil {
		return ErrParallelDAGFailed
	}

	return sf.parallelDAGManager.SubmitOperation(dagInstance, op)
}

// ShouldSkipAnchor determines if an anchor candidate should be skipped
func (sf *ShoalPPFramework) ShouldSkipAnchor(candidate *AnchorCandidate) bool {
	if !sf.config.AnchorSkippingEnabled || sf.anchorSkipManager == nil {
		return false
	}

	return sf.anchorSkipManager.ShouldSkip(candidate)
}

// GetInterleavedOrder gets the interleaved order for parallel DAG outputs
func (sf *ShoalPPFramework) GetInterleavedOrder(outputs []InterleavedDAGOutput) []InterleavedDAGOutput {
	if sf.interleavedOrderer == nil {
		return outputs
	}

	return sf.interleavedOrderer.OrderOutputs(outputs)
}

// SubmitCertificateOperation submits a certificate processing operation to the pipeline
func (sf *ShoalPPFramework) SubmitCertificateOperation(cert *types.Certificate, callback func(PipelineResult)) error {
	if !sf.config.EnablePipelining {
		// Process synchronously if pipelining is disabled
		if callback != nil {
			callback(PipelineResult{
				OperationID: cert.Hash.String(),
				Success:     true,
				Data:        cert,
				Duration:    0,
			})
		}
		return nil
	}

	pipeline, exists := sf.pipelineManager.GetPipeline("certificate_processing")
	if !exists {
		return ErrPipelineOperation
	}

	op := PipelineOperation{
		ID:       cert.Hash.String(),
		Type:     "certificate",
		Data:     cert,
		Callback: callback,
		ShoalPPFeatures: ShoalPPOperationFeatures{
			UseFastCommit:    sf.config.EnableFastDirectCommit,
			UseDynamicAnchor: sf.config.EnableDynamicAnchorFreq,
			UseParallelDAG:   sf.config.EnableParallelDAGs,
		},
	}

	return pipeline.SubmitOperation(op)
}

type EnhancedAnchorSelector struct {
	framework *ShoalPPFramework
	logger    *zap.Logger
}

func NewEnhancedAnchorSelector(framework *ShoalPPFramework) *EnhancedAnchorSelector {
	return &EnhancedAnchorSelector{
		framework: framework,
		logger:    framework.logger.Named("anchor_selector"),
	}
}

func (eas *EnhancedAnchorSelector) SelectAnchors(certificates []*types.Certificate, maxAnchors int) ([]*types.Certificate, error) {
	start := time.Now()
	defer func() {
		eas.framework.metrics.AnchorSelectionTime = time.Since(start)
	}()

	if len(certificates) == 0 {
		return nil, nil
	}

	candidates := make([]*AnchorCandidate, 0, len(certificates))
	for _, cert := range certificates {
		candidate := NewAnchorCandidate(cert.Header.Author, cert.Header.Round, cert)

		if eas.framework.config.EnableFastDirectCommit {
			uncertified := eas.countUncertifiedProposals(cert.Header.Author)
			candidate.UncertifiedProposals = uncertified
			if uncertified >= eas.framework.config.FastCommitThreshold {
				candidate.CommitRule = FastDirectCommitRule
			}
		}

		if eas.framework.config.EnableDynamicAnchorFreq {
			candidate.DynamicPriority = eas.calculateDynamicPriority(candidate)
		}

		if eas.framework.config.AnchorSkippingEnabled {
			if candidate.ShouldSkip(eas.framework.config.AnchorSkippingThreshold) {
				candidate.SkipReason = "timeout_exceeded"
				continue
			}
		}

		candidates = append(candidates, candidate)
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	selectedCandidates := eas.shoalPPSelection(candidates, maxAnchors)

	result := make([]*types.Certificate, len(selectedCandidates))
	for i, candidate := range selectedCandidates {
		result[i] = candidate.Certificate
	}

	eas.logger.Debug("Enhanced anchor selection completed",
		zap.Int("total_candidates", len(certificates)),
		zap.Int("valid_candidates", len(candidates)),
		zap.Int("selected_count", len(result)),
	)

	return result, nil
}

func (eas *EnhancedAnchorSelector) shoalPPSelection(candidates []*AnchorCandidate, maxAnchors int) []*AnchorCandidate {
	type candidateScore struct {
		candidate *AnchorCandidate
		score     float64
	}

	var scoredCandidates []candidateScore

	for _, candidate := range candidates {
		score := eas.calculateShoalPPScore(candidate)
		scoredCandidates = append(scoredCandidates, candidateScore{candidate, score})
	}

	sort.Slice(scoredCandidates, func(i, j int) bool {
		return scoredCandidates[i].score > scoredCandidates[j].score
	})

	result := make([]*AnchorCandidate, 0, maxAnchors)
	for i := 0; i < len(scoredCandidates) && i < maxAnchors; i++ {
		result = append(result, scoredCandidates[i].candidate)
	}

	return result
}

func (eas *EnhancedAnchorSelector) calculateShoalPPScore(candidate *AnchorCandidate) float64 {
	baseScore := 1.0

	if eas.framework.config.EnableLeaderReputation {
		rep := eas.framework.reputationManager.GetReputation(candidate.NodeID)
		reputationScore := rep.GetScore()
		baseScore *= reputationScore
	}

	if candidate.CommitRule == FastDirectCommitRule {
		baseScore *= 1.5
	}

	if eas.framework.config.EnableDynamicAnchorFreq {
		baseScore *= candidate.DynamicPriority
	}

	age := time.Since(candidate.Timestamp)
	timePenalty := float64(age.Nanoseconds()) / float64(time.Second.Nanoseconds())
	timePenalty *= 0.05

	return baseScore - timePenalty
}

func (eas *EnhancedAnchorSelector) countUncertifiedProposals(nodeID types.NodeID) int {
	rep := eas.framework.reputationManager.GetReputation(nodeID)
	metrics := rep.GetShoalPPMetrics()

	if count, ok := metrics["dynamic_anchor_count"].(int64); ok {
		return int(count)
	}

	return 3
}

func (eas *EnhancedAnchorSelector) calculateDynamicPriority(candidate *AnchorCandidate) float64 {
	basePriority := 1.0

	age := time.Since(candidate.Timestamp)
	ageFactor := 1.0 + (age.Seconds() / 10.0)

	proposalFactor := 1.0 + (float64(candidate.UncertifiedProposals) * 0.1)

	return basePriority * ageFactor * proposalFactor
}

// IsRunning returns whether the framework is currently running
func (sf *ShoalPPFramework) IsRunning() bool {
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	return sf.running
}

// GetConfig returns the current configuration
func (sf *ShoalPPFramework) GetConfig() *ShoalPPConfig {
	return sf.config
}

// GetAnchorSelector returns the enhanced anchor selector
func (sf *ShoalPPFramework) GetAnchorSelector() *EnhancedAnchorSelector {
	return sf.anchorSelector
}

// GetCertificateProcessor returns the certificate processor
func (sf *ShoalPPFramework) GetCertificateProcessor() *CertificateProcessor {
	return sf.certificateProcessor
}

// GetNetworkOptimizer returns the network optimizer
func (sf *ShoalPPFramework) GetNetworkOptimizer() *NetworkOptimizer {
	return sf.networkOptimizer
}

// GetShoalPPStats returns comprehensive Shoal++ statistics
func (sf *ShoalPPFramework) GetShoalPPStats() map[string]interface{} {
	stats := map[string]interface{}{
		"shoalpp_enabled": true,
		"running":         sf.IsRunning(),
		"configuration":   sf.getShoalPPConfigStats(),
	}

	// Add component stats
	if sf.fastCommitManager != nil {
		stats["fast_commit"] = sf.fastCommitManager.GetStats()
	}

	if sf.dynamicAnchorManager != nil {
		stats["dynamic_anchor"] = sf.dynamicAnchorManager.GetStats()
	}

	if sf.parallelDAGManager != nil {
		stats["parallel_dag"] = sf.parallelDAGManager.GetStats()
	}

	if sf.anchorSkipManager != nil {
		stats["anchor_skip"] = sf.anchorSkipManager.GetStats()
	}

	if sf.interleavedOrderer != nil {
		stats["interleaved_orderer"] = sf.interleavedOrderer.GetStats()
	}

	// Add pipeline stats
	stats["pipeline"] = sf.pipelineManager.GetShoalPPMetrics()

	// Add reputation stats
	stats["reputation"] = sf.reputationManager.GetShoalPPStats()

	return stats
}

// getShoalPPConfigStats returns configuration statistics
func (sf *ShoalPPFramework) getShoalPPConfigStats() map[string]interface{} {
	return map[string]interface{}{
		"fast_commit_enabled":      sf.config.EnableFastDirectCommit,
		"dynamic_anchor_enabled":   sf.config.EnableDynamicAnchorFreq,
		"parallel_dags_enabled":    sf.config.EnableParallelDAGs,
		"anchor_skipping_enabled":  sf.config.AnchorSkippingEnabled,
		"round_timeout_enabled":    sf.config.RoundTimeoutEnabled,
		"parallel_dag_instances":   sf.config.ParallelDAGInstances,
		"fast_commit_threshold":    sf.config.FastCommitThreshold,
		"dag_stagger_delay_ms":     sf.config.DAGStaggerDelay.Milliseconds(),
		"round_timeout_ms":         sf.config.RoundTimeout.Milliseconds(),
		"anchor_skip_threshold_ms": sf.config.AnchorSkippingThreshold.Milliseconds(),
	}
}
