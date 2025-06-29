package shoal

import (
	"context"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

// ShoalFramework implements the Shoal framework for enhancing Bullshark consensus
type ShoalFramework struct {
	config             *ShoalConfig
	reputationManager  *ReputationManager
	adaptiveTimeout    *AdaptiveTimeout
	pipelineManager    *PipelineManager
	metrics           *PerformanceMetrics
	
	// Components
	anchorSelector    *EnhancedAnchorSelector
	certificateProcessor *CertificateProcessor
	networkOptimizer  *NetworkOptimizer
	
	// State
	running           bool
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	
	// Logging
	logger            *zap.Logger
	
	mu                sync.RWMutex
}

// NewShoalFramework creates a new Shoal framework instance
func NewShoalFramework(config *ShoalConfig, logger *zap.Logger) *ShoalFramework {
	if config == nil {
		config = DefaultShoalConfig()
	}
	
	if logger == nil {
		logger, _ = zap.NewDevelopment()
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	framework := &ShoalFramework{
		config:            config,
		reputationManager: NewReputationManager(config),
		adaptiveTimeout:   NewAdaptiveTimeout(config),
		pipelineManager:   NewPipelineManager(config),
		metrics:          NewPerformanceMetrics(),
		ctx:              ctx,
		cancel:           cancel,
		logger:           logger,
	}
	
	// Initialize components
	framework.anchorSelector = NewEnhancedAnchorSelector(framework)
	framework.certificateProcessor = NewCertificateProcessor(framework)
	framework.networkOptimizer = NewNetworkOptimizer(framework)
	
	return framework
}

// Start starts the Shoal framework
func (sf *ShoalFramework) Start() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	
	if sf.running {
		return nil
	}
	
	sf.logger.Info("Starting Shoal framework")
	
	// Start background services
	if sf.config.EnableMetrics {
		sf.wg.Add(1)
		go sf.metricsCollector()
	}
	
	if sf.config.EnableLeaderReputation {
		sf.wg.Add(1)
		go sf.reputationUpdater()
	}
	
	// Create main processing pipelines
	if sf.config.EnablePipelining {
		sf.pipelineManager.CreatePipeline("certificate_processing", 3)
		sf.pipelineManager.CreatePipeline("anchor_selection", 2)
		sf.pipelineManager.CreatePipeline("network_optimization", 2)
	}
	
	sf.running = true
	sf.logger.Info("Shoal framework started successfully")
	
	return nil
}

// Stop stops the Shoal framework
func (sf *ShoalFramework) Stop() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	
	if !sf.running {
		return nil
	}
	
	sf.logger.Info("Stopping Shoal framework")
	
	// Cancel context to stop all goroutines
	sf.cancel()
	
	// Stop pipelines
	sf.pipelineManager.StopAll()
	
	// Wait for all goroutines to finish
	sf.wg.Wait()
	
	sf.running = false
	sf.logger.Info("Shoal framework stopped successfully")
	
	return nil
}

// metricsCollector collects and updates performance metrics
func (sf *ShoalFramework) metricsCollector() {
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

// collectMetrics collects current performance metrics
func (sf *ShoalFramework) collectMetrics() {
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
	
	// Log metrics periodically
	if sf.logger != nil {
		sf.logger.Debug("Metrics updated",
			zap.Duration("avg_latency", sf.metrics.AvgLatency),
			zap.Float64("messages_per_second", sf.metrics.MessagesPerSecond),
			zap.Float64("error_rate", sf.metrics.ErrorRate),
			zap.Duration("adaptive_timeout", sf.metrics.AdaptiveTimeoutValue),
			zap.Int("best_leaders_count", len(bestLeaders)),
		)
	}
}

// reputationUpdater updates leader reputations and applies decay
func (sf *ShoalFramework) reputationUpdater() {
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

// GetMetrics returns current performance metrics
func (sf *ShoalFramework) GetMetrics() PerformanceMetrics {
	return sf.metrics.GetSnapshot()
}

// GetBestLeaders returns the best leaders based on reputation
func (sf *ShoalFramework) GetBestLeaders(count int) []types.NodeID {
	if !sf.config.EnableLeaderReputation {
		return nil
	}
	return sf.reputationManager.GetBestLeaders(count)
}

// UpdateLatency updates latency measurements for adaptive timeouts
func (sf *ShoalFramework) UpdateLatency(latency time.Duration) {
	if sf.config.EnableAdaptiveTimeouts {
		sf.adaptiveTimeout.UpdateLatency(latency)
	}
	sf.metrics.UpdateLatency(latency)
}

// UpdateReputation updates the reputation for a node
func (sf *ShoalFramework) UpdateReputation(nodeID types.NodeID, latency time.Duration, success bool) {
	if sf.config.EnableLeaderReputation {
		sf.reputationManager.UpdateReputation(nodeID, latency, success)
	}
}

// GetCurrentTimeout returns the current adaptive timeout value
func (sf *ShoalFramework) GetCurrentTimeout() time.Duration {
	if sf.config.EnableAdaptiveTimeouts {
		return sf.adaptiveTimeout.GetTimeout()
	}
	return sf.config.BaseTimeout
}

// SubmitCertificateOperation submits a certificate processing operation to the pipeline
func (sf *ShoalFramework) SubmitCertificateOperation(cert *types.Certificate, callback func(PipelineResult)) error {
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
		Type:     "certificate_processing",
		Data:     cert,
		Callback: callback,
	}
	
	return pipeline.SubmitOperation(op)
}

// EnhancedAnchorSelector implements enhanced anchor selection with reputation
type EnhancedAnchorSelector struct {
	framework *ShoalFramework
	logger    *zap.Logger
}

// NewEnhancedAnchorSelector creates a new enhanced anchor selector
func NewEnhancedAnchorSelector(framework *ShoalFramework) *EnhancedAnchorSelector {
	return &EnhancedAnchorSelector{
		framework: framework,
		logger:    framework.logger.Named("anchor_selector"),
	}
}

// SelectAnchors selects anchors using reputation-based selection
func (eas *EnhancedAnchorSelector) SelectAnchors(certificates []*types.Certificate, maxAnchors int) ([]*types.Certificate, error) {
	start := time.Now()
	defer func() {
		eas.framework.metrics.AnchorSelectionTime = time.Since(start)
	}()
	
	if len(certificates) == 0 {
		return nil, nil
	}
	
	// If reputation is not enabled, use default selection
	if !eas.framework.config.EnableLeaderReputation {
		return eas.defaultSelection(certificates, maxAnchors), nil
	}
	
	// Reputation-based selection
	return eas.reputationBasedSelection(certificates, maxAnchors), nil
}

// defaultSelection implements the default anchor selection
func (eas *EnhancedAnchorSelector) defaultSelection(certificates []*types.Certificate, maxAnchors int) []*types.Certificate {
	// Sort by timestamp (earliest first)
	sortedCerts := make([]*types.Certificate, len(certificates))
	copy(sortedCerts, certificates)
	
	// Simple insertion sort by timestamp
	for i := 1; i < len(sortedCerts); i++ {
		key := sortedCerts[i]
		j := i - 1
		for j >= 0 && sortedCerts[j].Header.Timestamp.After(key.Header.Timestamp) {
			sortedCerts[j+1] = sortedCerts[j]
			j--
		}
		sortedCerts[j+1] = key
	}
	
	// Select up to maxAnchors
	if len(sortedCerts) > maxAnchors {
		return sortedCerts[:maxAnchors]
	}
	
	return sortedCerts
}

// reputationBasedSelection implements reputation-based anchor selection
func (eas *EnhancedAnchorSelector) reputationBasedSelection(certificates []*types.Certificate, maxAnchors int) []*types.Certificate {
	// Score each certificate based on author reputation and other factors
	type certScore struct {
		cert  *types.Certificate
		score float64
	}
	
	var scoredCerts []certScore
	
	for _, cert := range certificates {
		rep := eas.framework.reputationManager.GetReputation(cert.Header.Author)
		
		// Calculate composite score
		reputationScore := rep.GetScore()
		
		// Factor in timestamp (earlier is better)
		now := time.Now()
		timePenalty := float64(now.Sub(cert.Header.Timestamp).Nanoseconds()) / float64(time.Second.Nanoseconds())
		timePenalty = timePenalty * 0.1 // Small penalty for delayed certificates
		
		// Factor in certificate size (smaller is better for network efficiency)
		sizePenalty := float64(len(cert.Hash.Bytes())) * 0.01
		
		totalScore := reputationScore - timePenalty - sizePenalty
		
		scoredCerts = append(scoredCerts, certScore{cert, totalScore})
	}
	
	// Sort by score (descending) - O(n log n)
	sort.Slice(scoredCerts, func(i, j int) bool {
		return scoredCerts[i].score > scoredCerts[j].score
	})
	
	// Select top scorers
	result := make([]*types.Certificate, 0, maxAnchors)
	for i := 0; i < len(scoredCerts) && i < maxAnchors; i++ {
		result = append(result, scoredCerts[i].cert)
	}
	
	eas.logger.Debug("Selected anchors based on reputation",
		zap.Int("total_candidates", len(certificates)),
		zap.Int("selected_count", len(result)),
	)
	
	return result
}

// CertificateProcessor handles certificate processing with pipelining
type CertificateProcessor struct {
	framework *ShoalFramework
	logger    *zap.Logger
}

// NewCertificateProcessor creates a new certificate processor
func NewCertificateProcessor(framework *ShoalFramework) *CertificateProcessor {
	return &CertificateProcessor{
		framework: framework,
		logger:    framework.logger.Named("certificate_processor"),
	}
}

// ProcessCertificate processes a certificate with enhanced validation
func (cp *CertificateProcessor) ProcessCertificate(cert *types.Certificate) error {
	start := time.Now()
	
	// Basic validation
	if err := cp.validateCertificate(cert); err != nil {
		cp.framework.UpdateReputation(cert.Header.Author, time.Since(start), false)
		return err
	}
	
	// Enhanced validation with reputation consideration
	if cp.framework.config.EnableLeaderReputation {
		rep := cp.framework.reputationManager.GetReputation(cert.Header.Author)
		if rep.GetScore() < cp.framework.config.MinReputationScore {
			cp.logger.Debug("Certificate from low-reputation node",
				zap.String("author", string(cert.Header.Author)),
				zap.Float64("reputation", rep.GetScore()),
			)
		}
	}
	
	// Update reputation on successful processing
	cp.framework.UpdateReputation(cert.Header.Author, time.Since(start), true)
	
	return nil
}

// validateCertificate performs basic certificate validation
func (cp *CertificateProcessor) validateCertificate(cert *types.Certificate) error {
	if cert == nil {
		return ErrInvalidConfiguration
	}
	
	if cert.Header == nil {
		return ErrInvalidConfiguration
	}
	
	// Add more validation logic as needed
	return nil
}

// NetworkOptimizer optimizes network operations
type NetworkOptimizer struct {
	framework *ShoalFramework
	logger    *zap.Logger
}

// NewNetworkOptimizer creates a new network optimizer
func NewNetworkOptimizer(framework *ShoalFramework) *NetworkOptimizer {
	return &NetworkOptimizer{
		framework: framework,
		logger:    framework.logger.Named("network_optimizer"),
	}
}

// OptimizeMessage optimizes message sending based on current network conditions
func (no *NetworkOptimizer) OptimizeMessage(nodeID types.NodeID, messageType string, priority int) time.Duration {
	// Get reputation for target node
	rep := no.framework.reputationManager.GetReputation(nodeID)
	
	// Adjust timeout based on node reputation and network conditions
	baseTimeout := no.framework.GetCurrentTimeout()
	
	// Higher reputation nodes get shorter timeouts (they're more reliable)
	reputationFactor := rep.GetScore()
	if reputationFactor < 0.1 {
		reputationFactor = 0.1
	}
	
	optimizedTimeout := time.Duration(float64(baseTimeout) / reputationFactor)
	
	// Enforce bounds
	maxTimeout := no.framework.config.MaxTimeout
	minTimeout := no.framework.config.MinTimeout
	
	if optimizedTimeout > maxTimeout {
		optimizedTimeout = maxTimeout
	} else if optimizedTimeout < minTimeout {
		optimizedTimeout = minTimeout
	}
	
	no.logger.Debug("Optimized message timeout",
		zap.String("node_id", string(nodeID)),
		zap.String("message_type", messageType),
		zap.Int("priority", priority),
		zap.Duration("base_timeout", baseTimeout),
		zap.Duration("optimized_timeout", optimizedTimeout),
		zap.Float64("reputation", reputationFactor),
	)
	
	return optimizedTimeout
}

// IsRunning returns whether the framework is currently running
func (sf *ShoalFramework) IsRunning() bool {
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	return sf.running
}

// GetConfig returns the current configuration
func (sf *ShoalFramework) GetConfig() *ShoalConfig {
	return sf.config
}

// GetAnchorSelector returns the enhanced anchor selector
func (sf *ShoalFramework) GetAnchorSelector() *EnhancedAnchorSelector {
	return sf.anchorSelector
}

// GetCertificateProcessor returns the certificate processor
func (sf *ShoalFramework) GetCertificateProcessor() *CertificateProcessor {
	return sf.certificateProcessor
}

// GetNetworkOptimizer returns the network optimizer
func (sf *ShoalFramework) GetNetworkOptimizer() *NetworkOptimizer {
	return sf.networkOptimizer
}