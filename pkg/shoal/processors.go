package shoalpp

import (
	"time"

	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

type CertificateProcessor struct {
	framework *ShoalPPFramework
	logger    *zap.Logger
}

func NewCertificateProcessor(framework *ShoalPPFramework) *CertificateProcessor {
	return &CertificateProcessor{
		framework: framework,
		logger:    framework.logger.Named("certificate_processor"),
	}
}

func (cp *CertificateProcessor) ProcessCertificate(cert *types.Certificate) error {
	start := time.Now()

	if err := cp.validateCertificate(cert); err != nil {
		cp.framework.UpdateReputation(cert.Header.Author, time.Since(start), false)
		return err
	}

	if err := cp.shoalPPValidation(cert); err != nil {
		cp.framework.UpdateReputation(cert.Header.Author, time.Since(start), false)
		return err
	}

	if cp.framework.config.EnableFastDirectCommit {
		cp.checkFastCommitEligibility(cert)
	}

	if cp.framework.config.EnableDynamicAnchorFreq {
		cp.checkDynamicAnchorOpportunity(cert)
	}

	if cp.framework.config.EnableParallelDAGs {
		cp.submitToParallelDAG(cert)
	}

	processingTime := time.Since(start)
	cp.framework.UpdateReputation(cert.Header.Author, processingTime, true)

	cp.updateShoalPPMetrics(cert, processingTime)

	return nil
}

func (cp *CertificateProcessor) validateCertificate(cert *types.Certificate) error {
	if cert == nil {
		return ErrInvalidConfiguration
	}

	if cert.Header == nil {
		return ErrInvalidConfiguration
	}

	return nil
}

func (cp *CertificateProcessor) shoalPPValidation(cert *types.Certificate) error {
	if cp.framework.config.EnableLeaderReputation {
		rep := cp.framework.reputationManager.GetReputation(cert.Header.Author)
		if rep.GetScore() < cp.framework.config.MinReputationScore {
			cp.logger.Debug("Certificate from low-reputation node",
				zap.String("author", string(cert.Header.Author)),
				zap.Float64("reputation", rep.GetScore()),
			)
		}
	}

	if cp.detectByzantinePatterns(cert) {
		return ErrInvalidConfiguration
	}

	return nil
}

func (cp *CertificateProcessor) detectByzantinePatterns(cert *types.Certificate) bool {
	now := time.Now()
	if cert.Header.Timestamp.After(now.Add(time.Minute)) {
		cp.logger.Warn("Certificate with future timestamp detected",
			zap.String("author", string(cert.Header.Author)),
			zap.Time("cert_timestamp", cert.Header.Timestamp),
			zap.Time("current_time", now),
		)
		return true
	}

	if now.Sub(cert.Header.Timestamp) > time.Hour {
		cp.logger.Warn("Certificate too old",
			zap.String("author", string(cert.Header.Author)),
			zap.Duration("age", now.Sub(cert.Header.Timestamp)),
		)
		return true
	}

	return false
}

func (cp *CertificateProcessor) checkFastCommitEligibility(cert *types.Certificate) {
	if cp.framework.fastCommitManager != nil {
		cp.framework.fastCommitManager.UpdateUncertifiedCount(cert.Header.Author, 5)
	}
}

func (cp *CertificateProcessor) checkDynamicAnchorOpportunity(cert *types.Certificate) {
	if cp.shouldConsiderForDynamicAnchor(cert) {
		candidate := NewAnchorCandidate(cert.Header.Author, cert.Header.Round, cert)

		if cp.framework.dynamicAnchorManager != nil {
			go func() {
				_, err := cp.framework.dynamicAnchorManager.ProcessDynamicAnchor(candidate)
				if err != nil {
					cp.logger.Debug("Dynamic anchor processing failed",
						zap.String("node_id", string(candidate.NodeID)),
						zap.Error(err),
					)
				}
			}()
		}
	}
}

func (cp *CertificateProcessor) shouldConsiderForDynamicAnchor(cert *types.Certificate) bool {
	if cert.Header.Round%2 != 0 {
		return false
	}

	if cp.framework.config.EnableLeaderReputation {
		rep := cp.framework.reputationManager.GetReputation(cert.Header.Author)
		return rep.GetScore() > 0.7
	}

	return true
}

func (cp *CertificateProcessor) submitToParallelDAG(cert *types.Certificate) {
	if cp.framework.parallelDAGManager != nil {
		op := PipelineOperation{
			ID:   cert.Hash.String(),
			Type: "certificate",
			Data: cert,
			ShoalPPFeatures: ShoalPPOperationFeatures{
				UseParallelDAG: true,
			},
		}

		err := cp.framework.parallelDAGManager.DispatchOperation(op)
		if err != nil {
			cp.logger.Debug("Failed to submit to parallel DAG",
				zap.String("cert_hash", cert.Hash.String()),
				zap.Error(err),
			)
		}
	}
}

func (cp *CertificateProcessor) updateShoalPPMetrics(cert *types.Certificate, processingTime time.Duration) {
	cp.framework.metrics.UpdateShoalPPLatency("certificate", processingTime)

	cp.framework.UpdateShoalPPMetrics(
		cert.Header.Author,
		false,
		cp.shouldConsiderForDynamicAnchor(cert),
		false,
		false,
		0.8,
	)
}

type NetworkOptimizer struct {
	framework *ShoalPPFramework
	logger    *zap.Logger
}

func NewNetworkOptimizer(framework *ShoalPPFramework) *NetworkOptimizer {
	return &NetworkOptimizer{
		framework: framework,
		logger:    framework.logger.Named("network_optimizer"),
	}
}

func (no *NetworkOptimizer) OptimizeMessage(nodeID types.NodeID, messageType string, priority int) time.Duration {
	rep := no.framework.reputationManager.GetReputation(nodeID)

	baseTimeout := no.framework.GetCurrentTimeout()

	optimizedTimeout := no.applyShoalPPOptimizations(baseTimeout, nodeID, messageType, priority, rep)

	no.logger.Debug("Optimized message timeout",
		zap.String("node_id", string(nodeID)),
		zap.String("message_type", messageType),
		zap.Int("priority", priority),
		zap.Duration("base_timeout", baseTimeout),
		zap.Duration("optimized_timeout", optimizedTimeout),
		zap.Float64("reputation", rep.GetScore()),
	)

	return optimizedTimeout
}

func (no *NetworkOptimizer) applyShoalPPOptimizations(baseTimeout time.Duration, nodeID types.NodeID,
	messageType string, priority int, rep *LeaderReputation) time.Duration {

	optimizedTimeout := baseTimeout

	reputationFactor := rep.GetScore()
	if reputationFactor < 0.1 {
		reputationFactor = 0.1
	}

	optimizedTimeout = time.Duration(float64(optimizedTimeout) / reputationFactor)

	switch messageType {
	case "fast_commit":
		if no.framework.config.EnableFastDirectCommit {
			optimizedTimeout = no.framework.GetFastCommitTimeout()
		}
	case "dynamic_anchor":
		if no.framework.config.EnableDynamicAnchorFreq {
			optimizedTimeout = no.framework.adaptiveTimeout.GetDynamicAnchorTimeout()
		}
	case "parallel_dag":
		if no.framework.config.EnableParallelDAGs {
			optimizedTimeout = no.framework.adaptiveTimeout.GetParallelDAGTimeout()
		}
	}

	if priority > 8 {
		optimizedTimeout = time.Duration(float64(optimizedTimeout) * 0.7)
	} else if priority < 3 {
		optimizedTimeout = time.Duration(float64(optimizedTimeout) * 1.3)
	}

	shoalPPMetrics := rep.GetShoalPPMetrics()
	if fastCommitRatio, ok := shoalPPMetrics["fast_commit_ratio"].(float64); ok {
		if fastCommitRatio > 0.5 {
			optimizedTimeout = time.Duration(float64(optimizedTimeout) * 0.8)
		}
	}

	maxTimeout := no.framework.config.MaxTimeout
	minTimeout := no.framework.config.MinTimeout

	if optimizedTimeout > maxTimeout {
		optimizedTimeout = maxTimeout
	} else if optimizedTimeout < minTimeout {
		optimizedTimeout = minTimeout
	}

	return optimizedTimeout
}

func (no *NetworkOptimizer) OptimizeForNetworkConditions(conditions NetworkConditions) {
	congestionLevel := no.calculateCongestionLevel(conditions)
	byzantineLevel := no.calculateByzantineLevel(conditions)

	no.framework.adaptiveTimeout.UpdateNetworkCongestion(congestionLevel)
	no.framework.adaptiveTimeout.UpdateByzantineFaultDetection(byzantineLevel)

	no.logger.Debug("Network conditions optimized",
		zap.Float64("congestion_level", congestionLevel),
		zap.Float64("byzantine_level", byzantineLevel),
		zap.Float64("latency_avg_ms", conditions.AvgLatency.Milliseconds()),
		zap.Float64("error_rate", conditions.ErrorRate),
	)
}

type NetworkConditions struct {
	AvgLatency time.Duration
	ErrorRate  float64
	PacketLoss float64
	Throughput float64
	Jitter     time.Duration
}

func (no *NetworkOptimizer) calculateCongestionLevel(conditions NetworkConditions) float64 {
	latencyFactor := float64(conditions.AvgLatency.Milliseconds()) / 100.0
	packetLossFactor := conditions.PacketLoss * 10.0

	congestionLevel := (latencyFactor + packetLossFactor) / 2.0

	if congestionLevel < 0.5 {
		congestionLevel = 0.5
	} else if congestionLevel > 3.0 {
		congestionLevel = 3.0
	}

	return congestionLevel
}

func (no *NetworkOptimizer) calculateByzantineLevel(conditions NetworkConditions) float64 {
	errorFactor := conditions.ErrorRate * 5.0
	jitterFactor := float64(conditions.Jitter.Milliseconds()) / 50.0

	byzantineLevel := 1.0 + (errorFactor+jitterFactor)/2.0

	if byzantineLevel < 1.0 {
		byzantineLevel = 1.0
	} else if byzantineLevel > 2.0 {
		byzantineLevel = 2.0
	}

	return byzantineLevel
}

func (no *NetworkOptimizer) GetOptimizationStats() map[string]interface{} {
	factors := no.framework.adaptiveTimeout.GetNetworkConditionFactors()

	return map[string]interface{}{
		"congestion_factor":      factors["congestion_factor"],
		"byzantine_factor":       factors["byzantine_factor"],
		"effective_multiplier":   factors["effective_multiplier"],
		"adaptive_timeout_ms":    no.framework.GetCurrentTimeout().Milliseconds(),
		"fast_commit_timeout_ms": no.framework.GetFastCommitTimeout().Milliseconds(),
	}
}
