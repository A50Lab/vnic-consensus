package shoalpp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

type ParallelDAGPipeline struct {
	InstanceID int
	config     *ShoalPPConfig
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	operations    chan PipelineOperation
	results       chan PipelineResult
	workers       int
	staggerOffset time.Duration

	currentRound   types.Round
	certificates   map[types.Hash]*types.Certificate
	pendingBatches map[types.Hash]*types.Batch
	certMutex      sync.RWMutex

	interleavedOutput chan InterleavedDAGOutput
	outputSequence    int64
	lastOutputTime    time.Time

	metrics ParallelDAGMetrics

	fastCommitCandidates map[types.Hash]*FastCommitCandidate
	fastCommitMutex      sync.RWMutex
}

type InterleavedDAGOutput struct {
	InstanceID     int
	SequenceNumber int64
	Round          types.Round
	Certificates   []*types.Certificate
	Batches        []*types.Batch
	Timestamp      time.Time
	CommitRule     CommitRule
	FastCommitUsed bool
}

type FastCommitCandidate struct {
	Certificate          *types.Certificate
	UncertifiedProposals int
	Threshold            int
	CreationTime         time.Time
	CanCommitDirectly    bool
}

type ParallelDAGMetrics struct {
	ProcessedCertificates  int64
	ProcessedBatches       int64
	FastCommitsUsed        int64
	DynamicAnchorsUsed     int64
	AverageProcessingTime  time.Duration
	ThroughputCertsSec     float64
	InterleavedOutputs     int64
	StaggerDelayEfficiency float64

	CertificateLatency time.Duration
	BatchLatency       time.Duration
	FastCommitLatency  time.Duration

	mu sync.RWMutex
}

func NewParallelDAGPipeline(instanceID int, config *ShoalPPConfig) *ParallelDAGPipeline {
	ctx, cancel := context.WithCancel(context.Background())

	pipeline := &ParallelDAGPipeline{
		InstanceID:           instanceID,
		config:               config,
		ctx:                  ctx,
		cancel:               cancel,
		operations:           make(chan PipelineOperation, config.PipelineDepth),
		results:              make(chan PipelineResult, config.PipelineDepth),
		workers:              2,
		staggerOffset:        time.Duration(instanceID) * config.DAGStaggerDelay,
		currentRound:         types.Round(0),
		certificates:         make(map[types.Hash]*types.Certificate),
		pendingBatches:       make(map[types.Hash]*types.Batch),
		interleavedOutput:    make(chan InterleavedDAGOutput, 10),
		fastCommitCandidates: make(map[types.Hash]*FastCommitCandidate),
		lastOutputTime:       time.Now(),
	}

	for i := 0; i < pipeline.workers; i++ {
		pipeline.wg.Add(1)
		go pipeline.worker()
	}

	pipeline.wg.Add(1)
	go pipeline.outputInterleaver()

	if config.EnableFastDirectCommit {
		pipeline.wg.Add(1)
		go pipeline.fastCommitProcessor()
	}

	return pipeline
}

func (pdp *ParallelDAGPipeline) worker() {
	defer pdp.wg.Done()

	for {
		select {
		case <-pdp.ctx.Done():
			return
		case op := <-pdp.operations:
			start := time.Now()

			if pdp.staggerOffset > 0 {
				time.Sleep(pdp.staggerOffset)
			}

			result := pdp.processDAGOperation(op)
			result.Duration = time.Since(start)
			result.DAGInstance = pdp.InstanceID

			pdp.updateMetrics(result)

			select {
			case pdp.results <- result:
			case <-pdp.ctx.Done():
				return
			}

			if op.Callback != nil {
				op.Callback(result)
			}
		}
	}
}

func (pdp *ParallelDAGPipeline) processDAGOperation(op PipelineOperation) PipelineResult {
	result := PipelineResult{
		OperationID:     op.ID,
		Success:         true,
		DAGInstance:     pdp.InstanceID,
		ProcessingStage: "dag_processing",
	}

	switch op.Type {
	case "certificate":
		result = pdp.processCertificate(op, result)
	case "batch":
		result = pdp.processBatch(op, result)
	case "fast_commit":
		result = pdp.processFastCommit(op, result)
	case "dynamic_anchor":
		result = pdp.processDynamicAnchor(op, result)
	default:
		result.Success = false
		result.Error = ErrInvalidConfiguration
	}

	result.ProcessingStage = "completed"
	return result
}

func (pdp *ParallelDAGPipeline) processCertificate(op PipelineOperation, result PipelineResult) PipelineResult {
	cert, ok := op.Data.(*types.Certificate)
	if !ok {
		result.Success = false
		result.Error = ErrInvalidConfiguration
		return result
	}

	pdp.certMutex.Lock()
	defer pdp.certMutex.Unlock()

	pdp.certificates[cert.Hash] = cert

	if pdp.config.EnableFastDirectCommit {
		pdp.checkFastCommitOpportunity(cert)
	}

	result.Data = cert
	result.ShoalPPMetrics.ParallelDAGUsed = true

	atomic.AddInt64(&pdp.metrics.ProcessedCertificates, 1)

	if pdp.isCertificateAnchor(cert) {
		pdp.generateInterleavedOutput(cert)
	}

	return result
}

func (pdp *ParallelDAGPipeline) processBatch(op PipelineOperation, result PipelineResult) PipelineResult {
	batch, ok := op.Data.(*types.Batch)
	if !ok {
		result.Success = false
		result.Error = ErrInvalidConfiguration
		return result
	}

	pdp.certMutex.Lock()
	defer pdp.certMutex.Unlock()

	pdp.pendingBatches[batch.ID] = batch

	result.Data = batch
	result.ShoalPPMetrics.ParallelDAGUsed = true

	atomic.AddInt64(&pdp.metrics.ProcessedBatches, 1)

	return result
}

func (pdp *ParallelDAGPipeline) processFastCommit(op PipelineOperation, result PipelineResult) PipelineResult {
	candidate := op.ShoalPPFeatures.AnchorCandidate
	if candidate == nil {
		result.Success = false
		result.Error = ErrFastCommitFailed
		return result
	}

	if candidate.CanUseFastCommit(pdp.config.FastCommitThreshold) {
		result.ShoalPPMetrics.FastCommitUsed = true
		result.ShoalPPMetrics.CommitRuleApplied = FastDirectCommitRule
		result.ShoalPPMetrics.MessageDelayCount = 4
		result.ShoalPPMetrics.LatencyReduction = time.Millisecond * 25

		atomic.AddInt64(&pdp.metrics.FastCommitsUsed, 1)

		pdp.generateFastCommitOutput(candidate)
	} else {
		result.ShoalPPMetrics.CommitRuleApplied = OriginalCommitRule
		result.ShoalPPMetrics.MessageDelayCount = 6
	}

	result.Data = candidate
	return result
}

func (pdp *ParallelDAGPipeline) processDynamicAnchor(op PipelineOperation, result PipelineResult) PipelineResult {
	candidate := op.ShoalPPFeatures.AnchorCandidate
	if candidate == nil {
		result.Success = false
		result.Error = ErrDynamicAnchorFailed
		return result
	}

	result.ShoalPPMetrics.DynamicAnchorUsed = true
	result.ShoalPPMetrics.ThroughputGain = 1.4

	atomic.AddInt64(&pdp.metrics.DynamicAnchorsUsed, 1)

	candidate.DynamicPriority = pdp.calculateDynamicPriority(candidate)

	result.Data = candidate
	return result
}

func (pdp *ParallelDAGPipeline) checkFastCommitOpportunity(cert *types.Certificate) {
	pdp.fastCommitMutex.Lock()
	defer pdp.fastCommitMutex.Unlock()

	uncertifiedCount := pdp.countUncertifiedProposals(cert.Header.Author)

	candidate := &FastCommitCandidate{
		Certificate:          cert,
		UncertifiedProposals: uncertifiedCount,
		Threshold:            pdp.config.FastCommitThreshold,
		CreationTime:         time.Now(),
		CanCommitDirectly:    uncertifiedCount >= pdp.config.FastCommitThreshold,
	}

	pdp.fastCommitCandidates[cert.Hash] = candidate
}

func (pdp *ParallelDAGPipeline) countUncertifiedProposals(nodeID types.NodeID) int {
	count := 0
	for _, cert := range pdp.certificates {
		if cert.Header.Author == nodeID {
			count++
		}
	}
	return count
}

func (pdp *ParallelDAGPipeline) isCertificateAnchor(cert *types.Certificate) bool {
	return cert.Header.Round%2 == 0
}

func (pdp *ParallelDAGPipeline) calculateDynamicPriority(candidate *AnchorCandidate) float64 {
	basePriority := 1.0

	age := time.Since(candidate.Timestamp)
	ageFactor := 1.0 + (age.Seconds() / 10.0)

	proposalFactor := 1.0 + (float64(candidate.UncertifiedProposals) * 0.1)

	return basePriority * ageFactor * proposalFactor
}

func (pdp *ParallelDAGPipeline) generateInterleavedOutput(cert *types.Certificate) {
	output := InterleavedDAGOutput{
		InstanceID:     pdp.InstanceID,
		SequenceNumber: atomic.AddInt64(&pdp.outputSequence, 1),
		Round:          cert.Header.Round,
		Certificates:   []*types.Certificate{cert},
		Batches:        pdp.getRelatedBatches(cert),
		Timestamp:      time.Now(),
		CommitRule:     OriginalCommitRule,
		FastCommitUsed: false,
	}

	select {
	case pdp.interleavedOutput <- output:
		atomic.AddInt64(&pdp.metrics.InterleavedOutputs, 1)
	default:
	}
}

func (pdp *ParallelDAGPipeline) generateFastCommitOutput(candidate *AnchorCandidate) {
	output := InterleavedDAGOutput{
		InstanceID:     pdp.InstanceID,
		SequenceNumber: atomic.AddInt64(&pdp.outputSequence, 1),
		Round:          candidate.Round,
		Certificates:   []*types.Certificate{candidate.Certificate},
		Batches:        pdp.getRelatedBatches(candidate.Certificate),
		Timestamp:      time.Now(),
		CommitRule:     FastDirectCommitRule,
		FastCommitUsed: true,
	}

	select {
	case pdp.interleavedOutput <- output:
		atomic.AddInt64(&pdp.metrics.InterleavedOutputs, 1)
	default:
	}
}

func (pdp *ParallelDAGPipeline) getRelatedBatches(cert *types.Certificate) []*types.Batch {
	pdp.certMutex.RLock()
	defer pdp.certMutex.RUnlock()

	var batches []*types.Batch
	count := 0
	for _, batch := range pdp.pendingBatches {
		if count >= 3 {
			break
		}
		batches = append(batches, batch)
		count++
	}

	return batches
}

func (pdp *ParallelDAGPipeline) outputInterleaver() {
	defer pdp.wg.Done()

	for {
		select {
		case <-pdp.ctx.Done():
			return
		case output := <-pdp.interleavedOutput:
			pdp.processInterleavedOutput(output)
		}
	}
}

func (pdp *ParallelDAGPipeline) processInterleavedOutput(output InterleavedDAGOutput) {
	pdp.lastOutputTime = output.Timestamp

	expectedDelay := pdp.staggerOffset
	actualDelay := output.Timestamp.Sub(pdp.lastOutputTime)
	efficiency := 1.0
	if expectedDelay > 0 {
		efficiency = float64(expectedDelay) / float64(actualDelay)
		if efficiency > 1.0 {
			efficiency = 1.0
		}
	}

	pdp.metrics.mu.Lock()
	pdp.metrics.StaggerDelayEfficiency = efficiency
	pdp.metrics.mu.Unlock()
}

func (pdp *ParallelDAGPipeline) fastCommitProcessor() {
	defer pdp.wg.Done()

	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()

	for {
		select {
		case <-pdp.ctx.Done():
			return
		case <-ticker.C:
			pdp.processFastCommitCandidates()
		}
	}
}

func (pdp *ParallelDAGPipeline) processFastCommitCandidates() {
	pdp.fastCommitMutex.Lock()
	defer pdp.fastCommitMutex.Unlock()

	now := time.Now()
	toDelete := make([]types.Hash, 0)

	for hash, candidate := range pdp.fastCommitCandidates {
		if now.Sub(candidate.CreationTime) > time.Millisecond*100 {
			if candidate.CanCommitDirectly {
				anchorCandidate := &AnchorCandidate{
					NodeID:               candidate.Certificate.Header.Author,
					Round:                candidate.Certificate.Header.Round,
					Certificate:          candidate.Certificate,
					Timestamp:            candidate.CreationTime,
					CommitRule:           FastDirectCommitRule,
					UncertifiedProposals: candidate.UncertifiedProposals,
				}
				pdp.generateFastCommitOutput(anchorCandidate)
			}
			toDelete = append(toDelete, hash)
		}
	}

	for _, hash := range toDelete {
		delete(pdp.fastCommitCandidates, hash)
	}
}

func (pdp *ParallelDAGPipeline) updateMetrics(result PipelineResult) {
	pdp.metrics.mu.Lock()
	defer pdp.metrics.mu.Unlock()

	alpha := 0.1
	if pdp.metrics.AverageProcessingTime == 0 {
		pdp.metrics.AverageProcessingTime = result.Duration
	} else {
		pdp.metrics.AverageProcessingTime = time.Duration(
			float64(pdp.metrics.AverageProcessingTime)*(1-alpha) + float64(result.Duration)*alpha,
		)
	}

	switch result.ProcessingStage {
	case "certificate":
		pdp.metrics.CertificateLatency = result.Duration
	case "batch":
		pdp.metrics.BatchLatency = result.Duration
	case "fast_commit":
		pdp.metrics.FastCommitLatency = result.Duration
	}

	elapsed := time.Since(pdp.lastOutputTime)
	if elapsed > 0 {
		pdp.metrics.ThroughputCertsSec = float64(pdp.metrics.ProcessedCertificates) / elapsed.Seconds()
	}
}

func (pdp *ParallelDAGPipeline) SubmitOperation(op PipelineOperation) error {
	select {
	case pdp.operations <- op:
		return nil
	case <-pdp.ctx.Done():
		return context.Canceled
	default:
		return ErrPipelineFull
	}
}

func (pdp *ParallelDAGPipeline) GetInterleavedOutput() <-chan InterleavedDAGOutput {
	return pdp.interleavedOutput
}

func (pdp *ParallelDAGPipeline) GetMetrics() ParallelDAGMetrics {
	pdp.metrics.mu.RLock()
	defer pdp.metrics.mu.RUnlock()

	return pdp.metrics
}

func (pdp *ParallelDAGPipeline) Stop() {
	pdp.cancel()
	close(pdp.operations)
	pdp.wg.Wait()
	close(pdp.results)
	close(pdp.interleavedOutput)
}

func (pdp *ParallelDAGPipeline) GetCurrentRound() types.Round {
	pdp.certMutex.RLock()
	defer pdp.certMutex.RUnlock()
	return pdp.currentRound
}

func (pdp *ParallelDAGPipeline) AdvanceRound() {
	pdp.certMutex.Lock()
	defer pdp.certMutex.Unlock()
	pdp.currentRound++
}

func (pdp *ParallelDAGPipeline) GetDAGStats() map[string]interface{} {
	pdp.certMutex.RLock()
	certCount := len(pdp.certificates)
	batchCount := len(pdp.pendingBatches)
	pdp.certMutex.RUnlock()

	pdp.fastCommitMutex.RLock()
	fastCommitCandidates := len(pdp.fastCommitCandidates)
	pdp.fastCommitMutex.RUnlock()

	metrics := pdp.GetMetrics()

	return map[string]interface{}{
		"instance_id":              pdp.InstanceID,
		"current_round":            int64(pdp.currentRound),
		"certificates_count":       certCount,
		"pending_batches_count":    batchCount,
		"fast_commit_candidates":   fastCommitCandidates,
		"processed_certificates":   metrics.ProcessedCertificates,
		"processed_batches":        metrics.ProcessedBatches,
		"fast_commits_used":        metrics.FastCommitsUsed,
		"dynamic_anchors_used":     metrics.DynamicAnchorsUsed,
		"average_processing_ms":    metrics.AverageProcessingTime.Milliseconds(),
		"throughput_certs_sec":     metrics.ThroughputCertsSec,
		"interleaved_outputs":      metrics.InterleavedOutputs,
		"stagger_delay_efficiency": metrics.StaggerDelayEfficiency,
		"certificate_latency_ms":   metrics.CertificateLatency.Milliseconds(),
		"batch_latency_ms":         metrics.BatchLatency.Milliseconds(),
		"fast_commit_latency_ms":   metrics.FastCommitLatency.Milliseconds(),
	}
}
