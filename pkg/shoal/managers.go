package shoalpp

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

type ParallelDAGManager struct {
	config  *ShoalPPConfig
	logger  *zap.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
	mu      sync.RWMutex

	dagInstances map[int]*ParallelDAGPipeline
	dispatcher   *DAGDispatcher
}

type DAGDispatcher struct {
	instances    map[int]*ParallelDAGPipeline
	roundRobin   int
	loadBalancer *DAGLoadBalancer
	mu           sync.Mutex
}

type DAGLoadBalancer struct {
	instances       map[int]*ParallelDAGPipeline
	loadMetrics     map[int]*DAGLoadMetrics
	balanceStrategy LoadBalanceStrategy
}

type DAGLoadMetrics struct {
	QueueLength    int
	ProcessingTime time.Duration
	Utilization    float64
	LastUpdate     time.Time
}

type LoadBalanceStrategy int

const (
	RoundRobinStrategy LoadBalanceStrategy = iota
	LeastLoadedStrategy
	WeightedStrategy
)

func NewParallelDAGManager(config *ShoalPPConfig, logger *zap.Logger) *ParallelDAGManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &ParallelDAGManager{
		config:       config,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		dagInstances: make(map[int]*ParallelDAGPipeline),
		dispatcher:   NewDAGDispatcher(),
	}
}

func (pdm *ParallelDAGManager) Start() error {
	pdm.mu.Lock()
	defer pdm.mu.Unlock()

	if pdm.running {
		return nil
	}

	pdm.logger.Info("Starting Parallel DAG Manager")

	// Create DAG instances
	for i := 0; i < pdm.config.ParallelDAGInstances; i++ {
		instance := NewParallelDAGPipeline(i, pdm.config)
		pdm.dagInstances[i] = instance
		pdm.dispatcher.AddInstance(i, instance)
	}

	pdm.running = true
	pdm.logger.Info("Parallel DAG Manager started with instances",
		zap.Int("instances", len(pdm.dagInstances)))

	return nil
}

func (pdm *ParallelDAGManager) Stop() {
	pdm.mu.Lock()
	defer pdm.mu.Unlock()

	if !pdm.running {
		return
	}

	pdm.logger.Info("Stopping Parallel DAG Manager")

	pdm.cancel()

	// Stop all DAG instances
	for _, instance := range pdm.dagInstances {
		instance.Stop()
	}

	pdm.running = false
	pdm.logger.Info("Parallel DAG Manager stopped")
}

func (pdm *ParallelDAGManager) SubmitOperation(dagInstance int, op PipelineOperation) error {
	if instance, exists := pdm.dagInstances[dagInstance]; exists {
		return instance.SubmitOperation(op)
	}
	return ErrParallelDAGFailed
}

func (pdm *ParallelDAGManager) DispatchOperation(op PipelineOperation) error {
	instanceID := pdm.dispatcher.SelectInstance()
	op.DAGInstance = instanceID
	return pdm.SubmitOperation(instanceID, op)
}

func (pdm *ParallelDAGManager) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"active_instances": len(pdm.dagInstances),
		"total_instances":  pdm.config.ParallelDAGInstances,
		"running":          pdm.running,
	}

	var totalOps, totalUtil float64
	instanceStats := make(map[string]interface{})

	for id, instance := range pdm.dagInstances {
		instanceMetrics := instance.GetDAGStats()
		instanceStats[fmt.Sprintf("instance_%d", id)] = instanceMetrics

		if ops, ok := instanceMetrics["processed_certificates"].(int64); ok {
			totalOps += float64(ops)
		}
		if util, ok := instanceMetrics["stagger_delay_efficiency"].(float64); ok {
			totalUtil += util
		}
	}

	if len(pdm.dagInstances) > 0 {
		stats["avg_utilization"] = totalUtil / float64(len(pdm.dagInstances))
	}

	stats["total_operations"] = totalOps
	stats["throughput_gain"] = totalOps / float64(pdm.config.ParallelDAGInstances)
	stats["instances"] = instanceStats

	return stats
}

func NewDAGDispatcher() *DAGDispatcher {
	return &DAGDispatcher{
		instances:    make(map[int]*ParallelDAGPipeline),
		loadBalancer: NewDAGLoadBalancer(),
	}
}

func (dd *DAGDispatcher) AddInstance(id int, instance *ParallelDAGPipeline) {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	dd.instances[id] = instance
	dd.loadBalancer.AddInstance(id, instance)
}

func (dd *DAGDispatcher) SelectInstance() int {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	return dd.loadBalancer.SelectInstance()
}

func NewDAGLoadBalancer() *DAGLoadBalancer {
	return &DAGLoadBalancer{
		instances:       make(map[int]*ParallelDAGPipeline),
		loadMetrics:     make(map[int]*DAGLoadMetrics),
		balanceStrategy: LeastLoadedStrategy,
	}
}

func (dlb *DAGLoadBalancer) AddInstance(id int, instance *ParallelDAGPipeline) {
	dlb.instances[id] = instance
	dlb.loadMetrics[id] = &DAGLoadMetrics{
		LastUpdate: time.Now(),
	}
}

func (dlb *DAGLoadBalancer) SelectInstance() int {
	switch dlb.balanceStrategy {
	case LeastLoadedStrategy:
		return dlb.selectLeastLoaded()
	case WeightedStrategy:
		return dlb.selectWeighted()
	default:
		return dlb.selectRoundRobin()
	}
}

func (dlb *DAGLoadBalancer) selectLeastLoaded() int {
	var bestInstance int
	var minLoad float64 = 1.0

	for id, metrics := range dlb.loadMetrics {
		if metrics.Utilization < minLoad {
			minLoad = metrics.Utilization
			bestInstance = id
		}
	}

	return bestInstance
}

func (dlb *DAGLoadBalancer) selectRoundRobin() int {
	instances := make([]int, 0, len(dlb.instances))
	for id := range dlb.instances {
		instances = append(instances, id)
	}

	if len(instances) == 0 {
		return 0
	}

	sort.Ints(instances)
	selected := instances[0] // Default to first instance
	return selected
}

func (dlb *DAGLoadBalancer) selectWeighted() int {
	// Implementation would use weighted selection based on performance
	return dlb.selectLeastLoaded() // Fallback for now
}

type AnchorSkipManager struct {
	config  *ShoalPPConfig
	logger  *zap.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
	mu      sync.RWMutex

	skipCandidates map[types.Hash]*SkipCandidate
	skipReasons    map[string]int64
	skipMutex      sync.RWMutex
}

type SkipCandidate struct {
	Candidate  *AnchorCandidate
	SkipReason string
	SkipTime   time.Time
	RetryCount int
	MaxRetries int
}

func NewAnchorSkipManager(config *ShoalPPConfig, logger *zap.Logger) *AnchorSkipManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &AnchorSkipManager{
		config:         config,
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
		skipCandidates: make(map[types.Hash]*SkipCandidate),
		skipReasons:    make(map[string]int64),
	}
}

func (asm *AnchorSkipManager) Start() error {
	asm.mu.Lock()
	defer asm.mu.Unlock()

	if asm.running {
		return nil
	}

	asm.logger.Info("Starting Anchor Skip Manager")

	// Start skip processor
	asm.wg.Add(1)
	go asm.skipProcessor()

	asm.running = true
	asm.logger.Info("Anchor Skip Manager started")

	return nil
}

func (asm *AnchorSkipManager) Stop() {
	asm.mu.Lock()
	defer asm.mu.Unlock()

	if !asm.running {
		return
	}

	asm.logger.Info("Stopping Anchor Skip Manager")

	asm.cancel()
	asm.wg.Wait()

	asm.running = false
	asm.logger.Info("Anchor Skip Manager stopped")
}

func (asm *AnchorSkipManager) ShouldSkip(candidate *AnchorCandidate) bool {
	if !asm.config.AnchorSkippingEnabled {
		return false
	}

	// Check timeout threshold
	if candidate.ShouldSkip(asm.config.AnchorSkippingThreshold) {
		asm.addSkipCandidate(candidate, "timeout_exceeded")
		return true
	}

	// Check for other skip conditions
	if asm.checkSkipConditions(candidate) {
		return true
	}

	return false
}

func (asm *AnchorSkipManager) checkSkipConditions(candidate *AnchorCandidate) bool {
	// Check if node is frequently unresponsive
	if asm.isNodeUnresponsive(candidate.NodeID) {
		asm.addSkipCandidate(candidate, "node_unresponsive")
		return true
	}

	// Check network conditions
	if asm.isPoorNetworkConditions() {
		asm.addSkipCandidate(candidate, "poor_network")
		return true
	}

	return false
}

func (asm *AnchorSkipManager) addSkipCandidate(candidate *AnchorCandidate, reason string) {
	asm.skipMutex.Lock()
	defer asm.skipMutex.Unlock()

	skipCandidate := &SkipCandidate{
		Candidate:  candidate,
		SkipReason: reason,
		SkipTime:   time.Now(),
		MaxRetries: 3,
	}

	asm.skipCandidates[candidate.Certificate.Hash] = skipCandidate
	asm.skipReasons[reason]++

	candidate.SkipReason = reason

	asm.logger.Debug("Anchor candidate skipped",
		zap.String("node_id", string(candidate.NodeID)),
		zap.String("reason", reason),
		zap.Uint64("round", uint64(candidate.Round)),
	)
}

func (asm *AnchorSkipManager) skipProcessor() {
	defer asm.wg.Done()

	ticker := time.NewTicker(time.Second * 10) // Process every 10 seconds
	defer ticker.Stop()

	for {
		select {
		case <-asm.ctx.Done():
			return
		case <-ticker.C:
			asm.processSkipCandidates()
		}
	}
}

func (asm *AnchorSkipManager) processSkipCandidates() {
	asm.skipMutex.Lock()
	defer asm.skipMutex.Unlock()

	now := time.Now()
	toDelete := make([]types.Hash, 0)

	for hash, skipCandidate := range asm.skipCandidates {
		// Check if skip candidate can be retried
		if now.Sub(skipCandidate.SkipTime) > time.Minute*5 { // Retry after 5 minutes
			if skipCandidate.RetryCount < skipCandidate.MaxRetries {
				skipCandidate.RetryCount++
				skipCandidate.SkipTime = now
				// Could re-evaluate the candidate here
			} else {
				// Max retries reached, remove from skip list
				toDelete = append(toDelete, hash)
			}
		}
	}

	// Clean up expired skip candidates
	for _, hash := range toDelete {
		delete(asm.skipCandidates, hash)
	}
}

func (asm *AnchorSkipManager) isNodeUnresponsive(nodeID types.NodeID) bool {
	// This would integrate with reputation management
	// For now, use a simple heuristic
	return false
}

func (asm *AnchorSkipManager) isPoorNetworkConditions() bool {
	// This would integrate with network monitoring
	// For now, return false
	return false
}

func (asm *AnchorSkipManager) GetStats() map[string]interface{} {
	asm.skipMutex.RLock()
	skipCount := len(asm.skipCandidates)
	reasons := make(map[string]int64)
	for k, v := range asm.skipReasons {
		reasons[k] = v
	}
	asm.skipMutex.RUnlock()

	return map[string]interface{}{
		"active_skips":   skipCount,
		"skip_reasons":   reasons,
		"enabled":        asm.config.AnchorSkippingEnabled,
		"skip_threshold": asm.config.AnchorSkippingThreshold.Milliseconds(),
	}
}

type InterleavedOrderer struct {
	config  *ShoalPPConfig
	logger  *zap.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
	mu      sync.RWMutex

	outputBuffer map[int64]*InterleavedDAGOutput
	nextSequence int64
	bufferMutex  sync.RWMutex
}

func NewInterleavedOrderer(config *ShoalPPConfig, logger *zap.Logger) *InterleavedOrderer {
	ctx, cancel := context.WithCancel(context.Background())

	return &InterleavedOrderer{
		config:       config,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		outputBuffer: make(map[int64]*InterleavedDAGOutput),
		nextSequence: 1,
	}
}

func (io *InterleavedOrderer) Start() error {
	io.mu.Lock()
	defer io.mu.Unlock()

	if io.running {
		return nil
	}

	io.logger.Info("Starting Interleaved Orderer")

	// Start ordering processor
	io.wg.Add(1)
	go io.orderingProcessor()

	io.running = true
	io.logger.Info("Interleaved Orderer started")

	return nil
}

func (io *InterleavedOrderer) Stop() {
	io.mu.Lock()
	defer io.mu.Unlock()

	if !io.running {
		return
	}

	io.logger.Info("Stopping Interleaved Orderer")

	io.cancel()
	io.wg.Wait()

	io.running = false
	io.logger.Info("Interleaved Orderer stopped")
}

func (io *InterleavedOrderer) OrderOutputs(outputs []InterleavedDAGOutput) []InterleavedDAGOutput {
	// Sort by sequence number and timestamp for deterministic ordering
	sort.Slice(outputs, func(i, j int) bool {
		if outputs[i].SequenceNumber != outputs[j].SequenceNumber {
			return outputs[i].SequenceNumber < outputs[j].SequenceNumber
		}
		return outputs[i].Timestamp.Before(outputs[j].Timestamp)
	})

	return outputs
}

func (io *InterleavedOrderer) AddOutput(output InterleavedDAGOutput) {
	io.bufferMutex.Lock()
	defer io.bufferMutex.Unlock()

	io.outputBuffer[output.SequenceNumber] = &output
}

func (io *InterleavedOrderer) GetNextOrdered() *InterleavedDAGOutput {
	io.bufferMutex.Lock()
	defer io.bufferMutex.Unlock()

	if output, exists := io.outputBuffer[io.nextSequence]; exists {
		delete(io.outputBuffer, io.nextSequence)
		io.nextSequence++
		return output
	}

	return nil
}

func (io *InterleavedOrderer) orderingProcessor() {
	defer io.wg.Done()

	ticker := time.NewTicker(time.Millisecond * 10) // Process every 10ms
	defer ticker.Stop()

	for {
		select {
		case <-io.ctx.Done():
			return
		case <-ticker.C:
			io.processOrdering()
		}
	}
}

func (io *InterleavedOrderer) processOrdering() {
	// Get available outputs
	output := io.GetNextOrdered()
	if output != nil {
		// Process the ordered output
		io.logger.Debug("Processing ordered output",
			zap.Int("instance_id", output.InstanceID),
			zap.Int64("sequence", output.SequenceNumber),
			zap.Bool("fast_commit", output.FastCommitUsed),
		)
	}
}

func (io *InterleavedOrderer) GetStats() map[string]interface{} {
	io.bufferMutex.RLock()
	bufferSize := len(io.outputBuffer)
	nextSeq := io.nextSequence
	io.bufferMutex.RUnlock()

	return map[string]interface{}{
		"buffer_size":   bufferSize,
		"next_sequence": nextSeq,
		"running":       io.running,
	}
}
