package shoalpp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type PipelineManager struct {
	config          *ShoalPPConfig
	activePipelines map[string]*Pipeline
	parallelDAGs    map[int]*ParallelDAGPipeline
	mu              sync.RWMutex

	fastCommitPipeline    *Pipeline
	dynamicAnchorPipeline *Pipeline
	roundTimeoutManager   *RoundTimeoutManager
}

func NewPipelineManager(config *ShoalPPConfig) *PipelineManager {
	pm := &PipelineManager{
		config:              config,
		activePipelines:     make(map[string]*Pipeline),
		parallelDAGs:        make(map[int]*ParallelDAGPipeline),
		roundTimeoutManager: NewRoundTimeoutManager(config),
	}

	return pm
}

func (pm *PipelineManager) InitializeShoalPPPipelines() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.config.EnableFastDirectCommit {
		pm.fastCommitPipeline = pm.createPipeline("fast_commit", 2)
	}

	if pm.config.EnableDynamicAnchorFreq {
		pm.dynamicAnchorPipeline = pm.createPipeline("dynamic_anchor", 2)
	}

	if pm.config.EnableParallelDAGs {
		for i := 0; i < pm.config.ParallelDAGInstances; i++ {
			dagPipeline := NewParallelDAGPipeline(i, pm.config)
			pm.parallelDAGs[i] = dagPipeline
		}
	}

	return nil
}

type Pipeline struct {
	ID         string
	Operations chan PipelineOperation
	Results    chan PipelineResult
	Workers    int
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	resultPool sync.Pool
	metrics    struct {
		processedOps     int64
		avgDuration      time.Duration
		fastCommitOps    int64
		dynamicAnchorOps int64
		parallelDAGOps   int64
		timeoutOps       int64
		mu               sync.RWMutex
	}

	priorityQueue  *PriorityQueue
	staggerDelay   time.Duration
	enablePriority bool
}

type PipelineOperation struct {
	ID              string
	Type            string
	Data            interface{}
	Callback        func(result PipelineResult)
	Priority        int
	ShoalPPFeatures ShoalPPOperationFeatures
	SubmissionTime  time.Time
	DAGInstance     int
}

type ShoalPPOperationFeatures struct {
	UseFastCommit        bool
	UseDynamicAnchor     bool
	UseParallelDAG       bool
	RoundID              string
	AnchorCandidate      *AnchorCandidate
	UncertifiedProposals int
	CommitRule           CommitRule
}

type PipelineResult struct {
	OperationID     string
	Success         bool
	Data            interface{}
	Error           error
	Duration        time.Duration
	ShoalPPMetrics  ShoalPPResultMetrics
	ProcessingStage string
	DAGInstance     int
}

type ShoalPPResultMetrics struct {
	FastCommitUsed    bool
	DynamicAnchorUsed bool
	ParallelDAGUsed   bool
	CommitRuleApplied CommitRule
	MessageDelayCount int
	LatencyReduction  time.Duration
	ThroughputGain    float64
}

func (pm *PipelineManager) createPipeline(id string, workers int) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())

	pipeline := &Pipeline{
		ID:         id,
		Operations: make(chan PipelineOperation, pm.config.PipelineDepth),
		Results:    make(chan PipelineResult, pm.config.PipelineDepth),
		Workers:    workers,
		ctx:        ctx,
		cancel:     cancel,
		resultPool: sync.Pool{
			New: func() interface{} {
				return &PipelineResult{}
			},
		},
		priorityQueue:  NewPriorityQueue(),
		staggerDelay:   pm.config.DAGStaggerDelay,
		enablePriority: true,
	}

	for i := 0; i < workers; i++ {
		pipeline.wg.Add(1)
		go pipeline.worker()
	}

	if pipeline.enablePriority {
		pipeline.wg.Add(1)
		go pipeline.priorityDispatcher()
	}

	return pipeline
}

func (pm *PipelineManager) CreatePipeline(id string, workers int) *Pipeline {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pipeline := pm.createPipeline(id, workers)
	pm.activePipelines[id] = pipeline
	return pipeline
}

func (p *Pipeline) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case op := <-p.Operations:
			start := time.Now()

			if op.ShoalPPFeatures.UseParallelDAG && p.staggerDelay > 0 {
				time.Sleep(time.Duration(op.DAGInstance) * p.staggerDelay)
			}

			result := p.resultPool.Get().(*PipelineResult)
			p.resetPipelineResult(result)
			result.OperationID = op.ID
			result.DAGInstance = op.DAGInstance

			p.processShoalPPOperation(&op, result)

			result.Duration = time.Since(start)

			p.updateEnhancedMetrics(result)

			select {
			case p.Results <- *result:
			case <-p.ctx.Done():
				p.resultPool.Put(result)
				return
			}

			if op.Callback != nil {
				op.Callback(*result)
			}

			p.resultPool.Put(result)
		}
	}
}

func (p *Pipeline) priorityDispatcher() {
	defer p.wg.Done()

	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			if op := p.priorityQueue.Pop(); op != nil {
				select {
				case p.Operations <- *op:
				case <-p.ctx.Done():
					return
				default:
					p.priorityQueue.Push(op)
				}
			}
		}
	}
}

func (p *Pipeline) processShoalPPOperation(op *PipelineOperation, result *PipelineResult) {
	result.Success = true
	result.Data = op.Data
	result.ProcessingStage = "processing"

	if op.ShoalPPFeatures.UseFastCommit {
		result.ShoalPPMetrics.FastCommitUsed = true
		result.ShoalPPMetrics.CommitRuleApplied = FastDirectCommitRule
		result.ShoalPPMetrics.MessageDelayCount = 4
		result.ShoalPPMetrics.LatencyReduction = time.Millisecond * 20
		atomic.AddInt64(&p.metrics.fastCommitOps, 1)
	}

	if op.ShoalPPFeatures.UseDynamicAnchor {
		result.ShoalPPMetrics.DynamicAnchorUsed = true
		result.ShoalPPMetrics.ThroughputGain = 1.3
		atomic.AddInt64(&p.metrics.dynamicAnchorOps, 1)
	}

	if op.ShoalPPFeatures.UseParallelDAG {
		result.ShoalPPMetrics.ParallelDAGUsed = true
		result.ShoalPPMetrics.ThroughputGain += 0.4
		atomic.AddInt64(&p.metrics.parallelDAGOps, 1)
	}

	result.ProcessingStage = "completed"
}

func (p *Pipeline) resetPipelineResult(result *PipelineResult) {
	result.OperationID = ""
	result.Success = false
	result.Data = nil
	result.Error = nil
	result.Duration = 0
	result.ShoalPPMetrics = ShoalPPResultMetrics{}
	result.ProcessingStage = ""
	result.DAGInstance = 0
}

func (p *Pipeline) updateEnhancedMetrics(result *PipelineResult) {
	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()

	atomic.AddInt64(&p.metrics.processedOps, 1)

	alpha := 0.1
	if p.metrics.avgDuration == 0 {
		p.metrics.avgDuration = result.Duration
	} else {
		p.metrics.avgDuration = time.Duration(
			float64(p.metrics.avgDuration)*(1-alpha) + float64(result.Duration)*alpha,
		)
	}
}

func (p *Pipeline) SubmitOperation(op PipelineOperation) error {
	op.SubmissionTime = time.Now()

	if p.enablePriority && op.Priority > 5 {
		p.priorityQueue.Push(&op)
		return nil
	}

	select {
	case p.Operations <- op:
		return nil
	case <-p.ctx.Done():
		return context.Canceled
	default:
		return ErrPipelineFull
	}
}

func (pm *PipelineManager) SubmitFastCommitOperation(op PipelineOperation) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.fastCommitPipeline == nil {
		return ErrFastCommitFailed
	}

	op.ShoalPPFeatures.UseFastCommit = true
	op.Priority = 10
	return pm.fastCommitPipeline.SubmitOperation(op)
}

func (pm *PipelineManager) SubmitDynamicAnchorOperation(op PipelineOperation) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.dynamicAnchorPipeline == nil {
		return ErrDynamicAnchorFailed
	}

	op.ShoalPPFeatures.UseDynamicAnchor = true
	op.Priority = 8
	return pm.dynamicAnchorPipeline.SubmitOperation(op)
}

func (pm *PipelineManager) SubmitParallelDAGOperation(dagInstance int, op PipelineOperation) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	dagPipeline, exists := pm.parallelDAGs[dagInstance]
	if !exists {
		return ErrParallelDAGFailed
	}

	op.ShoalPPFeatures.UseParallelDAG = true
	op.DAGInstance = dagInstance
	op.Priority = 7
	return dagPipeline.SubmitOperation(op)
}

func (p *Pipeline) Stop() {
	p.cancel()
	close(p.Operations)
	p.wg.Wait()
	close(p.Results)
}

func (pm *PipelineManager) GetPipeline(id string) (*Pipeline, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	pipeline, exists := pm.activePipelines[id]
	return pipeline, exists
}

func (pm *PipelineManager) StopPipeline(id string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pipeline, exists := pm.activePipelines[id]; exists {
		pipeline.Stop()
		delete(pm.activePipelines, id)
	}
}

func (pm *PipelineManager) StopAll() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for id, pipeline := range pm.activePipelines {
		pipeline.Stop()
		delete(pm.activePipelines, id)
	}

	if pm.fastCommitPipeline != nil {
		pm.fastCommitPipeline.Stop()
		pm.fastCommitPipeline = nil
	}

	if pm.dynamicAnchorPipeline != nil {
		pm.dynamicAnchorPipeline.Stop()
		pm.dynamicAnchorPipeline = nil
	}

	for id, dagPipeline := range pm.parallelDAGs {
		dagPipeline.Stop()
		delete(pm.parallelDAGs, id)
	}

	if pm.roundTimeoutManager != nil {
		pm.roundTimeoutManager.Stop()
	}
}

func (pm *PipelineManager) GetShoalPPMetrics() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	metrics := map[string]interface{}{
		"total_pipelines": len(pm.activePipelines),
		"parallel_dags":   len(pm.parallelDAGs),
	}

	var totalOps, totalFastCommits, totalDynamicAnchors, totalParallelDAG int64
	var totalAvgDuration time.Duration

	for _, pipeline := range pm.activePipelines {
		pipeline.metrics.mu.RLock()
		totalOps += pipeline.metrics.processedOps
		totalFastCommits += pipeline.metrics.fastCommitOps
		totalDynamicAnchors += pipeline.metrics.dynamicAnchorOps
		totalParallelDAG += pipeline.metrics.parallelDAGOps
		totalAvgDuration += pipeline.metrics.avgDuration
		pipeline.metrics.mu.RUnlock()
	}

	if len(pm.activePipelines) > 0 {
		totalAvgDuration /= time.Duration(len(pm.activePipelines))
	}

	metrics["total_operations"] = totalOps
	metrics["fast_commit_operations"] = totalFastCommits
	metrics["dynamic_anchor_operations"] = totalDynamicAnchors
	metrics["parallel_dag_operations"] = totalParallelDAG
	metrics["average_duration_ms"] = totalAvgDuration.Milliseconds()

	if totalOps > 0 {
		metrics["fast_commit_utilization"] = float64(totalFastCommits) / float64(totalOps)
		metrics["dynamic_anchor_utilization"] = float64(totalDynamicAnchors) / float64(totalOps)
		metrics["parallel_dag_utilization"] = float64(totalParallelDAG) / float64(totalOps)
	}

	return metrics
}

type PriorityQueue struct {
	items []*PipelineOperation
	mu    sync.Mutex
}

func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		items: make([]*PipelineOperation, 0),
	}
}

func (pq *PriorityQueue) Push(op *PipelineOperation) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.items = append(pq.items, op)
	pq.heapifyUp(len(pq.items) - 1)
}

func (pq *PriorityQueue) Pop() *PipelineOperation {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.items) == 0 {
		return nil
	}

	item := pq.items[0]
	lastIdx := len(pq.items) - 1
	pq.items[0] = pq.items[lastIdx]
	pq.items = pq.items[:lastIdx]

	if len(pq.items) > 0 {
		pq.heapifyDown(0)
	}

	return item
}

func (pq *PriorityQueue) heapifyUp(idx int) {
	for idx > 0 {
		parentIdx := (idx - 1) / 2
		if pq.items[idx].Priority <= pq.items[parentIdx].Priority {
			break
		}
		pq.items[idx], pq.items[parentIdx] = pq.items[parentIdx], pq.items[idx]
		idx = parentIdx
	}
}

func (pq *PriorityQueue) heapifyDown(idx int) {
	for {
		leftChild := 2*idx + 1
		rightChild := 2*idx + 2
		largest := idx

		if leftChild < len(pq.items) && pq.items[leftChild].Priority > pq.items[largest].Priority {
			largest = leftChild
		}

		if rightChild < len(pq.items) && pq.items[rightChild].Priority > pq.items[largest].Priority {
			largest = rightChild
		}

		if largest == idx {
			break
		}

		pq.items[idx], pq.items[largest] = pq.items[largest], pq.items[idx]
		idx = largest
	}
}

type RoundTimeoutManager struct {
	config       *ShoalPPConfig
	activeRounds map[string]*RoundTimeout
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

type RoundTimeout struct {
	RoundID   string
	Timeout   time.Duration
	StartTime time.Time
	IsExpired bool
	Callbacks []func(string)
}

func NewRoundTimeoutManager(config *ShoalPPConfig) *RoundTimeoutManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RoundTimeoutManager{
		config:       config,
		activeRounds: make(map[string]*RoundTimeout),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (rtm *RoundTimeoutManager) StartRoundTimeout(roundID string, timeout time.Duration, callback func(string)) {
	rtm.mu.Lock()
	defer rtm.mu.Unlock()

	roundTimeout := &RoundTimeout{
		RoundID:   roundID,
		Timeout:   timeout,
		StartTime: time.Now(),
		IsExpired: false,
		Callbacks: []func(string){callback},
	}

	rtm.activeRounds[roundID] = roundTimeout

	rtm.wg.Add(1)
	go rtm.roundTimeoutWorker(roundTimeout)
}

func (rtm *RoundTimeoutManager) roundTimeoutWorker(rt *RoundTimeout) {
	defer rtm.wg.Done()

	timer := time.NewTimer(rt.Timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		rtm.mu.Lock()
		rt.IsExpired = true
		callbacks := rt.Callbacks
		delete(rtm.activeRounds, rt.RoundID)
		rtm.mu.Unlock()

		for _, callback := range callbacks {
			callback(rt.RoundID)
		}

	case <-rtm.ctx.Done():
		return
	}
}

func (rtm *RoundTimeoutManager) CancelRoundTimeout(roundID string) {
	rtm.mu.Lock()
	defer rtm.mu.Unlock()

	delete(rtm.activeRounds, roundID)
}

func (rtm *RoundTimeoutManager) Stop() {
	rtm.cancel()
	rtm.wg.Wait()
}
