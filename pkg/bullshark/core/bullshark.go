package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/abci"
	bullsharktypes "github.com/vietchain/vniccss/pkg/bullshark/types"
	narwhal "github.com/vietchain/vniccss/pkg/narwhal/core"
	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

// Bullshark represents the complete Bullshark consensus engine
type Bullshark struct {
	logger     *zap.Logger
	mempool    *narwhal.DAGMempool
	abciClient *abci.Client

	// Core components
	anchorSelector *Selector
	orderingEngine *Engine
	blockCreator   *Creator

	// State management
	consensusState *bullsharktypes.ConsensusState
	stateMutex     sync.RWMutex

	// Configuration
	blockInterval   time.Duration
	maxBlockSize    int
	enableAutoBlock bool

	// Control
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	isRunning    bool
	runningMutex sync.RWMutex

	// Callbacks
	onBlockCreated  func(*bullsharktypes.BullsharkBlock)
	onBlockExecuted func(*ExecutionResult)
}

// NewBullshark creates a new Bullshark consensus engine
func NewBullshark(
	mempool *narwhal.DAGMempool,
	abciClient *abci.Client,
	logger *zap.Logger,
) *Bullshark {
	ctx, cancel := context.WithCancel(context.Background())

	// Create consensus state
	consensusState := bullsharktypes.NewConsensusState()

	// Create core components
	anchorSelector := NewSelector(
		mempool,
		bullsharktypes.DefaultAnchorSelectionStrategy(),
		logger,
	)

	orderingEngine := NewEngine(
		mempool,
		bullsharktypes.DefaultOrderingStrategy(),
		logger,
	)

	blockCreator := NewCreator(abciClient, logger)

	return &Bullshark{
		logger:          logger,
		mempool:         mempool,
		abciClient:      abciClient,
		anchorSelector:  anchorSelector,
		orderingEngine:  orderingEngine,
		blockCreator:    blockCreator,
		consensusState:  consensusState,
		blockInterval:   300 * time.Millisecond,
		maxBlockSize:    1000,
		enableAutoBlock: true,
		ctx:             ctx,
		cancel:          cancel,
		isRunning:       false,
	}
}

// Start starts the Bullshark consensus engine
func (b *Bullshark) Start() error {
	b.runningMutex.Lock()
	defer b.runningMutex.Unlock()

	if b.isRunning {
		return fmt.Errorf("Bullshark is already running")
	}

	b.logger.Info("Starting Bullshark consensus engine",
		zap.Duration("block_interval", b.blockInterval),
		zap.Int("max_block_size", b.maxBlockSize),
		zap.Bool("auto_block", b.enableAutoBlock),
	)

	if b.enableAutoBlock {
		// Start automatic block production
		b.wg.Add(1)
		go b.blockProductionLoop()
	}

	// Start consensus monitoring
	b.wg.Add(1)
	go b.consensusMonitoringLoop()

	b.isRunning = true
	b.logger.Info("Bullshark consensus engine started successfully")

	return nil
}

// Stop stops the Bullshark consensus engine
func (b *Bullshark) Stop() error {
	b.runningMutex.Lock()
	defer b.runningMutex.Unlock()

	if !b.isRunning {
		return nil
	}

	b.logger.Info("Stopping Bullshark consensus engine")

	b.cancel()
	b.wg.Wait()

	b.isRunning = false
	b.logger.Info("Bullshark consensus engine stopped")

	return nil
}

// CreateBlock manually creates a block for the current state
func (b *Bullshark) CreateBlock() (*bullsharktypes.BullsharkBlock, error) {
	b.stateMutex.Lock()
	defer b.stateMutex.Unlock()

	currentRound := b.mempool.GetCurrentRound()

	b.logger.Debug("Creating block manually",
		zap.Uint64("round", uint64(currentRound)),
		zap.Int64("height", int64(b.consensusState.CurrentHeight)),
	)

	return b.createBlockForRound(currentRound)
}

// blockProductionLoop handles automatic block production
func (b *Bullshark) blockProductionLoop() {
	defer b.wg.Done()

	b.logger.Debug("Starting block production loop")
	ticker := time.NewTicker(b.blockInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			b.logger.Debug("Block production loop stopped")
			return
		case <-ticker.C:
			if err := b.tryCreateBlockAutomatically(); err != nil {
				b.logger.Error("Failed to create block automatically", zap.Error(err))
			}
		}
	}
}

// consensusMonitoringLoop monitors the consensus state
func (b *Bullshark) consensusMonitoringLoop() {
	defer b.wg.Done()

	b.logger.Debug("Starting consensus monitoring loop")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			b.logger.Debug("Consensus monitoring loop stopped")
			return
		case <-ticker.C:
			b.monitorConsensusState()
		}
	}
}

// tryCreateBlockAutomatically attempts to create a block automatically
func (b *Bullshark) tryCreateBlockAutomatically() error {
	b.stateMutex.Lock()
	defer b.stateMutex.Unlock()

	currentRound := b.mempool.GetCurrentRound()

	b.logger.Debug("Creating block automatically",
		zap.Uint64("round", uint64(currentRound)),
		zap.Int64("height", int64(b.consensusState.CurrentHeight)),
	)

	// Always create a block to ensure continuous block production
	block, err := b.createBlockForRound(currentRound)
	if err != nil {
		return fmt.Errorf("failed to create block for round %d: %w", currentRound, err)
	}

	if block != nil {
		blockType := "with transactions"
		if block.TransactionCount == 0 {
			blockType = "empty"
		}

		b.logger.Info("Created block automatically",
			zap.Int64("height", int64(block.Height)),
			zap.Uint64("round", uint64(currentRound)),
			zap.Int("transactions", block.TransactionCount),
			zap.String("type", blockType),
		)
	}

	return nil
}

// createBlockForRound creates a block for a specific round
func (b *Bullshark) createBlockForRound(round types.Round) (*bullsharktypes.BullsharkBlock, error) {
	// Select anchors for this round
	anchors, err := b.anchorSelector.SelectAnchorsForHeight(
		b.consensusState.CurrentHeight,
		round,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to select anchors: %w", err)
	}

	// If no anchors available, create empty block
	if len(anchors) == 0 {
		b.logger.Debug("No anchors available, creating empty block",
			zap.Uint64("round", uint64(round)),
		)

		executionResult, err := b.blockCreator.CreateEmptyBlock(
			b.consensusState.CurrentHeight,
			b.consensusState.LastBlockHash,
			round,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create empty block: %w", err)
		}

		// Update consensus state after successful block creation
		b.consensusState.UpdateState(executionResult.Block)

		// Update our state to match ABCI state
		abciState := b.abciClient.GetState()
		b.logger.Info("Empty block committed to Cosmos",
			zap.Int64("bullshark_height", int64(executionResult.Block.Height)),
			zap.Int64("abci_height", abciState.LastBlockHeight),
			zap.String("abci_app_hash", fmt.Sprintf("%X", abciState.AppHash)),
		)

		// Call callbacks
		if b.onBlockCreated != nil {
			b.onBlockCreated(executionResult.Block)
		}
		if b.onBlockExecuted != nil {
			b.onBlockExecuted(executionResult)
		}

		return executionResult.Block, nil
	}

	// Create ordering result
	orderingResult, err := b.orderingEngine.CreateOrderingResult(
		anchors,
		round,
		b.consensusState.CurrentHeight,
		b.consensusState.LastBlockHash,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create ordering result: %w", err)
	}

	// Create and execute block
	executionResult, err := b.blockCreator.CreateAndExecuteBlock(orderingResult)
	if err != nil {
		return nil, fmt.Errorf("failed to create and execute block: %w", err)
	}

	// Update consensus state
	b.consensusState.UpdateState(executionResult.Block)

	// Update our state to match ABCI state
	abciState := b.abciClient.GetState()
	b.logger.Info("Block with transactions committed to Cosmos",
		zap.Int64("bullshark_height", int64(executionResult.Block.Height)),
		zap.Int64("abci_height", abciState.LastBlockHeight),
		zap.Int("transactions", executionResult.Block.TransactionCount),
		zap.String("abci_app_hash", fmt.Sprintf("%X", abciState.AppHash)),
	)

	// Call callbacks
	if b.onBlockCreated != nil {
		b.onBlockCreated(executionResult.Block)
	}
	if b.onBlockExecuted != nil {
		b.onBlockExecuted(executionResult)
	}

	return executionResult.Block, nil
}

// monitorConsensusState monitors and logs consensus state
func (b *Bullshark) monitorConsensusState() {
	b.stateMutex.RLock()
	defer b.stateMutex.RUnlock()

	currentRound := b.mempool.GetCurrentRound()
	mempoolStats := b.mempool.GetStats()

	// Log state occasionally
	b.logger.Debug("Consensus state",
		zap.Int64("height", int64(b.consensusState.CurrentHeight)),
		zap.Uint64("round", uint64(currentRound)),
		zap.Int("certificates", mempoolStats["certificates"].(int)),
		zap.Int("batches", mempoolStats["batches"].(int)),
		zap.Int("pending_txs", mempoolStats["pending_txs"].(int)),
	)
}

// GetCurrentHeight returns the current block height
func (b *Bullshark) GetCurrentHeight() bullsharktypes.BlockHeight {
	b.stateMutex.RLock()
	defer b.stateMutex.RUnlock()
	return b.consensusState.CurrentHeight
}

// GetCurrentRound returns the current consensus round
func (b *Bullshark) GetCurrentRound() types.Round {
	return b.mempool.GetCurrentRound()
}

// GetBlock retrieves a block by height
func (b *Bullshark) GetBlock(height bullsharktypes.BlockHeight) (*bullsharktypes.BullsharkBlock, bool) {
	b.stateMutex.RLock()
	defer b.stateMutex.RUnlock()
	return b.consensusState.GetBlock(height)
}

// GetLastBlock returns the last committed block
func (b *Bullshark) GetLastBlock() *bullsharktypes.BullsharkBlock {
	b.stateMutex.RLock()
	defer b.stateMutex.RUnlock()
	return b.consensusState.LastBlock
}

// SetCallbacks sets callback functions for events
func (b *Bullshark) SetCallbacks(
	onBlockCreated func(*bullsharktypes.BullsharkBlock),
	onBlockExecuted func(*ExecutionResult),
) {
	b.onBlockCreated = onBlockCreated
	b.onBlockExecuted = onBlockExecuted
}

// UpdateConfig updates the Bullshark configuration
func (b *Bullshark) UpdateConfig(
	blockInterval time.Duration,
	maxBlockSize int,
	enableAutoBlock bool,
) {
	b.blockInterval = blockInterval
	b.maxBlockSize = maxBlockSize
	b.enableAutoBlock = enableAutoBlock

	b.logger.Info("Updated Bullshark configuration",
		zap.Duration("block_interval", blockInterval),
		zap.Int("max_block_size", maxBlockSize),
		zap.Bool("auto_block", enableAutoBlock),
	)
}

// GetStats returns comprehensive statistics about Bullshark
func (b *Bullshark) GetStats() map[string]interface{} {
	b.stateMutex.RLock()
	defer b.stateMutex.RUnlock()

	b.runningMutex.RLock()
	isRunning := b.isRunning
	b.runningMutex.RUnlock()

	return map[string]interface{}{
		"is_running":        isRunning,
		"current_height":    int64(b.consensusState.CurrentHeight),
		"current_round":     uint64(b.GetCurrentRound()),
		"last_block_hash":   b.consensusState.LastBlockHash.String(),
		"block_interval_ms": b.blockInterval.Milliseconds(),
		"max_block_size":    b.maxBlockSize,
		"enable_auto_block": b.enableAutoBlock,
		"anchor_selector":   b.anchorSelector.GetStats(),
		"ordering_engine":   b.orderingEngine.GetStats(),
		"block_creator":     b.blockCreator.GetStats(),
		"committed_blocks":  len(b.consensusState.CommittedBlocks),
	}
}

// IsRunning returns whether Bullshark is currently running
func (b *Bullshark) IsRunning() bool {
	b.runningMutex.RLock()
	defer b.runningMutex.RUnlock()
	return b.isRunning
}

// InitializeFromABCI initializes Bullshark consensus state from ABCI state
func (b *Bullshark) InitializeFromABCI(height int64, appHash []byte, lastBlockHash []byte) {
	b.stateMutex.Lock()
	defer b.stateMutex.Unlock()

	// Convert ABCI height to Bullshark height
	bullsharkHeight := bullsharktypes.BlockHeight(height)

	// Create hash objects
	var blockHash types.Hash
	if len(lastBlockHash) > 0 {
		blockHash = types.Hash(lastBlockHash)
	} else {
		blockHash = types.NewHash([]byte("genesis"))
	}

	// Update consensus state
	b.consensusState.CurrentHeight = bullsharkHeight + 1 // Next height to produce
	b.consensusState.LastBlockHash = blockHash

	b.logger.Info("Initialized Bullshark from ABCI state",
		zap.Int64("current_height", int64(b.consensusState.CurrentHeight)),
		zap.Int64("last_block_height", height),
		zap.String("app_hash", fmt.Sprintf("%X", appHash)),
		zap.String("last_block_hash", blockHash.String()),
	)
}

// GetEngine returns the ordering engine
func (b *Bullshark) GetEngine() *Engine {
	return b.orderingEngine
}

// GetSelector returns the anchor selector
func (b *Bullshark) GetSelector() *Selector {
	return b.anchorSelector
}
