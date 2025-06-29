package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/p2p"
	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

// Worker handles transaction batching and batch creation in Narwhal
type Worker struct {
	nodeID    types.NodeID
	primaryID types.NodeID
	logger    *zap.Logger
	network   *p2p.Network
	mempool   *DAGMempool

	// Configuration
	batchSize    int
	batchTimeout time.Duration
	maxBatches   int

	// State
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Batching
	currentBatch    []*types.Transaction
	batchMutex      sync.Mutex
	batchTimer      *time.Timer
	lastBatchTime   time.Time

	// Callbacks
	onBatchCreated func(*types.Batch)
}

// NewWorker creates a new worker node
func NewWorker(
	nodeID types.NodeID,
	primaryID types.NodeID,
	network *p2p.Network,
	mempool *DAGMempool,
	logger *zap.Logger,
) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		nodeID:       nodeID,
		primaryID:    primaryID,
		logger:       logger,
		network:      network,
		mempool:      mempool,
		ctx:          ctx,
		cancel:       cancel,
		batchSize:    100,
		batchTimeout: 5 * time.Second,
		maxBatches:   1000,
		currentBatch: make([]*types.Transaction, 0),
	}
}

// SetConfig sets the worker configuration
func (w *Worker) SetConfig(batchSize int, batchTimeout time.Duration, maxBatches int) {
	w.batchSize = batchSize
	w.batchTimeout = batchTimeout
	w.maxBatches = maxBatches
}

// SetOnBatchCreated sets the callback for when a batch is created
func (w *Worker) SetOnBatchCreated(callback func(*types.Batch)) {
	w.onBatchCreated = callback
}

// Start starts the worker node
func (w *Worker) Start() error {
	w.logger.Info("Starting worker node",
		zap.String("worker_id", string(w.nodeID)),
		zap.String("primary_id", string(w.primaryID)),
	)

	// Set up network handlers
	w.network.SetHandlers(
		w.handleBatch,
		nil, // Workers don't handle headers directly
		nil, // Workers don't handle certificates directly
		nil, // Workers don't handle votes directly
	)

	// Start the batching process
	w.wg.Add(1)
	go w.batchingLoop()

	// Start transaction processing
	w.wg.Add(1)
	go w.transactionProcessingLoop()

	w.logger.Info("Worker node started successfully")
	return nil
}

// Stop stops the worker node
func (w *Worker) Stop() error {
	w.logger.Info("Stopping worker node")

	w.cancel()
	w.wg.Wait()

	if w.batchTimer != nil {
		w.batchTimer.Stop()
	}

	w.logger.Info("Worker node stopped")
	return nil
}

// AddTransaction adds a transaction to the worker for batching
func (w *Worker) AddTransaction(tx *types.Transaction) error {
	w.batchMutex.Lock()
	defer w.batchMutex.Unlock()

	// Add transaction to current batch
	w.currentBatch = append(w.currentBatch, tx)

	// Set last batch time if this is the first transaction
	if len(w.currentBatch) == 1 {
		w.lastBatchTime = time.Now()
	}

	w.logger.Debug("Added transaction to worker batch",
		zap.String("tx_hash", tx.Hash.String()),
		zap.Int("batch_size", len(w.currentBatch)),
	)

	// Check if batch is full
	if len(w.currentBatch) >= w.batchSize {
		w.logger.Debug("Batch is full, creating batch")
		return w.createBatchLocked()
	}

	// Start or reset the batch timer
	w.resetBatchTimer()

	return nil
}

// GetPendingTransactionCount returns the number of pending transactions
func (w *Worker) GetPendingTransactionCount() int {
	w.batchMutex.Lock()
	defer w.batchMutex.Unlock()
	return len(w.currentBatch)
}

// ForceBatch forces the creation of a batch with current transactions
func (w *Worker) ForceBatch() error {
	w.batchMutex.Lock()
	defer w.batchMutex.Unlock()

	if len(w.currentBatch) == 0 {
		return fmt.Errorf("no transactions to batch")
	}

	return w.createBatchLocked()
}

// batchingLoop handles the automatic batching based on timeout
func (w *Worker) batchingLoop() {
	defer w.wg.Done()

	w.logger.Debug("Starting batching loop")

	for {
		select {
		case <-w.ctx.Done():
			w.logger.Debug("Batching loop stopped")
			return
		default:
			// Check if we have pending transactions that need to be batched
			w.batchMutex.Lock()
			if len(w.currentBatch) > 0 && !w.lastBatchTime.IsZero() {
				// Check if timeout has elapsed since last batch was started
				if time.Since(w.lastBatchTime) > w.batchTimeout {
					w.logger.Debug("Batch timeout elapsed, creating batch",
						zap.Duration("elapsed", time.Since(w.lastBatchTime)),
						zap.Duration("timeout", w.batchTimeout),
					)
					w.createBatchLocked()
				}
			}
			w.batchMutex.Unlock()

			time.Sleep(100 * time.Millisecond) // Small sleep to prevent busy waiting
		}
	}
}

// transactionProcessingLoop processes transactions from the mempool
func (w *Worker) transactionProcessingLoop() {
	defer w.wg.Done()

	w.logger.Debug("Starting transaction processing loop")

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			w.logger.Debug("Transaction processing loop stopped")
			return
		case <-ticker.C:
			// Get pending transactions from mempool
			txs := w.mempool.GetPendingTransactions(w.batchSize)
			if len(txs) > 0 {
				w.logger.Debug("Processing transactions from mempool",
					zap.Int("tx_count", len(txs)),
				)

				for _, tx := range txs {
					if err := w.AddTransaction(tx); err != nil {
						w.logger.Error("Failed to add transaction to batch",
							zap.String("tx_hash", tx.Hash.String()),
							zap.Error(err),
						)
					}
				}
			}
		}
	}
}

// createBatchLocked creates a batch from current transactions (must be called with lock held)
func (w *Worker) createBatchLocked() error {
	if len(w.currentBatch) == 0 {
		return nil
	}

	// Create the batch
	batch := types.NewBatch(w.currentBatch, w.nodeID)

	w.logger.Info("Created new batch",
		zap.String("batch_id", batch.ID.String()),
		zap.Int("tx_count", len(batch.Transactions)),
		zap.String("author", string(batch.Author)),
	)

	// Add batch to mempool
	if err := w.mempool.AddBatch(batch); err != nil {
		w.logger.Error("Failed to add batch to mempool",
			zap.String("batch_id", batch.ID.String()),
			zap.Error(err),
		)
		return err
	}

	// Broadcast batch to network
	if err := w.network.BroadcastBatch(batch); err != nil {
		w.logger.Error("Failed to broadcast batch",
			zap.String("batch_id", batch.ID.String()),
			zap.Error(err),
		)
		// Don't return error here, batch is still valid locally
	}

	// Call callback if set
	if w.onBatchCreated != nil {
		w.onBatchCreated(batch)
	}

	// Clear current batch
	w.currentBatch = make([]*types.Transaction, 0)
	w.lastBatchTime = time.Time{} // Reset batch time

	// Stop the batch timer
	if w.batchTimer != nil {
		w.batchTimer.Stop()
		w.batchTimer = nil
	}

	return nil
}

// resetBatchTimer resets the batch timeout timer with race protection
func (w *Worker) resetBatchTimer() {
	// Stop existing timer if any
	if w.batchTimer != nil {
		if !w.batchTimer.Stop() {
			// Timer may have already fired, drain the channel if needed
			select {
			case <-w.batchTimer.C:
			default:
			}
		}
	}

	w.batchTimer = time.AfterFunc(w.batchTimeout, func() {
		// Use a timeout context to prevent hanging
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		select {
		case <-ctx.Done():
			w.logger.Warn("Batch timer callback timed out")
			return
		default:
		}
		
		w.batchMutex.Lock()
		defer w.batchMutex.Unlock()

		if len(w.currentBatch) > 0 {
			w.logger.Debug("Batch timer expired, creating batch",
				zap.Int("batch_size", len(w.currentBatch)),
			)
			if err := w.createBatchLocked(); err != nil {
				w.logger.Error("Failed to create batch on timer expiry", zap.Error(err))
			}
		}
	})
}

// handleBatch handles incoming batch messages from the network
func (w *Worker) handleBatch(batch *types.Batch) {
	w.logger.Debug("Received batch from network",
		zap.String("batch_id", batch.ID.String()),
		zap.String("author", string(batch.Author)),
		zap.Int("tx_count", len(batch.Transactions)),
	)

	// Add batch to mempool if not already present
	if err := w.mempool.AddBatch(batch); err != nil {
		w.logger.Debug("Batch already exists in mempool or failed to add",
			zap.String("batch_id", batch.ID.String()),
			zap.Error(err),
		)
		return
	}

	w.logger.Debug("Added received batch to mempool",
		zap.String("batch_id", batch.ID.String()),
	)
}

// GetStats returns worker statistics
func (w *Worker) GetStats() map[string]interface{} {
	w.batchMutex.Lock()
	defer w.batchMutex.Unlock()

	return map[string]interface{}{
		"node_id":          string(w.nodeID),
		"primary_id":       string(w.primaryID),
		"pending_txs":      len(w.currentBatch),
		"batch_size":       w.batchSize,
		"batch_timeout_ms": w.batchTimeout.Milliseconds(),
		"max_batches":      w.maxBatches,
	}
}
