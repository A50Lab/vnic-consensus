package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/p2p"
	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

// Narwhal represents the complete Narwhal consensus engine
type Narwhal struct {
	nodeID     types.NodeID
	privateKey crypto.PrivKey
	publicKey  crypto.PubKey
	logger     *zap.Logger

	// Core components
	network *p2p.Network
	mempool *DAGMempool
	primary *Primary
	workers []*Worker

	// Configuration
	config *types.NodeConfig

	// State
	isRunning bool
	mutex     sync.RWMutex

	// Callbacks
	onTransactionReceived func(*types.Transaction)
	onCertificateCreated  func(*types.Certificate)
	onBatchCreated        func(*types.Batch)
}

// NewNarwhal creates a new Narwhal consensus engine
func NewNarwhal(config *types.NodeConfig, logger *zap.Logger) (*Narwhal, error) {
	// Generate keys if not provided
	if config.PrivateKey == nil {
		privKey := ed25519.GenPrivKey()
		config.PrivateKey = privKey
		config.PublicKey = privKey.PubKey()
	}

	// Create P2P network
	network, err := p2p.NewNetwork(config.ListenAddr, config.NodeID, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create P2P network: %w", err)
	}

	// Create DAG mempool
	dagMempool := NewDAGMempool(logger)

	// Create primary node
	primaryNode := NewPrimary(
		config.NodeID,
		config.PrivateKey,
		network,
		dagMempool,
		logger,
	)

	// Create workers
	workers := make([]*Worker, len(config.Workers))
	for i, workerInfo := range config.Workers {
		workerNode := NewWorker(
			workerInfo.ID,
			config.NodeID, // Primary ID
			network,
			dagMempool,
			logger,
		)
		workers[i] = workerNode
		primaryNode.AddWorker(workerNode)
	}

	narwhal := &Narwhal{
		nodeID:     config.NodeID,
		privateKey: config.PrivateKey,
		publicKey:  config.PublicKey,
		logger:     logger,
		network:    network,
		mempool:    dagMempool,
		primary:    primaryNode,
		workers:    workers,
		config:     config,
		isRunning:  false,
	}

	// Set up callbacks
	narwhal.setupCallbacks()

	return narwhal, nil
}

// Start starts the Narwhal consensus engine
func (n *Narwhal) Start() error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.isRunning {
		return fmt.Errorf("Narwhal is already running")
	}

	n.logger.Info("Starting Narwhal consensus engine",
		zap.String("node_id", string(n.nodeID)),
		zap.Int("worker_count", len(n.workers)),
	)

	// Start P2P network
	if err := n.network.Start(); err != nil {
		return fmt.Errorf("failed to start P2P network: %w", err)
	}

	// Start primary node
	if err := n.primary.Start(); err != nil {
		return fmt.Errorf("failed to start primary node: %w", err)
	}

	// Start worker nodes
	for i, worker := range n.workers {
		if err := worker.Start(); err != nil {
			return fmt.Errorf("failed to start worker %d: %w", i, err)
		}
	}

	// Update committee information
	committee := make(map[types.NodeID]*types.PrimaryInfo)
	for _, primaryInfo := range n.config.Committee {
		committee[primaryInfo.ID] = &primaryInfo
	}
	n.primary.UpdateCommittee(committee)

	n.isRunning = true
	n.logger.Info("Narwhal consensus engine started successfully")

	return nil
}

// Stop stops the Narwhal consensus engine
func (n *Narwhal) Stop() error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if !n.isRunning {
		return nil
	}

	n.logger.Info("Stopping Narwhal consensus engine")

	// Stop workers
	for i, worker := range n.workers {
		if err := worker.Stop(); err != nil {
			n.logger.Error("Failed to stop worker",
				zap.Int("worker_index", i),
				zap.Error(err),
			)
		}
	}

	// Stop primary
	if err := n.primary.Stop(); err != nil {
		n.logger.Error("Failed to stop primary", zap.Error(err))
	}

	// Stop network
	if err := n.network.Stop(); err != nil {
		n.logger.Error("Failed to stop network", zap.Error(err))
	}

	n.isRunning = false
	n.logger.Info("Narwhal consensus engine stopped")

	return nil
}

// SubmitTransaction submits a transaction to Narwhal for processing
func (n *Narwhal) SubmitTransaction(data []byte) (*types.Transaction, error) {
	if !n.isRunning {
		return nil, fmt.Errorf("Narwhal is not running")
	}

	// Create transaction
	tx := types.NewTransaction(data)

	n.logger.Debug("Submitting transaction to Narwhal",
		zap.String("tx_hash", tx.Hash.String()),
		zap.Int("data_size", len(data)),
		zap.String("data_hex", fmt.Sprintf("%X", data)),
	)

	// Add to mempool
	if err := n.mempool.AddTransaction(tx); err != nil {
		return nil, fmt.Errorf("failed to add transaction to mempool: %w", err)
	}

	// Call callback if set
	if n.onTransactionReceived != nil {
		n.onTransactionReceived(tx)
	}

	return tx, nil
}

// GetCertificate retrieves a certificate by hash
func (n *Narwhal) GetCertificate(hash types.Hash) (*types.Certificate, bool) {
	return n.mempool.GetCertificate(hash)
}

// GetBatch retrieves a batch by hash
func (n *Narwhal) GetBatch(hash types.Hash) (*types.Batch, bool) {
	return n.mempool.GetBatch(hash)
}

// GetCurrentRound returns the current consensus round
func (n *Narwhal) GetCurrentRound() types.Round {
	return n.primary.GetCurrentRound()
}

// AdvanceRound manually advances to the next round (for testing)
func (n *Narwhal) AdvanceRound() {
	n.primary.AdvanceRound()
}

// GetStats returns comprehensive statistics about Narwhal
func (n *Narwhal) GetStats() map[string]interface{} {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	stats := map[string]interface{}{
		"node_id":     string(n.nodeID),
		"is_running":  n.isRunning,
		"listen_addr": n.config.ListenAddr,
	}

	// Add mempool stats
	mempoolStats := n.mempool.GetStats()
	stats["mempool"] = mempoolStats

	// Add primary stats
	primaryStats := n.primary.GetStats()
	stats["primary"] = primaryStats

	// Add worker stats
	workerStats := make([]map[string]interface{}, len(n.workers))
	for i, worker := range n.workers {
		workerStats[i] = worker.GetStats()
	}
	stats["workers"] = workerStats

	// Add network stats
	connectedPeers := n.network.GetConnectedPeers()
	stats["network"] = map[string]interface{}{
		"peer_id":         n.network.GetPeerID().String(),
		"connected_peers": len(connectedPeers),
	}

	return stats
}

// SetCallbacks sets callback functions for various events
func (n *Narwhal) SetCallbacks(
	onTransactionReceived func(*types.Transaction),
	onCertificateCreated func(*types.Certificate),
	onBatchCreated func(*types.Batch),
) {
	n.onTransactionReceived = onTransactionReceived
	n.onCertificateCreated = onCertificateCreated
	n.onBatchCreated = onBatchCreated
}

// setupCallbacks sets up internal callbacks between components
func (n *Narwhal) setupCallbacks() {
	// Set mempool callbacks
	n.mempool.SetCallbacks(
		func(cert *types.Certificate) {
			n.logger.Debug("Certificate added to mempool",
				zap.String("cert_hash", cert.Hash.String()),
			)
			if n.onCertificateCreated != nil {
				n.onCertificateCreated(cert)
			}
		},
		func(batch *types.Batch) {
			n.logger.Debug("Batch added to mempool",
				zap.String("batch_id", batch.ID.String()),
			)
			if n.onBatchCreated != nil {
				n.onBatchCreated(batch)
			}
		},
	)

	// Set primary callbacks
	n.primary.SetCallbacks(
		func(cert *types.Certificate) {
			n.logger.Debug("Certificate created by primary",
				zap.String("cert_hash", cert.Hash.String()),
			)
		},
		func(round types.Round) {
			n.logger.Info("Round advanced",
				zap.Uint64("new_round", uint64(round)),
			)
		},
	)

	// Set worker callbacks
	for i, worker := range n.workers {
		worker.SetOnBatchCreated(func(batch *types.Batch) {
			n.logger.Debug("Batch created by worker",
				zap.String("batch_id", batch.ID.String()),
				zap.Int("worker_index", i),
			)
		})
	}
}

// GetNodeID returns the node ID
func (n *Narwhal) GetNodeID() types.NodeID {
	return n.nodeID
}

// GetNetwork returns the P2P network instance
func (n *Narwhal) GetNetwork() *p2p.Network {
	return n.network
}

// GetMempool returns the DAG mempool instance
func (n *Narwhal) GetMempool() *DAGMempool {
	return n.mempool
}

// GetPrimary returns the primary node instance
func (n *Narwhal) GetPrimary() *Primary {
	return n.primary
}

// IsRunning returns whether Narwhal is currently running
func (n *Narwhal) IsRunning() bool {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.isRunning
}

// WaitForCertificate waits for a certificate to be created (for testing)
func (n *Narwhal) WaitForCertificate(timeout time.Duration) (*types.Certificate, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		currentRound := n.GetCurrentRound()
		certs := n.mempool.GetCertificatesByRound(currentRound)
		if len(certs) > 0 {
			return certs[0], nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("timeout waiting for certificate")
}

// ForceBatchCreation forces all workers to create batches with current transactions
func (n *Narwhal) ForceBatchCreation() error {
	var errors []error

	for i, worker := range n.workers {
		if err := worker.ForceBatch(); err != nil {
			n.logger.Error("Failed to force batch creation",
				zap.Int("worker_index", i),
				zap.Error(err),
			)
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to force batch creation on %d workers", len(errors))
	}

	return nil
}
