package core

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

// DAGMempool implements a DAG-based mempool for storing certificates and batches
type DAGMempool struct {
	logger *zap.Logger
	mutex  sync.RWMutex

	// Storage for certificates and batches
	certificates map[types.Hash]*types.Certificate
	batches      map[types.Hash]*types.Batch
	headers      map[types.Hash]*types.Header

	// DAG structure tracking
	parents  map[types.Hash][]types.Hash // certificate -> parent certificates
	children map[types.Hash][]types.Hash // certificate -> child certificates

	// Round and epoch tracking
	currentRound types.Round
	currentEpoch uint64

	// Certificate tracking by round
	certificatesByRound  map[types.Round][]types.Hash
	certificatesByAuthor map[types.NodeID][]types.Hash

	// Pending transactions (not yet included in batches)
	pendingTxs []*types.Transaction
	txMutex    sync.Mutex

	// Configuration
	maxCertificatesPerRound int
	maxBatchSize            int
	batchTimeout            time.Duration

	// Callbacks
	onCertificateAdded func(*types.Certificate)
	onBatchAdded       func(*types.Batch)
}

// NewDAGMempool creates a new DAG-based mempool
func NewDAGMempool(logger *zap.Logger) *DAGMempool {
	return &DAGMempool{
		logger:                  logger,
		certificates:            make(map[types.Hash]*types.Certificate),
		batches:                 make(map[types.Hash]*types.Batch),
		headers:                 make(map[types.Hash]*types.Header),
		parents:                 make(map[types.Hash][]types.Hash),
		children:                make(map[types.Hash][]types.Hash),
		certificatesByRound:     make(map[types.Round][]types.Hash),
		certificatesByAuthor:    make(map[types.NodeID][]types.Hash),
		pendingTxs:              make([]*types.Transaction, 0),
		currentRound:            0,
		currentEpoch:            0,
		maxCertificatesPerRound: 1000,
		maxBatchSize:            100,
		batchTimeout:            time.Second * 5,
	}
}

// SetCallbacks sets the callback functions
func (m *DAGMempool) SetCallbacks(
	onCertificateAdded func(*types.Certificate),
	onBatchAdded func(*types.Batch),
) {
	m.onCertificateAdded = onCertificateAdded
	m.onBatchAdded = onBatchAdded
}

// AddTransaction adds a transaction to the pending pool
func (m *DAGMempool) AddTransaction(tx *types.Transaction) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	m.txMutex.Lock()
	defer m.txMutex.Unlock()

	// Check if transaction already exists
	for _, existingTx := range m.pendingTxs {
		if existingTx.Hash == tx.Hash {
			return fmt.Errorf("transaction already exists: %s", tx.Hash.String())
		}
	}

	m.pendingTxs = append(m.pendingTxs, tx)
	m.logger.Debug("Added transaction to pending pool",
		zap.String("tx_hash", tx.Hash.String()),
		zap.Int("pending_count", len(m.pendingTxs)),
	)

	return nil
}

// GetPendingTransactions returns pending transactions for batching
func (m *DAGMempool) GetPendingTransactions(maxCount int) []*types.Transaction {
	m.txMutex.Lock()
	defer m.txMutex.Unlock()

	if len(m.pendingTxs) == 0 {
		return nil
	}

	count := maxCount
	if count > len(m.pendingTxs) {
		count = len(m.pendingTxs)
	}

	txs := make([]*types.Transaction, count)
	copy(txs, m.pendingTxs[:count])

	// Remove the returned transactions from pending
	m.pendingTxs = m.pendingTxs[count:]

	return txs
}

// AddBatch adds a batch to the mempool with proper validation
func (m *DAGMempool) AddBatch(batch *types.Batch) error {
	if batch == nil {
		return fmt.Errorf("batch is nil")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if batch already exists
	if _, exists := m.batches[batch.ID]; exists {
		return fmt.Errorf("batch already exists: %s", batch.ID.String())
	}

	// Validate batch before adding
	if err := m.validateBatch(batch); err != nil {
		return fmt.Errorf("invalid batch: %w", err)
	}

	m.batches[batch.ID] = batch
	m.logger.Debug("Added batch to mempool",
		zap.String("batch_id", batch.ID.String()),
		zap.String("author", string(batch.Author)),
		zap.Int("tx_count", len(batch.Transactions)),
	)

	if m.onBatchAdded != nil {
		m.onBatchAdded(batch)
	}

	return nil
}

// AddCertificate adds a certificate to the DAG
func (m *DAGMempool) AddCertificate(cert *types.Certificate) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if certificate already exists
	if _, exists := m.certificates[cert.Hash]; exists {
		return fmt.Errorf("certificate already exists: %s", cert.Hash.String())
	}

	// Validate certificate
	if err := m.validateCertificate(cert); err != nil {
		return fmt.Errorf("invalid certificate: %w", err)
	}

	// Add certificate to storage
	m.certificates[cert.Hash] = cert
	m.headers[cert.Hash] = cert.Header

	// Update DAG structure
	m.updateDAGStructure(cert)

	// Update round tracking
	m.updateRoundTracking(cert)

	m.logger.Debug("Added certificate to DAG",
		zap.String("cert_hash", cert.Hash.String()),
		zap.String("author", string(cert.Header.Author)),
		zap.Uint64("round", uint64(cert.Header.Round)),
	)

	if m.onCertificateAdded != nil {
		m.onCertificateAdded(cert)
	}

	return nil
}

// GetCertificate retrieves a certificate by hash
func (m *DAGMempool) GetCertificate(hash types.Hash) (*types.Certificate, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	cert, exists := m.certificates[hash]
	return cert, exists
}

// GetBatch retrieves a batch by hash
func (m *DAGMempool) GetBatch(hash types.Hash) (*types.Batch, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	batch, exists := m.batches[hash]
	return batch, exists
}

// GetCertificatesByRound returns all certificates for a given round
func (m *DAGMempool) GetCertificatesByRound(round types.Round) []*types.Certificate {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	hashes, exists := m.certificatesByRound[round]
	if !exists {
		return nil
	}

	certificates := make([]*types.Certificate, 0, len(hashes))
	for _, hash := range hashes {
		if cert, exists := m.certificates[hash]; exists {
			certificates = append(certificates, cert)
		}
	}

	return certificates
}

// GetCertificatesByAuthor returns all certificates by a specific author
func (m *DAGMempool) GetCertificatesByAuthor(author types.NodeID) []*types.Certificate {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	hashes, exists := m.certificatesByAuthor[author]
	if !exists {
		return nil
	}

	certificates := make([]*types.Certificate, 0, len(hashes))
	for _, hash := range hashes {
		if cert, exists := m.certificates[hash]; exists {
			certificates = append(certificates, cert)
		}
	}

	return certificates
}

// GetParents returns the parent certificates of a given certificate
func (m *DAGMempool) GetParents(hash types.Hash) []*types.Certificate {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	parentHashes, exists := m.parents[hash]
	if !exists {
		return nil
	}

	parents := make([]*types.Certificate, 0, len(parentHashes))
	for _, parentHash := range parentHashes {
		if cert, exists := m.certificates[parentHash]; exists {
			parents = append(parents, cert)
		}
	}

	return parents
}

// GetChildren returns the child certificates of a given certificate
func (m *DAGMempool) GetChildren(hash types.Hash) []*types.Certificate {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	childHashes, exists := m.children[hash]
	if !exists {
		return nil
	}

	children := make([]*types.Certificate, 0, len(childHashes))
	for _, childHash := range childHashes {
		if cert, exists := m.certificates[childHash]; exists {
			children = append(children, cert)
		}
	}

	return children
}

// GetCurrentRound returns the current round
func (m *DAGMempool) GetCurrentRound() types.Round {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.currentRound
}

// GetCurrentEpoch returns the current epoch
func (m *DAGMempool) GetCurrentEpoch() uint64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.currentEpoch
}

// GetStats returns mempool statistics
func (m *DAGMempool) GetStats() map[string]interface{} {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	m.txMutex.Lock()
	pendingTxCount := len(m.pendingTxs)
	m.txMutex.Unlock()

	return map[string]interface{}{
		"certificates":  len(m.certificates),
		"batches":       len(m.batches),
		"pending_txs":   pendingTxCount,
		"current_round": uint64(m.currentRound),
		"current_epoch": m.currentEpoch,
	}
}

// validateCertificate validates a certificate before adding it to the DAG
func (m *DAGMempool) validateCertificate(cert *types.Certificate) error {
	// Check if header hash matches certificate hash
	if cert.Hash != cert.Header.Hash() {
		return fmt.Errorf("certificate hash mismatch")
	}

	// Check if all parent certificates exist
	for _, parentHash := range cert.Header.Parents {
		if _, exists := m.certificates[parentHash]; !exists {
			return fmt.Errorf("parent certificate not found: %s", parentHash.String())
		}
	}

	// Check if all payload batches exist
	for _, batchHash := range cert.Header.Payload {
		if _, exists := m.batches[batchHash]; !exists {
			return fmt.Errorf("payload batch not found: %s", batchHash.String())
		}
	}

	// Validate round progression
	if cert.Header.Round < m.currentRound {
		return fmt.Errorf("certificate round %d is less than current round %d",
			cert.Header.Round, m.currentRound)
	}

	return nil
}

// updateDAGStructure updates the parent-child relationships in the DAG
func (m *DAGMempool) updateDAGStructure(cert *types.Certificate) {
	certHash := cert.Hash

	// Add parent relationships
	m.parents[certHash] = make([]types.Hash, len(cert.Header.Parents))
	copy(m.parents[certHash], cert.Header.Parents)

	// Add child relationships
	for _, parentHash := range cert.Header.Parents {
		if m.children[parentHash] == nil {
			m.children[parentHash] = make([]types.Hash, 0)
		}
		m.children[parentHash] = append(m.children[parentHash], certHash)
	}
}

// updateRoundTracking updates round and author tracking
func (m *DAGMempool) updateRoundTracking(cert *types.Certificate) {
	round := cert.Header.Round
	author := cert.Header.Author
	certHash := cert.Hash

	// Update current round if necessary
	if round > m.currentRound {
		m.currentRound = round
	}

	// Update round tracking
	if m.certificatesByRound[round] == nil {
		m.certificatesByRound[round] = make([]types.Hash, 0)
	}
	m.certificatesByRound[round] = append(m.certificatesByRound[round], certHash)

	// Update author tracking
	if m.certificatesByAuthor[author] == nil {
		m.certificatesByAuthor[author] = make([]types.Hash, 0)
	}
	m.certificatesByAuthor[author] = append(m.certificatesByAuthor[author], certHash)
}

// validateBatch validates a batch before adding to mempool
func (m *DAGMempool) validateBatch(batch *types.Batch) error {
	if len(batch.Transactions) == 0 {
		return fmt.Errorf("batch has no transactions")
	}

	if len(batch.Transactions) > m.maxBatchSize {
		return fmt.Errorf("batch size %d exceeds maximum %d",
			len(batch.Transactions), m.maxBatchSize)
	}

	// Validate each transaction in batch
	for i, tx := range batch.Transactions {
		if tx == nil {
			return fmt.Errorf("transaction %d is nil", i)
		}
		if len(tx.Data) == 0 {
			return fmt.Errorf("transaction %d has no data", i)
		}
	}

	return nil
}

// RemoveProcessedBatches removes batches that have been processed in a block
func (m *DAGMempool) RemoveProcessedBatches(batchHashes []types.Hash) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	removedCount := 0
	for _, batchHash := range batchHashes {
		if _, exists := m.batches[batchHash]; exists {
			delete(m.batches, batchHash)
			removedCount++
			m.logger.Debug("Removed processed batch",
				zap.String("batch_hash", batchHash.String()),
			)
		}
	}

	if removedCount > 0 {
		m.logger.Info("Cleaned up processed batches",
			zap.Int("removed_count", removedCount),
		)
	}
}

// RemoveProcessedCertificates removes certificates that have been processed
func (m *DAGMempool) RemoveProcessedCertificates(certHashes []types.Hash) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	removedCount := 0
	for _, certHash := range certHashes {
		if cert, exists := m.certificates[certHash]; exists {
			// Remove from main storage
			delete(m.certificates, certHash)
			delete(m.headers, certHash)

			// Remove from DAG structure
			delete(m.parents, certHash)
			
			// Remove from children references (clean up parent->child relationships)
			for _, parentHash := range cert.Header.Parents {
				if children, exists := m.children[parentHash]; exists {
					// Remove this certificate from parent's children list
					for i, childHash := range children {
						if childHash == certHash {
							m.children[parentHash] = append(children[:i], children[i+1:]...)
							break
						}
					}
				}
			}
			delete(m.children, certHash)

			// Remove from round tracking
			round := cert.Header.Round
			if roundCerts, exists := m.certificatesByRound[round]; exists {
				for i, hash := range roundCerts {
					if hash == certHash {
						m.certificatesByRound[round] = append(roundCerts[:i], roundCerts[i+1:]...)
						break
					}
				}
				// Remove round entry if empty
				if len(m.certificatesByRound[round]) == 0 {
					delete(m.certificatesByRound, round)
				}
			}

			// Remove from author tracking
			author := cert.Header.Author
			if authorCerts, exists := m.certificatesByAuthor[author]; exists {
				for i, hash := range authorCerts {
					if hash == certHash {
						m.certificatesByAuthor[author] = append(authorCerts[:i], authorCerts[i+1:]...)
						break
					}
				}
				// Remove author entry if empty
				if len(m.certificatesByAuthor[author]) == 0 {
					delete(m.certificatesByAuthor, author)
				}
			}

			removedCount++
			m.logger.Debug("Removed processed certificate",
				zap.String("cert_hash", certHash.String()),
				zap.Uint64("round", uint64(cert.Header.Round)),
			)
		}
	}

	if removedCount > 0 {
		m.logger.Info("Cleaned up processed certificates",
			zap.Int("removed_count", removedCount),
		)
	}
}

// Clear removes old data based on some criteria (for memory management)
func (m *DAGMempool) Clear(beforeRound types.Round) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	removedCount := 0
	// Remove certificates and related data for rounds before the specified round
	for round := types.Round(0); round < beforeRound; round++ {
		if hashes, exists := m.certificatesByRound[round]; exists {
			for _, hash := range hashes {
				// Remove from certificates
				delete(m.certificates, hash)
				delete(m.headers, hash)

				// Remove from DAG structure
				delete(m.parents, hash)
				delete(m.children, hash)
				removedCount++

				// Remove from author tracking
				// (This is simplified - in a full implementation, we'd need to remove specific entries)
			}
			delete(m.certificatesByRound, round)
		}
	}

	m.logger.Info("Cleared old certificates",
		zap.Uint64("before_round", uint64(beforeRound)),
		zap.Int("removed_count", removedCount),
	)
}
