package types

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/cometbft/cometbft/crypto"
)

// NodeID represents a unique identifier for a node in the network
type NodeID string

// Round represents a consensus round number
type Round uint64

// Hash represents a cryptographic hash
type Hash [32]byte

func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

func (h Hash) Bytes() []byte {
	return h[:]
}

// NewHash creates a new hash from bytes
func NewHash(data []byte) Hash {
	return sha256.Sum256(data)
}

// Transaction represents a transaction in the system
type Transaction struct {
	Data []byte
	Hash Hash
}

// NewTransaction creates a new transaction
func NewTransaction(data []byte) *Transaction {
	return &Transaction{
		Data: data,
		Hash: NewHash(data),
	}
}

// Batch represents a collection of transactions
type Batch struct {
	ID           Hash
	Transactions []*Transaction
	Author       NodeID
	Timestamp    time.Time
}

// NewBatch creates a new batch of transactions
func NewBatch(txs []*Transaction, author NodeID) *Batch {
	// Serialize batch data for hashing
	var data []byte
	for _, tx := range txs {
		data = append(data, tx.Data...)
	}
	data = append(data, []byte(author)...)

	return &Batch{
		ID:           NewHash(data),
		Transactions: txs,
		Author:       author,
		Timestamp:    time.Now(),
	}
}

// Header represents a certificate header in Narwhal
type Header struct {
	Author    NodeID
	Round     Round
	Epoch     uint64
	Parents   []Hash // Hashes of parent certificates
	Payload   []Hash // Hashes of batches included in this certificate
	Timestamp time.Time
	Signature []byte
}

// Hash returns the hash of the header
func (h *Header) Hash() Hash {
	// Create deterministic serialization for hashing
	data := make([]byte, 0)
	data = append(data, []byte(h.Author)...)

	// Add round and epoch
	roundBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		roundBytes[i] = byte(h.Round >> (i * 8))
	}
	data = append(data, roundBytes...)

	epochBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		epochBytes[i] = byte(h.Epoch >> (i * 8))
	}
	data = append(data, epochBytes...)

	// Add parents
	for _, parent := range h.Parents {
		data = append(data, parent.Bytes()...)
	}

	// Add payload
	for _, payload := range h.Payload {
		data = append(data, payload.Bytes()...)
	}

	// Add timestamp
	timestampBytes, _ := h.Timestamp.MarshalBinary()
	data = append(data, timestampBytes...)

	return NewHash(data)
}

// Certificate represents a signed certificate in Narwhal DAG
type Certificate struct {
	Header    *Header
	Signature []byte
	Hash      Hash
}

// NewCertificate creates a new certificate
func NewCertificate(header *Header, signature []byte) *Certificate {
	return &Certificate{
		Header:    header,
		Signature: signature,
		Hash:      header.Hash(),
	}
}

// Vote represents a vote for a certificate
type Vote struct {
	CertificateHash Hash
	Author          NodeID
	Signature       []byte
	Timestamp       time.Time
}

// WorkerInfo represents information about a worker node
type WorkerInfo struct {
	ID        NodeID
	Address   string
	PublicKey crypto.PubKey
}

// PrimaryInfo represents information about a primary node
type PrimaryInfo struct {
	ID        NodeID
	Address   string
	PublicKey crypto.PubKey
	Workers   []WorkerInfo
}

// NetworkMessage represents a message sent over the network
type NetworkMessage struct {
	Type    MessageType
	From    NodeID
	To      NodeID
	Payload interface{}
}

// MessageType represents the type of network message
type MessageType int

const (
	MessageTypeBatch MessageType = iota
	MessageTypeHeader
	MessageTypeCertificate
	MessageTypeVote
	MessageTypeSync
	MessageTypeHeartbeat
)

// BatchMessage represents a batch being sent over the network
type BatchMessage struct {
	Batch *Batch
}

// HeaderMessage represents a header being sent over the network
type HeaderMessage struct {
	Header *Header
}

// CertificateMessage represents a certificate being sent over the network
type CertificateMessage struct {
	Certificate *Certificate
}

// VoteMessage represents a vote being sent over the network
type VoteMessage struct {
	Vote *Vote
}

// SyncMessage represents a synchronization request
type SyncMessage struct {
	FromRound Round
	ToRound   Round
}

// HeartbeatMessage represents a heartbeat message
type HeartbeatMessage struct {
	NodeID    NodeID
	Timestamp time.Time
	Round     Round
}

// NodeConfig represents configuration for a Narwhal node
type NodeConfig struct {
	NodeID       NodeID
	PrivateKey   crypto.PrivKey
	PublicKey    crypto.PubKey
	ListenAddr   string
	Workers      []WorkerInfo
	Committee    []PrimaryInfo
	BatchSize    int
	BatchTimeout time.Duration
}

// DAGState represents the current state of the DAG
type DAGState struct {
	CurrentRound  Round
	CurrentEpoch  uint64
	LastCommitted Hash
	Certificates  map[Hash]*Certificate
	Batches       map[Hash]*Batch
}

// NewDAGState creates a new DAG state
func NewDAGState() *DAGState {
	return &DAGState{
		CurrentRound: 0,
		CurrentEpoch: 0,
		Certificates: make(map[Hash]*Certificate),
		Batches:      make(map[Hash]*Batch),
	}
}
