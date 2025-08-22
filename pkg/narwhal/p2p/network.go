package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

const (
	// Protocol IDs for different types of communication
	ProtocolBatch       = "/narwhal/batch/1.0.0"
	ProtocolHeader      = "/narwhal/header/1.0.0"
	ProtocolCertificate = "/narwhal/certificate/1.0.0"
	ProtocolVote        = "/narwhal/vote/1.0.0"
	ProtocolSync        = "/narwhal/sync/1.0.0"

	// Discovery service tag
	DiscoveryServiceTag = "narwhal-node"
)

// Network handles all P2P networking for Narwhal
type Network struct {
	host   host.Host
	logger *zap.Logger
	nodeID types.NodeID
	ctx    context.Context
	cancel context.CancelFunc

	// Message handlers
	batchHandler       func(*types.Batch)
	headerHandler      func(*types.Header)
	certificateHandler func(*types.Certificate)
	voteHandler        func(*types.Vote)

	// Peer management
	peers      map[peer.ID]types.NodeID
	peersMutex sync.RWMutex

	// Discovery (simplified for now)
	// discovery interface{}
}

// NewNetwork creates a new P2P network instance
func NewNetwork(listenAddr string, nodeID types.NodeID, logger *zap.Logger) (*Network, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Parse listen address
	addr, err := multiaddr.NewMultiaddr(listenAddr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("invalid listen address: %w", err)
	}

	// Create libp2p host
	h, err := libp2p.New(
		libp2p.ListenAddrs(addr),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	network := &Network{
		host:   h,
		logger: logger,
		nodeID: nodeID,
		ctx:    ctx,
		cancel: cancel,
		peers:  make(map[peer.ID]types.NodeID),
	}

	// Set up protocol handlers
	network.setupProtocolHandlers()

	logger.Info("P2P network created",
		zap.String("node_id", string(nodeID)),
		zap.String("peer_id", h.ID().String()),
		zap.Strings("addrs", multiaddrsToStrings(h.Addrs())),
	)

	return network, nil
}

// Start starts the P2P network
func (n *Network) Start() error {
	n.logger.Info("Starting P2P network")

	// For Phase 2, we'll use a simplified P2P setup without mDNS discovery
	// This can be enhanced in later phases

	n.logger.Info("P2P network started successfully")
	return nil
}

// Stop stops the P2P network
func (n *Network) Stop() error {
	n.logger.Info("Stopping P2P network")

	n.cancel()
	return n.host.Close()
}

// SetHandlers sets the message handlers
func (n *Network) SetHandlers(
	batchHandler func(*types.Batch),
	headerHandler func(*types.Header),
	certificateHandler func(*types.Certificate),
	voteHandler func(*types.Vote),
) {
	n.batchHandler = batchHandler
	n.headerHandler = headerHandler
	n.certificateHandler = certificateHandler
	n.voteHandler = voteHandler
}

// BroadcastBatch broadcasts a batch to all connected peers
func (n *Network) BroadcastBatch(batch *types.Batch) error {
	msg := &types.BatchMessage{Batch: batch}
	return n.broadcast(ProtocolBatch, msg)
}

// BroadcastHeader broadcasts a header to all connected peers
func (n *Network) BroadcastHeader(header *types.Header) error {
	msg := &types.HeaderMessage{Header: header}
	return n.broadcast(ProtocolHeader, msg)
}

// BroadcastCertificate broadcasts a certificate to all connected peers
func (n *Network) BroadcastCertificate(cert *types.Certificate) error {
	msg := &types.CertificateMessage{Certificate: cert}
	return n.broadcast(ProtocolCertificate, msg)
}

// BroadcastVote broadcasts a vote to all connected peers
func (n *Network) BroadcastVote(vote *types.Vote) error {
	msg := &types.VoteMessage{Vote: vote}
	return n.broadcast(ProtocolVote, msg)
}

// SendBatchToPeer sends a batch to a specific peer
func (n *Network) SendBatchToPeer(peerID peer.ID, batch *types.Batch) error {
	msg := &types.BatchMessage{Batch: batch}
	return n.sendToPeer(peerID, ProtocolBatch, msg)
}

// GetConnectedPeers returns the list of connected peers
func (n *Network) GetConnectedPeers() []peer.ID {
	return n.host.Network().Peers()
}

// GetNodeID returns the node ID
func (n *Network) GetNodeID() types.NodeID {
	return n.nodeID
}

// GetPeerID returns the libp2p peer ID
func (n *Network) GetPeerID() peer.ID {
	return n.host.ID()
}

// setupProtocolHandlers sets up handlers for all protocols
func (n *Network) setupProtocolHandlers() {
	n.host.SetStreamHandler(protocol.ID(ProtocolBatch), n.handleBatchStream)
	n.host.SetStreamHandler(protocol.ID(ProtocolHeader), n.handleHeaderStream)
	n.host.SetStreamHandler(protocol.ID(ProtocolCertificate), n.handleCertificateStream)
	n.host.SetStreamHandler(protocol.ID(ProtocolVote), n.handleVoteStream)
	n.host.SetStreamHandler(protocol.ID(ProtocolSync), n.handleSyncStream)
}

// broadcast sends a message to all connected peers
func (n *Network) broadcast(protocolID string, msg interface{}) error {
	peers := n.GetConnectedPeers()
	if len(peers) == 0 {
		n.logger.Debug("No peers connected for broadcast")
		return nil
	}

	var errors []error
	for _, peerID := range peers {
		if err := n.sendToPeer(peerID, protocolID, msg); err != nil {
			n.logger.Error("Failed to send message to peer",
				zap.String("peer", peerID.String()),
				zap.Error(err),
			)
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to send to %d peers", len(errors))
	}

	return nil
}

// sendToPeer sends a message to a specific peer
func (n *Network) sendToPeer(peerID peer.ID, protocolID string, msg interface{}) error {
	stream, err := n.host.NewStream(n.ctx, peerID, protocol.ID(protocolID))
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	defer stream.Close()

	// Set deadline for the operation
	deadline := time.Now().Add(10 * time.Second)
	stream.SetDeadline(deadline)

	// Encode and send message
	encoder := json.NewEncoder(stream)
	if err := encoder.Encode(msg); err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	return nil
}

// ConnectToPeer connects to a specific peer by multiaddr
func (n *Network) ConnectToPeer(addr string) error {
	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return fmt.Errorf("invalid multiaddr: %w", err)
	}

	peerinfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return fmt.Errorf("failed to get peer info: %w", err)
	}

	if err := n.host.Connect(n.ctx, *peerinfo); err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}

	n.peersMutex.Lock()
	n.peers[peerinfo.ID] = types.NodeID(peerinfo.ID.String())
	n.peersMutex.Unlock()

	n.logger.Info("Connected to peer",
		zap.String("peer_id", peerinfo.ID.String()),
		zap.String("addr", addr),
	)

	return nil
}

// ConnectToPeers connects to multiple peers from a list of addresses
func (n *Network) ConnectToPeers(addrs []string) error {
	var errors []error
	
	for _, addr := range addrs {
		if err := n.ConnectToPeer(addr); err != nil {
			n.logger.Error("Failed to connect to peer",
				zap.String("addr", addr),
				zap.Error(err),
			)
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to connect to %d peers", len(errors))
	}

	return nil
}

// startDiscovery starts discovery for finding other nodes
func (n *Network) startDiscovery() error {
	// For now, we use manual peer connection
	// In future versions, we can add mDNS or DHT discovery
	n.logger.Info("Peer discovery started (manual connection mode)")
	return nil
}

// Stream handlers for different message types

func (n *Network) handleBatchStream(stream network.Stream) {
	defer stream.Close()

	var msg types.BatchMessage
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&msg); err != nil {
		n.logger.Error("Failed to decode batch message", zap.Error(err))
		return
	}

	if n.batchHandler != nil {
		n.batchHandler(msg.Batch)
	}
}

func (n *Network) handleHeaderStream(stream network.Stream) {
	defer stream.Close()

	var msg types.HeaderMessage
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&msg); err != nil {
		n.logger.Error("Failed to decode header message", zap.Error(err))
		return
	}

	if n.headerHandler != nil {
		n.headerHandler(msg.Header)
	}
}

func (n *Network) handleCertificateStream(stream network.Stream) {
	defer stream.Close()

	var msg types.CertificateMessage
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&msg); err != nil {
		n.logger.Error("Failed to decode certificate message", zap.Error(err))
		return
	}

	if n.certificateHandler != nil {
		n.certificateHandler(msg.Certificate)
	}
}

func (n *Network) handleVoteStream(stream network.Stream) {
	defer stream.Close()

	var msg types.VoteMessage
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&msg); err != nil {
		n.logger.Error("Failed to decode vote message", zap.Error(err))
		return
	}

	if n.voteHandler != nil {
		n.voteHandler(msg.Vote)
	}
}

func (n *Network) handleSyncStream(stream network.Stream) {
	defer stream.Close()

	var msg types.SyncMessage
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&msg); err != nil {
		n.logger.Error("Failed to decode sync message", zap.Error(err))
		return
	}

	// Handle sync request - for now just log it
	n.logger.Info("Received sync request",
		zap.Uint64("from_round", uint64(msg.FromRound)),
		zap.Uint64("to_round", uint64(msg.ToRound)),
	)
}

// Peer discovery will be implemented in later phases
// For Phase 2, we focus on single-node Narwhal functionality

// Helper functions

func multiaddrsToStrings(addrs []multiaddr.Multiaddr) []string {
	strs := make([]string, len(addrs))
	for i, addr := range addrs {
		strs[i] = addr.String()
	}
	return strs
}
