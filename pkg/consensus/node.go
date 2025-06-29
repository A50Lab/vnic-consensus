package consensus

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	comet_abciclient "github.com/cometbft/cometbft/abci/client"
	"github.com/cometbft/cometbft/crypto/ed25519"
	cometlog "github.com/cometbft/cometbft/libs/log"
	genutiltypes "github.com/cosmos/cosmos-sdk/x/genutil/types"
	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/abci"
	bullshark "github.com/vietchain/vniccss/pkg/bullshark/core"
	bullsharktypes "github.com/vietchain/vniccss/pkg/bullshark/types"
	"github.com/vietchain/vniccss/pkg/config"
	narwhal "github.com/vietchain/vniccss/pkg/narwhal/core"
	narwhaltypes "github.com/vietchain/vniccss/pkg/narwhal/types"
	"github.com/vietchain/vniccss/pkg/rpc"
	"github.com/vietchain/vniccss/pkg/shoal"
)

type Node struct {
	config           *config.Config
	logger           *zap.Logger
	abciClient       *abci.Client
	rpcServer        *rpc.Server
	narwhal          *narwhal.Narwhal
	bullshark        *bullshark.Bullshark
	shoalIntegration *shoal.ShoalIntegration
}

func NewNode(cfg *config.Config) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	// Initialize Shoal integration
	shoalConfig := shoal.DefaultShoalConfig()
	shoalIntegration := shoal.NewShoalIntegration(shoalConfig, logger.Named("shoal"))

	return &Node{
		config:           cfg,
		logger:           logger,
		shoalIntegration: shoalIntegration,
	}, nil
}

func (n *Node) Start() error {
	n.logger.Info("Starting Narwhal-Bullshark consensus node",
		zap.String("consensus_addr", n.config.RPC.ListenAddress),
		zap.String("app_addr", n.config.AppAddr),
		zap.String("genesis_file", n.config.GenesisFile),
		zap.String("home_dir", n.config.HomeDir),
	)

	// Initialize ABCI client
	if err := n.initABCIClient(); err != nil {
		return fmt.Errorf("failed to initialize ABCI client: %w", err)
	}

	// Initialize Narwhal consensus
	if err := n.initNarwhal(); err != nil {
		return fmt.Errorf("failed to initialize Narwhal: %w", err)
	}

	// Initialize Bullshark consensus
	if err := n.initBullshark(); err != nil {
		return fmt.Errorf("failed to initialize Bullshark: %w", err)
	}

	// Initialize RPC server
	if err := n.initRPCServer(); err != nil {
		return fmt.Errorf("failed to initialize RPC server: %w", err)
	}

	// Start Shoal integration
	if err := n.shoalIntegration.Start(); err != nil {
		return fmt.Errorf("failed to start Shoal integration: %w", err)
	}

	// Start Narwhal
	if err := n.narwhal.Start(); err != nil {
		return fmt.Errorf("failed to start Narwhal: %w", err)
	}

	// Start Bullshark
	if err := n.bullshark.Start(); err != nil {
		return fmt.Errorf("failed to start Bullshark: %w", err)
	}

	// Start RPC server
	go func() {
		if err := n.rpcServer.Start(context.Background()); err != nil {
			n.logger.Error("RPC server failed", zap.Error(err))
		}
	}()

	n.logger.Info("Node started successfully")

	// Keep the node running
	select {}
}

func (n *Node) initABCIClient() error {
	n.logger.Info("Initializing ABCI client", zap.String("app_addr", n.config.AppAddr))

	appGenesis, err := genutiltypes.AppGenesisFromFile(n.config.GenesisFile)
	if err != nil {
		return fmt.Errorf("failed to read genesis file: %w", err)
	}
	genesisDoc, err := appGenesis.ToGenesisDoc()
	if err != nil {
		return fmt.Errorf("failed to convert genesis: %w", err)
	}

	// Create ABCI client
	var client comet_abciclient.Client
	if n.config.ConnectionMode == "grpc" {
		client = comet_abciclient.NewGRPCClient(n.config.AppAddr, true)
	} else {
		client = comet_abciclient.NewSocketClient(n.config.AppAddr, true)
	}

	// Create logger for ABCI client
	cometLogger := cometlog.NewTMLogger(cometlog.NewSyncWriter(os.Stdout))
	client.SetLogger(cometLogger)

	// Start the client
	if err := client.Start(); err != nil {
		return fmt.Errorf("failed to start ABCI client: %w", err)
	}

	n.abciClient = abci.NewClient(client, genesisDoc, n.logger)

	// Check current chain state first
	info, err := n.abciClient.Info(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get app info: %w", err)
	}

	n.logger.Info("Retrieved app info",
		zap.String("app_name", info.Data),
		zap.String("version", info.Version),
		zap.Int64("last_block_height", info.LastBlockHeight),
		zap.String("last_block_app_hash", fmt.Sprintf("%X", info.LastBlockAppHash)),
	)

	// Only initialize chain if this is genesis (height 0)
	if info.LastBlockHeight == 0 {
		n.logger.Info("Genesis detected, initializing chain via ABCI")
		if err := n.abciClient.InitChain(context.Background()); err != nil {
			return fmt.Errorf("failed to initialize chain: %w", err)
		}
	} else {
		n.logger.Info("Existing chain detected, syncing with current state",
			zap.Int64("current_height", info.LastBlockHeight),
		)

		homeDir := n.config.HomeDir
		exportedGenesisPath := filepath.Join(homeDir, "exported_genesis.json")
		if _, err := os.Stat(exportedGenesisPath); err != nil {
			return fmt.Errorf("failed to load exported genesis: %w", err)
		}

		n.logger.Info("Found exported genesis file, loading current state", zap.String("path", exportedGenesisPath))
		genesisDoc, err = abci.LoadGenesisDocFromFile(exportedGenesisPath)
		if err != nil {
			return fmt.Errorf("failed to load exported genesis: %w", err)
		}
		n.abciClient = abci.NewClient(client, genesisDoc, n.logger)
		if err := n.abciClient.SyncWithChainState(info); err != nil {
			return fmt.Errorf("failed to sync with chain state: %w", err)
		}
	}

	n.logger.Info("ABCI client initialized successfully")
	return nil
}

func (n *Node) initNarwhal() error {
	n.logger.Info("Initializing Narwhal consensus engine")

	// Generate a unique node ID
	nodeID := narwhaltypes.NodeID(fmt.Sprintf("node-%s", n.config.RPC.ListenAddress))

	// Generate cryptographic keys
	privateKey := ed25519.GenPrivKey()

	// Create worker configurations (for single node setup, create one worker)
	workers := []narwhaltypes.WorkerInfo{
		{
			ID:        narwhaltypes.NodeID(fmt.Sprintf("%s-worker-0", nodeID)),
			Address:   n.config.RPC.ListenAddress,
			PublicKey: privateKey.PubKey(),
		},
	}

	// Create primary info for committee (single node committee)
	primaryInfo := narwhaltypes.PrimaryInfo{
		ID:        nodeID,
		Address:   n.config.RPC.ListenAddress,
		PublicKey: privateKey.PubKey(),
		Workers:   workers,
	}

	// Create Narwhal configuration
	narwhalConfig := &narwhaltypes.NodeConfig{
		NodeID:       nodeID,
		PrivateKey:   privateKey,
		PublicKey:    privateKey.PubKey(),
		ListenAddr:   "/ip4/127.0.0.1/tcp/9000", // P2P listen address
		Workers:      workers,
		Committee:    []narwhaltypes.PrimaryInfo{primaryInfo}, // Single node committee
		BatchSize:    50,
		BatchTimeout: 5 * time.Second,
	}

	// Create Narwhal instance
	var err error
	n.narwhal, err = narwhal.NewNarwhal(narwhalConfig, n.logger)
	if err != nil {
		return fmt.Errorf("failed to create Narwhal instance: %w", err)
	}

	// Configure primary for faster certificate creation to match block interval
	n.narwhal.GetPrimary().SetConfig(
		3*time.Second, // Match Bullshark block interval
		100,           // Max payload size
		0,             // Min payload size (allow empty certificates)
	)

	// Set up callbacks for transaction handling
	n.narwhal.SetCallbacks(
		func(tx *narwhaltypes.Transaction) {
			n.logger.Debug("Transaction received by Narwhal",
				zap.String("tx_hash", tx.Hash.String()),
			)
		},
		func(cert *narwhaltypes.Certificate) {
			n.logger.Debug("Certificate created by Narwhal",
				zap.String("cert_hash", cert.Hash.String()),
				zap.Uint64("round", uint64(cert.Header.Round)),
			)
		},
		func(batch *narwhaltypes.Batch) {
			n.logger.Debug("Batch created by Narwhal",
				zap.String("batch_id", batch.ID.String()),
				zap.Int("tx_count", len(batch.Transactions)),
			)
		},
	)

	n.logger.Info("Narwhal consensus engine initialized successfully",
		zap.String("node_id", string(nodeID)),
		zap.Int("worker_count", len(workers)),
	)

	return nil
}

func (n *Node) initBullshark() error {
	n.logger.Info("Initializing Bullshark consensus engine")

	// Create Bullshark instance
	n.bullshark = bullshark.NewBullshark(
		n.narwhal.GetMempool(), // Get DAG mempool from Narwhal
		n.abciClient,           // ABCI client for block execution
		n.logger,               // Logger
	)

	// Integrate Shoal with Bullshark components
	n.shoalIntegration.SetBullsharkEngine(n.bullshark.GetEngine())
	n.shoalIntegration.SetBullsharkSelector(n.bullshark.GetSelector())
	n.shoalIntegration.SetNarwhalPrimary(n.narwhal.GetPrimary())

	// Initialize Bullshark consensus state with current ABCI state
	abciState := n.abciClient.GetState()
	n.logger.Info("Initializing Bullshark with ABCI state",
		zap.Int64("abci_height", abciState.LastBlockHeight),
		zap.String("app_hash", fmt.Sprintf("%X", abciState.AppHash)),
		zap.String("chain_id", abciState.ChainID),
	)
	n.bullshark.InitializeFromABCI(abciState.LastBlockHeight, abciState.AppHash, abciState.LastBlockID.Hash)

	// Validate chain continuity before proceeding
	if err := n.abciClient.ValidateChainContinuity(context.Background()); err != nil {
		return fmt.Errorf("chain continuity validation failed: %w", err)
	}

	// Set up callbacks for block events
	n.bullshark.SetCallbacks(
		func(block *bullsharktypes.BullsharkBlock) {
			n.logger.Info("Block created by Bullshark",
				zap.Int64("height", int64(block.Height)),
				zap.Int("transactions", block.TransactionCount),
				zap.String("block_hash", block.BlockHash.String()),
			)

			// Update Shoal metrics with block creation latency
			if n.shoalIntegration.IsRunning() {
				blockLatency := time.Since(block.Timestamp)
				n.shoalIntegration.UpdateLatency(blockLatency)
			}
		},
		func(result *bullshark.ExecutionResult) {
			n.logger.Info("Block executed by Bullshark",
				zap.Int64("height", int64(result.Block.Height)),
				zap.Bool("success", result.Success),
				zap.Duration("execution_time", result.ExecutionTime),
			)
		},
	)

	n.logger.Info("Bullshark consensus engine initialized successfully")
	return nil
}

func (n *Node) initRPCServer() error {
	n.logger.Info("Initializing RPC server", zap.String("listen_addr", n.config.RPC.ListenAddress))

	n.rpcServer = rpc.NewServer(n.config.RPC, n.abciClient, n.logger)

	n.logger.Info("RPC server initialized successfully")
	return nil
}

func (n *Node) Stop() error {
	n.logger.Info("Stopping consensus node")

	// Export chain state before shutdown
	homeDir := n.config.HomeDir
	if strings.HasPrefix(homeDir, "~") {
		if userHome, err := os.UserHomeDir(); err == nil {
			homeDir = filepath.Join(userHome, homeDir[1:])
		}
	}
	exportedGenesisPath := filepath.Join(homeDir, "exported_genesis.json")
	if n.abciClient != nil {
		err := n.abciClient.ExportStateToGenesisFile(exportedGenesisPath)
		if err != nil {
			n.logger.Error("Failed to export chain state", zap.Error(err))
		}
	}

	// Log final Shoal metrics before shutdown
	if n.shoalIntegration != nil && n.shoalIntegration.IsRunning() {
		metrics := n.shoalIntegration.GetMetrics()
		n.logger.Info("Final Shoal metrics",
			zap.Duration("avg_latency", metrics.AvgLatency),
			zap.Float64("messages_per_second", metrics.MessagesPerSecond),
			zap.Float64("error_rate", metrics.ErrorRate),
			zap.Duration("adaptive_timeout", metrics.AdaptiveTimeoutValue),
		)
	}

	if n.rpcServer != nil {
		if err := n.rpcServer.Stop(); err != nil {
			n.logger.Error("Failed to stop RPC server", zap.Error(err))
		}
	}

	if n.bullshark != nil {
		if err := n.bullshark.Stop(); err != nil {
			n.logger.Error("Failed to stop Bullshark", zap.Error(err))
		}
	}

	if n.narwhal != nil {
		if err := n.narwhal.Stop(); err != nil {
			n.logger.Error("Failed to stop Narwhal", zap.Error(err))
		}
	}

	if n.shoalIntegration != nil {
		if err := n.shoalIntegration.Stop(); err != nil {
			n.logger.Error("Failed to stop Shoal integration", zap.Error(err))
		}
	}

	if n.abciClient != nil {
		n.abciClient.Stop()
	}

	n.logger.Info("Node stopped")
	return nil
}

// GetShoalMetrics returns current Shoal performance metrics
func (n *Node) GetShoalMetrics() shoal.PerformanceMetrics {
	if n.shoalIntegration != nil {
		return n.shoalIntegration.GetMetrics()
	}
	return shoal.PerformanceMetrics{}
}

// GetShoalStats returns detailed Shoal statistics
func (n *Node) GetShoalStats() map[string]interface{} {
	if n.shoalIntegration != nil {
		return n.shoalIntegration.GetDetailedStats()
	}
	return map[string]interface{}{"shoal_enabled": false}
}
