package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v2"
	"github.com/vietchain/vniccss/pkg/config"
	"github.com/vietchain/vniccss/pkg/consensus"
)

const version = "v0.1.0"

func main() {
	app := &cli.App{
		Name:    "vniccss",
		Usage:   "Narwhal and Bullshark consensus for Cosmos applications",
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "app-addr",
				Usage:    "Address of the Cosmos application ABCI server",
				Value:    "tcp://0.0.0.0:26658",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "genesis-file",
				Usage:    "Path to the genesis file",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "home-dir",
				Usage:    "Home directory for node configuration and data",
				Value:    "~/.vniccss",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "connection-mode",
				Usage:    "ABCI connection mode (grpc or socket)",
				Value:    "grpc",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "rpc-listen-addr",
				Usage:    "Address for RPC server to listen on",
				Value:    "tcp://0.0.0.0:26656",
				Required: false,
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(c *cli.Context) error {
	rpcConfig := config.DefaultRPCConfig()
	rpcConfig.ListenAddress = c.String("rpc-listen-addr")

	cfg := &config.Config{
		AppAddr:        c.String("app-addr"),
		GenesisFile:    c.String("genesis-file"),
		HomeDir:        c.String("home-dir"),
		ConnectionMode: c.String("connection-mode"),
		RPC:            rpcConfig,
	}

	fmt.Printf("Starting vniccss consensus engine...\n")
	fmt.Printf("App address: %s\n", cfg.AppAddr)
	fmt.Printf("RPC listen address: %s\n", cfg.RPC.ListenAddress)
	fmt.Printf("Genesis file: %s\n", cfg.GenesisFile)
	fmt.Printf("Home directory: %s\n", cfg.HomeDir)
	fmt.Printf("Connection mode: %s\n", cfg.ConnectionMode)

	node, err := consensus.NewNode(cfg)
	if err != nil {
		return fmt.Errorf("failed to create consensus node: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- node.Start()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		fmt.Printf("Received signal: %s, shutting down...\n", sig)
		node.Stop()
		return nil
	case err := <-errCh:
		return err
	}
}
