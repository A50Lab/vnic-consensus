package config

import "fmt"

type Config struct {
	AppAddr        string
	GenesisFile    string
	HomeDir        string
	ConnectionMode string
	RPC            RPCConfig
	P2P            P2PConfig
}

type P2PConfig struct {
	ListenAddress string
	BootstrapPeers []string
	MaxPeers      int
}

type RPCConfig struct {
	ListenAddress             string
	CORSAllowedOrigins        []string
	CORSAllowedMethods        []string
	CORSAllowedHeaders        []string
	MaxOpenConnections        int
	MaxSubscriptionClients    int
	MaxSubscriptionsPerClient int
	TimeoutBroadcastTxCommit  string
	MaxBodyBytes              int64
	MaxHeaderBytes            int
	TLSCertFile               string
	TLSKeyFile                string
}

func (c *Config) Validate() error {
	if c.ConnectionMode != "grpc" && c.ConnectionMode != "socket" {
		return fmt.Errorf("invalid connection mode: %s, must be 'grpc' or 'socket'", c.ConnectionMode)
	}
	return nil
}

func DefaultRPCConfig() RPCConfig {
	return RPCConfig{
		ListenAddress:             "tcp://0.0.0.0:26657",
		CORSAllowedOrigins:        []string{"*"},
		CORSAllowedMethods:        []string{"HEAD", "GET", "POST"},
		CORSAllowedHeaders:        []string{"Origin", "Accept", "Content-Type", "X-Requested-With", "X-Server-Time"},
		MaxOpenConnections:        900,
		MaxSubscriptionClients:    100,
		MaxSubscriptionsPerClient: 5,
		TimeoutBroadcastTxCommit:  "10s",
		MaxBodyBytes:              1000000,
		MaxHeaderBytes:            1048576,
		TLSCertFile:               "",
		TLSKeyFile:                "",
	}
}

func DefaultP2PConfig() P2PConfig {
	return P2PConfig{
		ListenAddress:  "/ip4/127.0.0.1/tcp/9000",
		BootstrapPeers: []string{},
		MaxPeers:       50,
	}
}
