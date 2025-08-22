package rpc

import (
	"time"

	"github.com/cometbft/cometbft/crypto"
)

// Custom VniCCSS RPC types for enhanced functionality

type ResultNarwhalStatus struct {
	DAGHeight           int64         `json:"dag_height"`
	PendingCertificates int           `json:"pending_certificates"`
	ProcessedBatches    int64         `json:"processed_batches"`
	ActiveWorkers       []WorkerInfo  `json:"active_workers"`
	MemPoolSize         int           `json:"mempool_size"`
	BatchesPerSecond    float64       `json:"batches_per_second"`
	NetworkLatency      time.Duration `json:"network_latency"`
	CertificateLatency  time.Duration `json:"certificate_latency"`
}

type WorkerInfo struct {
	ID               string        `json:"id"`
	Address          string        `json:"address"`
	IsOnline         bool          `json:"is_online"`
	LastSeenTime     time.Time     `json:"last_seen_time"`
	ProcessedBatches int64         `json:"processed_batches"`
	Latency          time.Duration `json:"latency"`
}

type ResultBullsharkStatus struct {
	ConsensusHeight          int64             `json:"consensus_height"`
	LastAnchorHeight         int64             `json:"last_anchor_height"`
	OrderingLatency          time.Duration     `json:"ordering_latency"`
	TransactionsOrdered      int64             `json:"transactions_ordered"`
	ActiveValidators         []ValidatorStatus `json:"active_validators"`
	BlockInterval            time.Duration     `json:"block_interval"`
	AnchorSelectionAlgorithm string            `json:"anchor_selection_algorithm"`
}

type ValidatorStatus struct {
	Address       crypto.Address `json:"address"`
	PubKey        crypto.PubKey  `json:"pub_key"`
	VotingPower   int64          `json:"voting_power"`
	IsProposer    bool           `json:"is_proposer"`
	LastProposal  time.Time      `json:"last_proposal"`
	ProposalCount int64          `json:"proposal_count"`
}

type ResultShoalMetrics struct {
	PerformanceScore      float64           `json:"performance_score"`
	LatencyReduction      float64           `json:"latency_reduction_percent"`
	AdaptiveTimeout       time.Duration     `json:"adaptive_timeout"`
	LeaderReputation      []ReputationScore `json:"leader_reputation"`
	CertificatePipeline   PipelineMetrics   `json:"certificate_pipeline"`
	FailureRecoveryTime   time.Duration     `json:"failure_recovery_time"`
	ThroughputImprovement float64           `json:"throughput_improvement_percent"`
}

type ReputationScore struct {
	NodeID           string        `json:"node_id"`
	Score            float64       `json:"score"`
	SuccessfulBlocks int64         `json:"successful_blocks"`
	FailedBlocks     int64         `json:"failed_blocks"`
	AverageLatency   time.Duration `json:"average_latency"`
	LastUpdated      time.Time     `json:"last_updated"`
}

type PipelineMetrics struct {
	ActivePipelines    int           `json:"active_pipelines"`
	CompletedPipelines int64         `json:"completed_pipelines"`
	AverageLatency     time.Duration `json:"average_latency"`
	ParallelismLevel   int           `json:"parallelism_level"`
	EfficiencyScore    float64       `json:"efficiency_score"`
}

type ResultVniccssInfo struct {
	Version         string                `json:"version"`
	ConsensusEngine string                `json:"consensus_engine"`
	Features        []string              `json:"features"`
	Narwhal         ResultNarwhalStatus   `json:"narwhal"`
	Bullshark       ResultBullsharkStatus `json:"bullshark"`
	Shoal           ResultShoalMetrics    `json:"shoal"`
	StartTime       time.Time             `json:"start_time"`
	Uptime          time.Duration         `json:"uptime"`
}

// CosmJS compatibility types
type CosmJSAccount struct {
	Address       string `json:"address"`
	AccountNumber uint64 `json:"account_number"`
	Sequence      uint64 `json:"sequence"`
	PubKey        string `json:"pub_key,omitempty"`
}

type CosmJSBalance struct {
	Denom  string `json:"denom"`
	Amount string `json:"amount"`
}

type CosmJSTxResponse struct {
	Height    string        `json:"height"`
	TxHash    string        `json:"txhash"`
	Code      uint32        `json:"code"`
	Data      string        `json:"data"`
	RawLog    string        `json:"raw_log"`
	Logs      []CosmJSTxLog `json:"logs"`
	Info      string        `json:"info"`
	GasWanted string        `json:"gas_wanted"`
	GasUsed   string        `json:"gas_used"`
	Tx        interface{}   `json:"tx"`
	Timestamp string        `json:"timestamp"`
	Events    []CosmJSEvent `json:"events"`
}

type CosmJSTxLog struct {
	MsgIndex int           `json:"msg_index"`
	Log      string        `json:"log"`
	Events   []CosmJSEvent `json:"events"`
}

type CosmJSEvent struct {
	Type       string                 `json:"type"`
	Attributes []CosmJSEventAttribute `json:"attributes"`
}

type CosmJSEventAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Index bool   `json:"index,omitempty"`
}

// Custom ValidatorInfo for proper JSON formatting
type CustomValidatorInfo struct {
	Address     string       `json:"address"`
	PubKey      PubKeyObject `json:"pub_key"`
	VotingPower string       `json:"voting_power"`
}

type PubKeyObject struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// CustomResultStatus for status endpoint with proper validator_info formatting
type CustomResultStatus struct {
	NodeInfo      interface{}          `json:"node_info"`
	SyncInfo      interface{}          `json:"sync_info"`
	ValidatorInfo CustomValidatorInfo  `json:"validator_info"`
}
