package rpc

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/crypto/merkle"

	"github.com/cometbft/cometbft/libs/bytes"
	"github.com/cometbft/cometbft/p2p"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"

	"github.com/cometbft/cometbft/version"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/spf13/cast"
	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/abci"
	"github.com/vietchain/vniccss/pkg/config"
	narwhal "github.com/vietchain/vniccss/pkg/narwhal/core"
)

type Server struct {
	config        config.RPCConfig
	logger        *zap.Logger
	abciClient    *abci.Client
	narwhal       *narwhal.Narwhal
	httpServer    *http.Server
	upgrader      websocket.Upgrader
	subscriptions map[string]*Subscription
	subsMutex     sync.RWMutex
	eventBus      *EventBus
}

type JSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
}

type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

type Subscription struct {
	ID      string
	Query   string
	Conn    *websocket.Conn
	EventCh chan *coretypes.ResultEvent
	QuitCh  chan struct{}
}

type EventBus struct {
	subscribers map[string][]*Subscription
	mutex       sync.RWMutex
}

func NewServer(cfg config.RPCConfig, abciClient *abci.Client, narwhal *narwhal.Narwhal, logger *zap.Logger) *Server {
	return &Server{
		config:        cfg,
		logger:        logger,
		abciClient:    abciClient,
		narwhal:       narwhal,
		subscriptions: make(map[string]*Subscription),
		eventBus:      NewEventBus(),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[string][]*Subscription),
	}
}

func (s *Server) Start(ctx context.Context) error {
	router := mux.NewRouter()

	router.HandleFunc("/", s.handleJSONRPC).Methods("POST")
	router.HandleFunc("/websocket", s.handleWebSocket)

	router.HandleFunc("/health", s.handleHTTPEndpoint("health")).Methods("GET")
	router.HandleFunc("/status", s.handleHTTPEndpoint("status")).Methods("GET")
	router.HandleFunc("/genesis", s.handleHTTPEndpoint("genesis")).Methods("GET")
	router.HandleFunc("/validators", s.handleHTTPEndpoint("validators")).Methods("GET")
	router.HandleFunc("/block", s.handleHTTPEndpoint("block")).Methods("GET")
	router.HandleFunc("/block_results", s.handleHTTPEndpoint("block_results")).Methods("GET")
	router.HandleFunc("/blockchain", s.handleHTTPEndpoint("blockchain")).Methods("GET")
	router.HandleFunc("/tx", s.handleHTTPEndpoint("tx")).Methods("GET")
	router.HandleFunc("/abci_info", s.handleHTTPEndpoint("abci_info")).Methods("GET")
	router.HandleFunc("/abci_query", s.handleHTTPEndpoint("abci_query")).Methods("GET")
	router.HandleFunc("/net_info", s.handleHTTPEndpoint("net_info")).Methods("GET")
	router.HandleFunc("/consensus_params", s.handleHTTPEndpoint("consensus_params")).Methods("GET")
	router.HandleFunc("/broadcast_tx_sync", s.handleHTTPEndpoint("broadcast_tx_sync")).Methods("GET", "POST")
	router.HandleFunc("/broadcast_tx_async", s.handleHTTPEndpoint("broadcast_tx_async")).Methods("GET", "POST")
	router.HandleFunc("/broadcast_tx_commit", s.handleHTTPEndpoint("broadcast_tx_commit")).Methods("GET", "POST")

	// Custom VniCCSS endpoints
	router.HandleFunc("/narwhal_status", s.handleHTTPEndpoint("narwhal_status")).Methods("GET")
	router.HandleFunc("/bullshark_status", s.handleHTTPEndpoint("bullshark_status")).Methods("GET")
	router.HandleFunc("/shoal_metrics", s.handleHTTPEndpoint("shoal_metrics")).Methods("GET")
	router.HandleFunc("/vniccss_info", s.handleHTTPEndpoint("vniccss_info")).Methods("GET")

	// CosmJS compatibility endpoints
	router.HandleFunc("/cosmos/auth/v1beta1/accounts/{address}", s.handleCosmosAccount).Methods("GET")
	router.HandleFunc("/cosmos/bank/v1beta1/balances/{address}", s.handleCosmosBalances).Methods("GET")
	router.HandleFunc("/cosmos/tx/v1beta1/txs/{hash}", s.handleCosmosTx).Methods("GET")

	corsHandler := handlers.CORS(
		handlers.AllowedOrigins(s.config.CORSAllowedOrigins),
		handlers.AllowedMethods(s.config.CORSAllowedMethods),
		handlers.AllowedHeaders(s.config.CORSAllowedHeaders),
	)(router)

	u, err := url.Parse(s.config.ListenAddress)
	if err != nil {
		return fmt.Errorf("invalid listen address: %w", err)
	}

	addr := u.Host
	if u.Scheme == "unix" {
		addr = u.Path
	}

	s.httpServer = &http.Server{
		Addr:           addr,
		Handler:        corsHandler,
		MaxHeaderBytes: s.config.MaxHeaderBytes,
	}

	s.logger.Info("Starting RPC server", zap.String("address", s.config.ListenAddress))

	go func() {
		<-ctx.Done()
		s.logger.Info("Shutting down RPC server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.httpServer.Shutdown(shutdownCtx)
	}()

	if u.Scheme == "unix" {
		listener, err := net.Listen("unix", addr)
		if err != nil {
			return err
		}
		return s.httpServer.Serve(listener)
	}

	return s.httpServer.ListenAndServe()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) handleHTTPEndpoint(method string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var params interface{}

		switch r.Method {
		case "GET":
			query := r.URL.Query()
			paramMap := make(map[string]interface{})
			for k, v := range query {
				if len(v) == 1 {
					paramMap[k] = v[0]
				} else {
					paramMap[k] = v
				}
			}

			// Extract path variables for cosmos endpoints
			vars := mux.Vars(r)
			for k, v := range vars {
				paramMap[k] = v
			}

			params = paramMap
		case "POST":
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
				params = body
			}
		}

		result, err := s.handleRPCMethod(r.Context(), method, params)

		response := JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      -1,
		}

		if err != nil {
			response.Error = &RPCError{
				Code:    -32603,
				Message: err.Error(),
			}
		} else {
			response.Result = result
		}

		response.Result = s.formatRPCResponse(response.Result)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

func (s *Server) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	var req JSONRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, nil, -32700, "Parse error", err.Error())
		return
	}

	if req.JSONRPC != "2.0" {
		s.sendError(w, req.ID, -32600, "Invalid Request", "jsonrpc must be 2.0")
		return
	}

	result, err := s.handleRPCMethod(r.Context(), req.Method, req.Params)
	if err != nil {
		s.sendError(w, req.ID, -32603, "Internal error", err.Error())
		return
	}

	response := JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      -1,
		Result:  result,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) formatRPCResponse(result any) any {
	tmp := map[string]any{}
	r, _ := json.Marshal(result)
	err := json.Unmarshal(r, &tmp)
	if err != nil {
		return result
	}

	for k, v := range tmp {
		fmt.Println("TMP: ", k, " ", v, " ", reflect.TypeOf(v))
		if _, ok := v.(map[string]any); ok {
			tmp[k] = s.formatRPCResponse(v)
		} else if _, ok := v.(bool); ok {
			tmp[k] = v
		} else {
			tmp[k] = cast.ToString(v)
		}
	}

	return tmp
}

func (s *Server) handleRPCMethod(ctx context.Context, method string, params interface{}) (interface{}, error) {
	fmt.Println("DEBUG: METHOD: ", method)
	fmt.Println("DEBUG: PARAMS: ", params)

	switch method {
	case "health":
		return s.checkHealth(ctx)
	case "status":
		return s.status(ctx)
	case "genesis":
		return s.genesis(ctx)
	case "validators":
		return s.validators(ctx, params)
	case "block":
		return s.block(ctx, params)
	case "block_results":
		return s.blockResults(ctx, params)
	case "blockchain":
		return s.blockchain(ctx, params)
	case "tx":
		return s.tx(ctx, params)
	case "tx_search":
		return s.txSearch(ctx, params)
	case "abci_info":
		return s.abciInfo(ctx)
	case "abci_query":
		return s.abciQuery(ctx, params)
	case "net_info":
		return s.netInfo(ctx)
	case "consensus_params":
		return s.consensusParams(ctx, params)
	case "broadcast_tx_sync":
		return s.broadcastTxSync(ctx, params)
	case "broadcast_tx_async":
		return s.broadcastTxAsync(ctx, params)
	case "broadcast_tx_commit":
		return s.broadcastTxCommit(ctx, params)
	case "subscribe":
		return s.subscribe(ctx, params)
	case "unsubscribe":
		return s.unsubscribe(ctx, params)
	case "unsubscribe_all":
		return s.unsubscribeAll(ctx, params)
	// Custom VniCCSS methods
	case "narwhal_status":
		return s.narwhalStatus(ctx)
	case "bullshark_status":
		return s.bullsharkStatus(ctx)
	case "shoal_metrics":
		return s.shoalMetrics(ctx)
	case "vniccss_info":
		return s.vniccssInfo(ctx)
	// CosmJS compatibility methods
	case "cosmos_account":
		return s.cosmosAccount(ctx, params)
	case "cosmos_balances":
		return s.cosmosBalances(ctx, params)
	case "cosmos_tx":
		return s.cosmosTx(ctx, params)
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	}
}

func (s *Server) checkHealth(ctx context.Context) (interface{}, error) {
	return nil, nil
}

func (s *Server) status(ctx context.Context) (*map[string]interface{}, error) {
	state := s.abciClient.GetState()

	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.ProtocolVersion{
			P2P:   version.P2PProtocol,
			Block: version.BlockProtocol,
			App:   0,
		},
		DefaultNodeID: p2p.PubKeyToID(state.Validators.GetProposer().PubKey),
		ListenAddr:    s.config.ListenAddress,
		Network:       state.ChainID,
		Version:       version.TMCoreSemVer,
		Channels:      bytes.HexBytes{0x40, 0x20, 0x21, 0x22, 0x23, 0x30, 0x38, 0x60, 0x61, 0x00},
		Moniker:       "vniccss",
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    "on",
			RPCAddress: s.config.ListenAddress,
		},
	}

	syncInfo := coretypes.SyncInfo{
		LatestBlockHash:     state.LastBlockID.Hash,
		LatestAppHash:       state.AppHash,
		LatestBlockHeight:   state.LastBlockHeight,
		LatestBlockTime:     state.LastBlockTime,
		EarliestBlockHash:   state.LastBlockID.Hash,
		EarliestAppHash:     state.AppHash,
		EarliestBlockHeight: 1,
		EarliestBlockTime:   state.LastBlockTime,
		CatchingUp:          false,
	}

	var validatorInfo CustomValidatorInfo
	if state.Validators.Size() > 0 {
		val := state.Validators.GetProposer()

		// Get the public key bytes and encode as base64
		pubKeyBytes := val.PubKey.Bytes()
		pubKeyBase64 := base64.StdEncoding.EncodeToString(pubKeyBytes)

		validatorInfo = CustomValidatorInfo{
			Address: strings.ToUpper(hex.EncodeToString(val.Address)),
			PubKey: PubKeyObject{
				Type:  "tendermint/PubKeyEd25519",
				Value: pubKeyBase64,
			},
			VotingPower: fmt.Sprintf("%d", val.VotingPower*100),
		}
	}

	// result := map[string]interface{}{
	// 	"node_info":      nodeInfo,
	// 	"sync_info":      syncInfo,
	// 	"validator_info": validatorInfo,
	// }

	// return &map[string]interface{}{
	// 	"node_info":      nodeInfo,
	// 	"sync_info":      syncInfo,
	// 	"validator_info": validatorInfo,
	// }, nil

	return &map[string]interface{}{
		"node_info": map[string]interface{}{
			"channels":    nodeInfo.Channels,
			"id":          nodeInfo.DefaultNodeID,
			"listen_addr": nodeInfo.ListenAddr,
			"moniker":     nodeInfo.Moniker,
			"network":     nodeInfo.Network,
			"other": map[string]interface{}{
				"rpc_address": nodeInfo.Other.RPCAddress,
				"tx_index":    nodeInfo.Other.TxIndex,
			},
			"protocol_version": map[string]interface{}{
				"app":   "0",
				"block": "11",
				"p2p":   "8",
			},
			"version": version.TMCoreSemVer,
		},
		"sync_info": map[string]interface{}{
			"catching_up":           syncInfo.CatchingUp,
			"earliest_app_hash":     syncInfo.EarliestAppHash,
			"earliest_block_hash":   syncInfo.EarliestBlockHash,
			"earliest_block_height": syncInfo.EarliestBlockHeight,
			"earliest_block_time":   syncInfo.EarliestBlockTime,
			"latest_app_hash":       syncInfo.LatestAppHash,
			"latest_block_hash":     syncInfo.LatestBlockHash,
			"latest_block_height":   syncInfo.LatestBlockHeight,
			"latest_block_time":     syncInfo.LatestBlockTime,
		},
		"validator_info": map[string]interface{}{
			"address": validatorInfo.Address,
			"pub_key": map[string]interface{}{
				"type":  validatorInfo.PubKey.Type,
				"value": validatorInfo.PubKey.Value,
			},
			"voting_power": validatorInfo.VotingPower,
		},
	}, nil
}

func (s *Server) genesis(ctx context.Context) (*coretypes.ResultGenesis, error) {
	genDoc := s.abciClient.GetGenesisDoc()
	if genDoc == nil {
		return nil, fmt.Errorf("no genesis document available")
	}

	return &coretypes.ResultGenesis{
		Genesis: genDoc,
	}, nil
}

func (s *Server) validators(ctx context.Context, params interface{}) (*coretypes.ResultValidators, error) {
	var height int64
	var page, perPage int = 1, 30

	if params != nil {
		if paramMap, ok := params.(map[string]interface{}); ok {
			if h, exists := paramMap["height"]; exists {
				if hStr, ok := h.(string); ok {
					if parsed, err := strconv.ParseInt(hStr, 10, 64); err == nil {
						height = parsed
					}
				}
			}
			if p, exists := paramMap["page"]; exists {
				if pStr, ok := p.(string); ok {
					if parsed, err := strconv.Atoi(pStr); err == nil && parsed > 0 {
						page = parsed
					}
				}
			}
			if pp, exists := paramMap["per_page"]; exists {
				if ppStr, ok := pp.(string); ok {
					if parsed, err := strconv.Atoi(ppStr); err == nil && parsed > 0 {
						perPage = parsed
					}
				}
			}
		}
	}

	state := s.abciClient.GetState()
	if height == 0 {
		height = state.LastBlockHeight
	}

	validators := state.Validators.Validators
	total := len(validators)

	start := (page - 1) * perPage
	end := start + perPage
	if start >= total {
		validators = []*types.Validator{}
	} else {
		if end > total {
			end = total
		}
		validators = validators[start:end]
	}

	return &coretypes.ResultValidators{
		BlockHeight: height,
		Validators:  validators,
		Count:       len(validators),
		Total:       total,
	}, nil
}

func (s *Server) block(ctx context.Context, params interface{}) (*coretypes.ResultBlock, error) {
	var height int64

	if params != nil {
		if paramMap, ok := params.(map[string]interface{}); ok {
			if h, exists := paramMap["height"]; exists {
				if hStr, ok := h.(string); ok {
					if parsed, err := strconv.ParseInt(hStr, 10, 64); err == nil {
						height = parsed
					}
				}
			}
		}
	}

	state := s.abciClient.GetState()
	if height == 0 {
		height = state.LastBlockHeight
	}

	block := s.createBlockFromState(state, height)

	return &coretypes.ResultBlock{
		BlockID: state.LastBlockID,
		Block:   block,
	}, nil
}

func (s *Server) blockResults(ctx context.Context, params interface{}) (*coretypes.ResultBlockResults, error) {
	var height int64

	if params != nil {
		if paramMap, ok := params.(map[string]interface{}); ok {
			if h, exists := paramMap["height"]; exists {
				if hStr, ok := h.(string); ok {
					if parsed, err := strconv.ParseInt(hStr, 10, 64); err == nil {
						height = parsed
					}
				}
			}
		}
	}

	state := s.abciClient.GetState()
	if height == 0 {
		height = state.LastBlockHeight
	}

	return &coretypes.ResultBlockResults{
		Height:                height,
		TxsResults:            []*abcitypes.ExecTxResult{},
		FinalizeBlockEvents:   []abcitypes.Event{},
		ValidatorUpdates:      []abcitypes.ValidatorUpdate{},
		ConsensusParamUpdates: nil,
	}, nil
}

func (s *Server) blockchain(ctx context.Context, params interface{}) (*coretypes.ResultBlockchainInfo, error) {
	var minHeight, maxHeight int64 = 1, 20

	if params != nil {
		if paramMap, ok := params.(map[string]interface{}); ok {
			if min, exists := paramMap["minHeight"]; exists {
				if minStr, ok := min.(string); ok {
					if parsed, err := strconv.ParseInt(minStr, 10, 64); err == nil {
						minHeight = parsed
					}
				}
			}
			if max, exists := paramMap["maxHeight"]; exists {
				if maxStr, ok := max.(string); ok {
					if parsed, err := strconv.ParseInt(maxStr, 10, 64); err == nil {
						maxHeight = parsed
					}
				}
			}
		}
	}

	state := s.abciClient.GetState()
	lastHeight := state.LastBlockHeight

	if maxHeight > lastHeight {
		maxHeight = lastHeight
	}
	if minHeight > maxHeight {
		minHeight = maxHeight
	}

	blockMetas := []*types.BlockMeta{}
	for h := maxHeight; h >= minHeight; h-- {
		block := s.createBlockFromState(state, h)
		protoBlock, _ := block.ToProto()
		blockBytes := protoBlock.Size()
		blockMeta := &types.BlockMeta{
			BlockID:   state.LastBlockID,
			Header:    block.Header,
			BlockSize: blockBytes,
			NumTxs:    len(block.Data.Txs),
		}
		blockMetas = append(blockMetas, blockMeta)
	}

	return &coretypes.ResultBlockchainInfo{
		LastHeight: lastHeight,
		BlockMetas: blockMetas,
	}, nil
}

func (s *Server) tx(ctx context.Context, params interface{}) (*coretypes.ResultTx, error) {
	var hash []byte

	if params != nil {
		if paramMap, ok := params.(map[string]interface{}); ok {
			if h, exists := paramMap["hash"]; exists {
				if hStr, ok := h.(string); ok {
					if decoded, err := hex.DecodeString(strings.TrimPrefix(hStr, "0x")); err == nil {
						hash = decoded
					}
				}
			}
		}
	}

	if len(hash) == 0 {
		return nil, fmt.Errorf("hash parameter is required")
	}

	return &coretypes.ResultTx{
		Hash:     hash,
		Height:   1,
		Index:    0,
		TxResult: abcitypes.ExecTxResult{},
		Tx:       []byte{},
		Proof:    types.TxProof{},
	}, nil
}

func (s *Server) txSearch(ctx context.Context, params interface{}) (*coretypes.ResultTxSearch, error) {
	return &coretypes.ResultTxSearch{
		Txs:        []*coretypes.ResultTx{},
		TotalCount: 0,
	}, nil
}

func (s *Server) abciInfo(ctx context.Context) (*coretypes.ResultABCIInfo, error) {
	info, err := s.abciClient.Info(ctx)
	if err != nil {
		return nil, err
	}

	return &coretypes.ResultABCIInfo{
		Response: *info,
	}, nil
}

func (s *Server) abciQuery(ctx context.Context, params interface{}) (*coretypes.ResultABCIQuery, error) {
	var path string
	var data []byte
	var height int64
	var prove bool

	if params != nil {
		if paramMap, ok := params.(map[string]interface{}); ok {
			if p, exists := paramMap["path"]; exists {
				if pStr, ok := p.(string); ok {
					path = pStr
				}
			}
			if d, exists := paramMap["data"]; exists {
				if dStr, ok := d.(string); ok {
					if decoded, err := hex.DecodeString(strings.TrimPrefix(dStr, "0x")); err == nil {
						data = decoded
					}
				}
			}
			if h, exists := paramMap["height"]; exists {
				if hStr, ok := h.(string); ok {
					if parsed, err := strconv.ParseInt(hStr, 10, 64); err == nil {
						height = parsed
					}
				}
			}
			if pr, exists := paramMap["prove"]; exists {
				if prStr, ok := pr.(string); ok {
					prove = prStr == "true"
				}
			}
		}
	}

	response, err := s.abciClient.Query(ctx, data, path, height, prove)
	if err != nil {
		return nil, err
	}

	return &coretypes.ResultABCIQuery{
		Response: *response,
	}, nil
}

func (s *Server) netInfo(ctx context.Context) (*coretypes.ResultNetInfo, error) {
	return &coretypes.ResultNetInfo{
		Listening: true,
		Listeners: []string{s.config.ListenAddress},
		NPeers:    0,
		Peers:     []coretypes.Peer{},
	}, nil
}

func (s *Server) consensusParams(ctx context.Context, params interface{}) (*coretypes.ResultConsensusParams, error) {
	var height int64

	if params != nil {
		if paramMap, ok := params.(map[string]interface{}); ok {
			if h, exists := paramMap["height"]; exists {
				if hStr, ok := h.(string); ok {
					if parsed, err := strconv.ParseInt(hStr, 10, 64); err == nil {
						height = parsed
					}
				}
			}
		}
	}

	state := s.abciClient.GetState()
	if height == 0 {
		height = state.LastBlockHeight
	}

	return &coretypes.ResultConsensusParams{
		BlockHeight:     height,
		ConsensusParams: state.ConsensusParams,
	}, nil
}

func (s *Server) broadcastTxSync(ctx context.Context, params interface{}) (*coretypes.ResultBroadcastTx, error) {
	tx, err := s.extractTx(params)
	if err != nil {
		return nil, err
	}

	response, err := s.abciClient.CheckTx(ctx, tx, 0)
	if err != nil {
		return nil, err
	}

	if response.Code == 0 {
		txObj, err := s.narwhal.SubmitTransaction(tx)
		if err != nil {
			s.logger.Error("Failed to add transaction to mempool",
				zap.String("tx_hash", fmt.Sprintf("%X", types.Tx(tx).Hash())),
				zap.Error(err))
		} else {
			s.logger.Debug("Transaction added to mempool",
				zap.String("tx_hash", fmt.Sprintf("%X", types.Tx(tx).Hash())),
				zap.String("narwhal_tx_hash", txObj.Hash.String()),
				zap.Int("data_size", len(txObj.Data)),
			)
		}
	}

	return &coretypes.ResultBroadcastTx{
		Code:      response.Code,
		Data:      response.Data,
		Log:       response.Log,
		Codespace: response.Codespace,
		Hash:      types.Tx(tx).Hash(),
	}, nil
}

func (s *Server) broadcastTxAsync(ctx context.Context, params interface{}) (*coretypes.ResultBroadcastTx, error) {
	tx, err := s.extractTx(params)
	if err != nil {
		return nil, err
	}

	// For async broadcast, add directly to mempool without CheckTx
	// This is the expected behavior for async broadcasts
	_, err = s.narwhal.SubmitTransaction(tx)
	if err != nil {
		s.logger.Error("Failed to add transaction to mempool (async)",
			zap.String("tx_hash", fmt.Sprintf("%X", types.Tx(tx).Hash())),
			zap.Error(err))
		return &coretypes.ResultBroadcastTx{
			Code: 1,
			Data: []byte{},
			Log:  err.Error(),
			Hash: types.Tx(tx).Hash(),
		}, nil
	}

	s.logger.Debug("Transaction added to mempool (async)",
		zap.String("tx_hash", fmt.Sprintf("%X", types.Tx(tx).Hash())))

	return &coretypes.ResultBroadcastTx{
		Code: 0,
		Data: []byte{},
		Log:  "",
		Hash: types.Tx(tx).Hash(),
	}, nil
}

func (s *Server) broadcastTxCommit(ctx context.Context, params interface{}) (*coretypes.ResultBroadcastTxCommit, error) {
	tx, err := s.extractTx(params)
	if err != nil {
		return nil, err
	}

	checkTxResponse, err := s.abciClient.CheckTx(ctx, tx, 0)
	if err != nil {
		return nil, err
	}

	if checkTxResponse.Code != 0 {
		return &coretypes.ResultBroadcastTxCommit{
			CheckTx: *checkTxResponse,
			Hash:    types.Tx(tx).Hash(),
		}, nil
	}

	state := s.abciClient.GetState()
	nextHeight := state.LastBlockHeight + 1

	finalizeResponse, err := s.abciClient.FinalizeBlock(ctx, nextHeight, time.Now(), [][]byte{tx}, state.LastBlockID.Hash)
	if err != nil {
		return nil, err
	}

	var deliverTxResult abcitypes.ExecTxResult
	if len(finalizeResponse.TxResults) > 0 {
		deliverTxResult = *finalizeResponse.TxResults[0]
	}

	if _, err := s.abciClient.CommitBlock(ctx); err != nil {
		return nil, err
	}

	return &coretypes.ResultBroadcastTxCommit{
		CheckTx:  *checkTxResponse,
		TxResult: deliverTxResult,
		Hash:     types.Tx(tx).Hash(),
		Height:   nextHeight,
	}, nil
}

func (s *Server) subscribe(ctx context.Context, params interface{}) (*coretypes.ResultSubscribe, error) {
	return &coretypes.ResultSubscribe{}, nil
}

func (s *Server) unsubscribe(ctx context.Context, params interface{}) (*coretypes.ResultUnsubscribe, error) {
	return &coretypes.ResultUnsubscribe{}, nil
}

func (s *Server) unsubscribeAll(ctx context.Context, params interface{}) (*coretypes.ResultUnsubscribe, error) {
	return &coretypes.ResultUnsubscribe{}, nil
}

func (s *Server) extractTx(params interface{}) ([]byte, error) {
	if params == nil {
		return nil, fmt.Errorf("tx parameter is required")
	}

	var txStr string
	if paramMap, ok := params.(map[string]interface{}); ok {
		if tx, exists := paramMap["tx"]; exists {
			if txString, ok := tx.(string); ok {
				txStr = txString
			}
		}
	}

	if txStr == "" {
		return nil, fmt.Errorf("tx parameter is required")
	}

	txBytes, err := base64.StdEncoding.DecodeString(txStr)
	if err != nil {
		return nil, err
	}

	return txBytes, nil
}

func (s *Server) createBlockFromState(state state.State, height int64) *types.Block {
	header := types.Header{
		Version:            state.Version.Consensus,
		ChainID:            state.ChainID,
		Height:             height,
		Time:               state.LastBlockTime,
		LastBlockID:        state.LastBlockID,
		LastCommitHash:     merkle.HashFromByteSlices([][]byte{}),
		DataHash:           merkle.HashFromByteSlices([][]byte{}),
		ValidatorsHash:     state.Validators.Hash(),
		NextValidatorsHash: state.NextValidators.Hash(),
		ConsensusHash:      state.ConsensusParams.Hash(),
		AppHash:            state.AppHash,
		LastResultsHash:    merkle.HashFromByteSlices([][]byte{}),
		EvidenceHash:       merkle.HashFromByteSlices([][]byte{}),
		ProposerAddress:    state.Validators.GetProposer().Address,
	}

	return &types.Block{
		Header:   header,
		Data:     types.Data{Txs: []types.Tx{}},
		Evidence: types.EvidenceData{Evidence: types.EvidenceList{}},
		LastCommit: &types.Commit{
			Height:     height - 1,
			Round:      0,
			BlockID:    state.LastBlockID,
			Signatures: []types.CommitSig{},
		},
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("WebSocket upgrade failed", zap.Error(err))
		return
	}
	defer conn.Close()

	for {
		var req JSONRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			s.logger.Error("WebSocket read error", zap.Error(err))
			break
		}

		result, err := s.handleRPCMethod(r.Context(), req.Method, req.Params)

		response := JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      -1,
		}

		if err != nil {
			response.Error = &RPCError{
				Code:    -32603,
				Message: err.Error(),
			}
		} else {
			response.Result = result
		}

		if err := conn.WriteJSON(response); err != nil {
			s.logger.Error("WebSocket write error", zap.Error(err))
			break
		}
	}
}

func (s *Server) sendError(w http.ResponseWriter, id interface{}, code int, message, data string) {
	response := JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      -1,
		Error: &RPCError{
			Code:    code,
			Message: message,
			Data:    data,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (s *Server) Stop() error {
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// Custom VniCCSS endpoint implementations

func (s *Server) narwhalStatus(ctx context.Context) (*ResultNarwhalStatus, error) {
	// In a real implementation, this would query the actual Narwhal DAG
	// For now, we'll return mock data that represents the expected structure
	return &ResultNarwhalStatus{
		DAGHeight:           100,
		PendingCertificates: 5,
		ProcessedBatches:    1500,
		ActiveWorkers: []WorkerInfo{
			{
				ID:               "worker-1",
				Address:          "tcp://127.0.0.1:3001",
				IsOnline:         true,
				LastSeenTime:     time.Now().Add(-1 * time.Second),
				ProcessedBatches: 750,
				Latency:          50 * time.Millisecond,
			},
			{
				ID:               "worker-2",
				Address:          "tcp://127.0.0.1:3002",
				IsOnline:         true,
				LastSeenTime:     time.Now().Add(-2 * time.Second),
				ProcessedBatches: 750,
				Latency:          45 * time.Millisecond,
			},
		},
		MemPoolSize:        250,
		BatchesPerSecond:   15.5,
		NetworkLatency:     30 * time.Millisecond,
		CertificateLatency: 100 * time.Millisecond,
	}, nil
}

func (s *Server) bullsharkStatus(ctx context.Context) (*ResultBullsharkStatus, error) {
	state := s.abciClient.GetState()

	var validators []ValidatorStatus
	if state.Validators.Size() > 0 {
		for _, val := range state.Validators.Validators {
			validators = append(validators, ValidatorStatus{
				Address:       val.Address,
				PubKey:        val.PubKey,
				VotingPower:   val.VotingPower,
				IsProposer:    val.Address.String() == state.Validators.GetProposer().Address.String(),
				LastProposal:  time.Now().Add(-300 * time.Millisecond),
				ProposalCount: 100,
			})
		}
	}

	return &ResultBullsharkStatus{
		ConsensusHeight:          state.LastBlockHeight,
		LastAnchorHeight:         state.LastBlockHeight - 1,
		OrderingLatency:          200 * time.Millisecond,
		TransactionsOrdered:      5000,
		ActiveValidators:         validators,
		BlockInterval:            300 * time.Millisecond,
		AnchorSelectionAlgorithm: "weighted-reputation",
	}, nil
}

func (s *Server) shoalMetrics(ctx context.Context) (*ResultShoalMetrics, error) {
	return &ResultShoalMetrics{
		PerformanceScore: 0.92,
		LatencyReduction: 65.0,
		AdaptiveTimeout:  150 * time.Millisecond,
		LeaderReputation: []ReputationScore{
			{
				NodeID:           "validator-1",
				Score:            0.95,
				SuccessfulBlocks: 950,
				FailedBlocks:     5,
				AverageLatency:   120 * time.Millisecond,
				LastUpdated:      time.Now(),
			},
		},
		CertificatePipeline: PipelineMetrics{
			ActivePipelines:    3,
			CompletedPipelines: 1500,
			AverageLatency:     80 * time.Millisecond,
			ParallelismLevel:   4,
			EfficiencyScore:    0.88,
		},
		FailureRecoveryTime:   500 * time.Millisecond,
		ThroughputImprovement: 75.0,
	}, nil
}

func (s *Server) vniccssInfo(ctx context.Context) (*ResultVniccssInfo, error) {
	startTime := time.Now().Add(-24 * time.Hour) // Mock start time

	narwhal, _ := s.narwhalStatus(ctx)
	bullshark, _ := s.bullsharkStatus(ctx)
	shoal, _ := s.shoalMetrics(ctx)

	return &ResultVniccssInfo{
		Version:         "0.1.2",
		ConsensusEngine: "Narwhal & Bullshark with Shoal",
		Features: []string{
			"DAG Mempool",
			"High Throughput",
			"Byzantine Fault Tolerance",
			"Adaptive Timeouts",
			"Leader Reputation",
			"Certificate Pipelining",
		},
		Narwhal:   *narwhal,
		Bullshark: *bullshark,
		Shoal:     *shoal,
		StartTime: startTime,
		Uptime:    time.Since(startTime),
	}, nil
}

// CosmJS compatibility endpoint implementations

func (s *Server) cosmosAccount(ctx context.Context, params interface{}) (*CosmJSAccount, error) {
	var address string
	if paramMap, ok := params.(map[string]interface{}); ok {
		if addr, exists := paramMap["address"]; exists {
			if addrStr, ok := addr.(string); ok {
				address = addrStr
			}
		}
	}

	if address == "" {
		return nil, fmt.Errorf("address parameter is required")
	}

	// In a real implementation, this would query the actual account state
	return &CosmJSAccount{
		Address:       address,
		AccountNumber: 1,
		Sequence:      0,
		PubKey:        "",
	}, nil
}

func (s *Server) cosmosBalances(ctx context.Context, params interface{}) ([]CosmJSBalance, error) {
	var address string
	if paramMap, ok := params.(map[string]interface{}); ok {
		if addr, exists := paramMap["address"]; exists {
			if addrStr, ok := addr.(string); ok {
				address = addrStr
			}
		}
	}

	if address == "" {
		return nil, fmt.Errorf("address parameter is required")
	}

	// In a real implementation, this would query the actual balance state
	return []CosmJSBalance{
		{
			Denom:  "stake",
			Amount: "1000000",
		},
	}, nil
}

func (s *Server) cosmosTx(ctx context.Context, params interface{}) (*CosmJSTxResponse, error) {
	var hash string
	if paramMap, ok := params.(map[string]interface{}); ok {
		if h, exists := paramMap["hash"]; exists {
			if hashStr, ok := h.(string); ok {
				hash = hashStr
			}
		}
	}

	if hash == "" {
		return nil, fmt.Errorf("hash parameter is required")
	}

	// In a real implementation, this would query the actual transaction
	return &CosmJSTxResponse{
		Height:    "1",
		TxHash:    hash,
		Code:      0,
		Data:      "",
		RawLog:    "[]",
		Logs:      []CosmJSTxLog{},
		Info:      "",
		GasWanted: "100000",
		GasUsed:   "50000",
		Tx:        nil,
		Timestamp: time.Now().Format(time.RFC3339),
		Events:    []CosmJSEvent{},
	}, nil
}

// Dedicated cosmos REST endpoint handlers (not JSON-RPC wrapped)

func (s *Server) handleCosmosAccount(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	address := vars["address"]

	if address == "" {
		http.Error(w, "address parameter is required", http.StatusBadRequest)
		return
	}

	response := map[string]interface{}{
		"account": CosmJSAccount{
			Address:       address,
			AccountNumber: 1,
			Sequence:      0,
			PubKey:        "",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleCosmosBalances(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	address := vars["address"]

	if address == "" {
		http.Error(w, "address parameter is required", http.StatusBadRequest)
		return
	}

	response := map[string]interface{}{
		"balances": []CosmJSBalance{
			{
				Denom:  "stake",
				Amount: "1000000",
			},
		},
		"pagination": map[string]interface{}{
			"next_key": nil,
			"total":    "1",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleCosmosTx(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hash := vars["hash"]

	if hash == "" {
		http.Error(w, "hash parameter is required", http.StatusBadRequest)
		return
	}

	response := map[string]interface{}{
		"tx_response": CosmJSTxResponse{
			Height:    "1",
			TxHash:    hash,
			Code:      0,
			Data:      "",
			RawLog:    "[]",
			Logs:      []CosmJSTxLog{},
			Info:      "",
			GasWanted: "100000",
			GasUsed:   "50000",
			Tx:        nil,
			Timestamp: time.Now().Format(time.RFC3339),
			Events:    []CosmJSEvent{},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
