package abci

import (
	"bytes"
	"context"
	"fmt"
	"time"

	comet_abciclient "github.com/cometbft/cometbft/abci/client"
	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
	"go.uber.org/zap"
)

const ABCITimeout = 2 * time.Second

type Client struct {
	client     comet_abciclient.Client
	genesisDoc *types.GenesisDoc
	curState   state.State
	logger     *zap.Logger
}

func NewClient(client comet_abciclient.Client, genesisDoc *types.GenesisDoc, logger *zap.Logger) *Client {
	return &Client{
		client:     client,
		genesisDoc: genesisDoc,
		logger:     logger,
	}
}

func (c *Client) InitChain(ctx context.Context) error {
	c.logger.Info("Initializing chain via ABCI")

	// Create genesis state
	genState, err := state.MakeGenesisState(c.genesisDoc)
	if err != nil {
		return fmt.Errorf("failed to create genesis state: %w", err)
	}

	c.curState = genState

	c.logger.Info("Genesis state initialized",
		zap.String("chain_id", genState.ChainID),
		zap.Int64("initial_height", genState.InitialHeight),
		zap.Int64("last_block_height", genState.LastBlockHeight),
		zap.Int("validator_count", genState.Validators.Size()),
		zap.String("app_hash", fmt.Sprintf("%X", genState.AppHash)),
	)

	// Create InitChain request
	consensusParams := genState.ConsensusParams.ToProto()

	genesisValidators := c.genesisDoc.Validators
	validators := make([]*types.Validator, len(genesisValidators))
	for i, val := range genesisValidators {
		validators[i] = types.NewValidator(val.PubKey, val.Power)
	}
	validatorSet := types.NewValidatorSet(validators)
	nextVals := types.TM2PB.ValidatorUpdates(validatorSet)

	initChainRequest := &abcitypes.RequestInitChain{
		Validators:      nextVals,
		InitialHeight:   genState.InitialHeight,
		Time:            c.genesisDoc.GenesisTime,
		ChainId:         genState.ChainID,
		ConsensusParams: &consensusParams,
		AppStateBytes:   c.genesisDoc.AppState,
	}

	// Send InitChain request
	timeoutCtx, cancel := context.WithTimeout(ctx, ABCITimeout)
	defer cancel()

	response, err := c.client.InitChain(timeoutCtx, initChainRequest)
	if err != nil {
		return fmt.Errorf("failed to send InitChain: %w", err)
	}

	// Update state based on response
	if err := c.updateStateFromInit(response); err != nil {
		return fmt.Errorf("failed to update state from InitChain response: %w", err)
	}

	c.logger.Info("Chain initialized successfully",
		zap.String("chain_id", c.curState.ChainID),
		zap.Int64("initial_height", c.curState.InitialHeight),
		zap.Int64("last_block_height", c.curState.LastBlockHeight),
		zap.Int("validators", c.curState.Validators.Size()),
		zap.String("app_hash", fmt.Sprintf("%X", c.curState.AppHash)),
	)

	return nil
}

func (c *Client) updateStateFromInit(res *abcitypes.ResponseInitChain) error {
	// Update app hash if provided
	if len(res.AppHash) > 0 {
		c.curState.AppHash = res.AppHash
	}

	// Update validators if provided
	if len(res.Validators) > 0 {
		validators, err := types.PB2TM.ValidatorUpdates(res.Validators)
		if err != nil {
			return fmt.Errorf("failed to convert validator updates: %w", err)
		}

		c.curState.LastValidators = types.NewValidatorSet(validators)
		c.curState.Validators = types.NewValidatorSet(validators)
		c.curState.NextValidators = types.NewValidatorSet(validators).CopyIncrementProposerPriority(1)
	}

	// Update consensus params if provided
	if res.ConsensusParams != nil {
		c.curState.ConsensusParams = c.curState.ConsensusParams.Update(res.ConsensusParams)
		c.curState.Version.Consensus.App = c.curState.ConsensusParams.Version.App
	}

	return nil
}

func (c *Client) Info(ctx context.Context) (*abcitypes.ResponseInfo, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, ABCITimeout)
	defer cancel()

	return c.client.Info(timeoutCtx, &abcitypes.RequestInfo{})
}

func (c *Client) CheckTx(ctx context.Context, tx []byte, checkType abcitypes.CheckTxType) (*abcitypes.ResponseCheckTx, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, ABCITimeout)
	defer cancel()

	return c.client.CheckTx(timeoutCtx, &abcitypes.RequestCheckTx{
		Tx:   tx,
		Type: checkType,
	})
}

func (c *Client) Query(ctx context.Context, data []byte, path string, height int64, prove bool) (*abcitypes.ResponseQuery, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, ABCITimeout)
	defer cancel()

	return c.client.Query(timeoutCtx, &abcitypes.RequestQuery{
		Data:   data,
		Path:   path,
		Height: height,
		Prove:  prove,
	})
}

func (c *Client) Commit(ctx context.Context) (*abcitypes.ResponseCommit, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, ABCITimeout)
	defer cancel()

	return c.client.Commit(timeoutCtx, &abcitypes.RequestCommit{})
}

func (c *Client) Stop() {
	if c.client != nil {
		c.client.Stop()
	}
}

func (c *Client) GetState() state.State {
	return c.curState
}

func (c *Client) GetGenesisDoc() *types.GenesisDoc {
	return c.genesisDoc
}

// PrepareProposal prepares a proposal for the block (ABCI++ method)
func (c *Client) PrepareProposal(ctx context.Context, height int64, txs [][]byte) (*abcitypes.ResponsePrepareProposal, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, ABCITimeout)
	defer cancel()

	req := &abcitypes.RequestPrepareProposal{
		Height: height,
		Txs:    txs,
	}

	resp, err := c.client.PrepareProposal(timeoutCtx, req)
	if err != nil {
		c.logger.Error("ABCI PrepareProposal failed", zap.Error(err))
		return nil, err
	}

	c.logger.Debug("ABCI PrepareProposal response",
		zap.Int("output_tx_count", len(resp.Txs)),
		zap.Strings("output_tx_hashes", func() []string {
			hashes := make([]string, len(resp.Txs))
			for i, tx := range resp.Txs {
				hashes[i] = fmt.Sprintf("%X", tx)
			}
			return hashes
		}()),
	)

	return resp, nil
}

// FinalizeBlock finalizes a block with transactions (ABCI++ method)
func (c *Client) FinalizeBlock(ctx context.Context, height int64, timestamp time.Time, txs [][]byte, lastBlockHash []byte) (*abcitypes.ResponseFinalizeBlock, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, ABCITimeout)
	defer cancel()

	// Create proper request with all required fields
	req := &abcitypes.RequestFinalizeBlock{
		Hash:   lastBlockHash,
		Height: height,
		Time:   timestamp,
		Txs:    txs,
		// Add proposer address from current validator set
		ProposerAddress: c.curState.Validators.GetProposer().Address,
		// Add commit info for previous block
		DecidedLastCommit: abcitypes.CommitInfo{
			Round: 0, // Our consensus uses round 0
			Votes: c.buildVoteInfo(),
		},
	}

	resp, err := c.client.FinalizeBlock(timeoutCtx, req)
	if err != nil {
		return nil, err
	}

	// Update state after finalization but before commit
	if err := c.updateStateFromFinalize(resp, height, timestamp); err != nil {
		return nil, fmt.Errorf("failed to update state after FinalizeBlock: %w", err)
	}

	return resp, nil
}

// ProcessProposal validates a block proposal (ABCI++ method)
func (c *Client) ProcessProposal(ctx context.Context, height int64, timestamp time.Time, txs [][]byte, proposerAddress []byte, hash []byte) (*abcitypes.ResponseProcessProposal, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, ABCITimeout)
	defer cancel()

	req := &abcitypes.RequestProcessProposal{
		Hash:            hash,
		Height:          height,
		Time:            timestamp,
		Txs:             txs,
		ProposerAddress: proposerAddress,
	}

	return c.client.ProcessProposal(timeoutCtx, req)
}

// buildVoteInfo creates vote info for the current validator set
func (c *Client) buildVoteInfo() []abcitypes.VoteInfo {
	votes := make([]abcitypes.VoteInfo, 0, c.curState.Validators.Size())

	// For single node setup, create vote for our validator
	if c.curState.Validators.Size() > 0 {
		val := c.curState.Validators.Validators[0]
		vote := abcitypes.VoteInfo{
			Validator: abcitypes.Validator{
				Address: val.Address,
				Power:   val.VotingPower,
			},
			// Use numeric value for commit (BlockIDFlagCommit = 2)
			BlockIdFlag: 2,
		}
		votes = append(votes, vote)
	}

	return votes
}

// updateStateFromFinalize updates the consensus state after block finalization
func (c *Client) updateStateFromFinalize(resp *abcitypes.ResponseFinalizeBlock, height int64, timestamp time.Time) error {
	// Update basic state
	c.curState.LastBlockHeight = height
	c.curState.LastBlockTime = timestamp

	// Update app hash
	if len(resp.AppHash) > 0 {
		c.curState.AppHash = resp.AppHash
	}

	// Update validators if provided
	if len(resp.ValidatorUpdates) > 0 {
		validators, err := types.PB2TM.ValidatorUpdates(resp.ValidatorUpdates)
		if err != nil {
			return fmt.Errorf("failed to convert validator updates: %w", err)
		}

		// Rotate validator sets
		c.curState.LastValidators = c.curState.Validators.Copy()
		c.curState.Validators = c.curState.NextValidators.Copy()

		// Apply updates to next validators
		nextVals := c.curState.NextValidators.Copy()
		nextVals.UpdateWithChangeSet(validators)
		c.curState.NextValidators = nextVals
	}

	// Update consensus params if provided
	if resp.ConsensusParamUpdates != nil {
		c.curState.ConsensusParams = c.curState.ConsensusParams.Update(resp.ConsensusParamUpdates)
	}

	return nil
}

// CommitBlock commits the current block
func (c *Client) CommitBlock(ctx context.Context) (*abcitypes.ResponseCommit, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, ABCITimeout)
	defer cancel()

	resp, err := c.client.Commit(timeoutCtx, &abcitypes.RequestCommit{})
	if err != nil {
		return nil, err
	}

	// Create proper block hash from app hash
	// In CometBFT, the block hash is computed from multiple components
	// For our implementation, we'll use the app hash as the block content hash
	blockHash := c.curState.AppHash
	if len(blockHash) == 0 {
		// Fallback to a deterministic hash if no app hash
		blockHash = []byte(fmt.Sprintf("block_%d", c.curState.LastBlockHeight))
	}

	// Update the last block ID with proper hash
	c.curState.LastBlockID = types.BlockID{
		Hash: blockHash,
		PartSetHeader: types.PartSetHeader{
			Total: 1,
			Hash:  blockHash,
		},
	}

	c.logger.Info("Block committed successfully",
		zap.Int64("height", c.curState.LastBlockHeight),
		zap.String("app_hash", fmt.Sprintf("%X", c.curState.AppHash)),
		zap.String("block_hash", fmt.Sprintf("%X", blockHash)),
		zap.Int64("retain_height", resp.RetainHeight),
	)

	return resp, nil
}

// SyncWithChainState synchronizes the ABCI client state with an existing chain
func (c *Client) SyncWithChainState(info *abcitypes.ResponseInfo) error {
	c.logger.Info("Syncing with existing chain state",
		zap.Int64("chain_height", info.LastBlockHeight),
		zap.String("app_hash", fmt.Sprintf("%X", info.LastBlockAppHash)),
		zap.String("app_version", info.Version),
	)

	// Validate that we can sync with this state
	if info.LastBlockHeight < 0 {
		return fmt.Errorf("invalid chain height: %d", info.LastBlockHeight)
	}

	// Ensure genesis compatibility
	if c.genesisDoc.ChainID == "" {
		return fmt.Errorf("genesis document has no chain ID")
	}

	// Start by creating a complete genesis state to ensure all fields are initialized
	genState, err := state.MakeGenesisState(c.genesisDoc)
	if err != nil {
		return fmt.Errorf("failed to create genesis state for sync: %w", err)
	}

	// Use genesis state as base and update with current chain info
	c.curState = genState

	// Update fields that differ from genesis
	c.curState.LastBlockHeight = info.LastBlockHeight
	c.curState.AppHash = info.LastBlockAppHash

	// Create a block ID from the app hash (since we don't have the actual block hash)
	blockHash := info.LastBlockAppHash
	if len(blockHash) == 0 {
		// Fallback if no app hash available
		blockHash = []byte(fmt.Sprintf("synced_block_%d", info.LastBlockHeight))
	}

	c.curState.LastBlockID = types.BlockID{
		Hash: blockHash,
		PartSetHeader: types.PartSetHeader{
			Total: 1,
			Hash:  blockHash,
		},
	}

	// Set a reasonable last block time (current time minus some offset)
	// In a real implementation, this should be queried from the app
	c.curState.LastBlockTime = time.Now().Add(-time.Minute)

	// Initialize genesis state for other fields if not already set
	if c.curState.ChainID == "" {
		c.curState.ChainID = c.genesisDoc.ChainID
	}

	if c.curState.Validators == nil {
		// Create validator set from genesis for single node
		genesisValidators := c.genesisDoc.Validators
		validators := make([]*types.Validator, len(genesisValidators))
		for i, val := range genesisValidators {
			validators[i] = types.NewValidator(val.PubKey, val.Power)
		}
		validatorSet := types.NewValidatorSet(validators)

		c.curState.LastValidators = validatorSet.Copy()
		c.curState.Validators = validatorSet.Copy()
		c.curState.NextValidators = validatorSet.Copy()
	}

	if c.curState.ConsensusParams.Block.MaxBytes == 0 {
		// Set default consensus params from genesis
		genState, err := state.MakeGenesisState(c.genesisDoc)
		if err != nil {
			return fmt.Errorf("failed to create genesis state for sync: %w", err)
		}
		c.curState.ConsensusParams = genState.ConsensusParams
		c.curState.Version = genState.Version
	}

	// Set initial height if not set
	if c.curState.InitialHeight == 0 {
		c.curState.InitialHeight = c.genesisDoc.InitialHeight
	}

	c.logger.Info("Successfully synced with chain state",
		zap.String("chain_id", c.curState.ChainID),
		zap.Int64("initial_height", c.curState.InitialHeight),
		zap.Int64("last_block_height", c.curState.LastBlockHeight),
		zap.String("synced_block_hash", fmt.Sprintf("%X", blockHash)),
		zap.Time("last_block_time", c.curState.LastBlockTime),
		zap.Int("validators", c.curState.Validators.Size()),
		zap.String("app_hash", fmt.Sprintf("%X", c.curState.AppHash)),
		zap.Int64("last_height_validators_changed", c.curState.LastHeightValidatorsChanged),
		zap.Int64("last_height_consensus_params_changed", c.curState.LastHeightConsensusParamsChanged),
	)

	return nil
}

// ValidateChainContinuity validates that we can safely continue from current state
func (c *Client) ValidateChainContinuity(ctx context.Context) error {
	// Get current app info
	info, err := c.Info(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current app info: %w", err)
	}

	// Verify our state matches the app state
	if c.curState.LastBlockHeight != info.LastBlockHeight {
		c.logger.Warn("Height mismatch detected",
			zap.Int64("consensus_height", c.curState.LastBlockHeight),
			zap.Int64("app_height", info.LastBlockHeight),
		)

		// If app is ahead, we need to sync
		if info.LastBlockHeight > c.curState.LastBlockHeight {
			c.logger.Info("App is ahead, updating consensus state")
			return c.SyncWithChainState(info)
		}

		// If consensus is ahead, this is an error
		if c.curState.LastBlockHeight > info.LastBlockHeight {
			return fmt.Errorf("consensus ahead of app: consensus=%d, app=%d",
				c.curState.LastBlockHeight, info.LastBlockHeight)
		}
	}

	// Verify app hash matches if both are set
	if len(c.curState.AppHash) > 0 && len(info.LastBlockAppHash) > 0 {
		if !bytes.Equal(c.curState.AppHash, info.LastBlockAppHash) {
			c.logger.Warn("App hash mismatch detected",
				zap.String("consensus_hash", fmt.Sprintf("%X", c.curState.AppHash)),
				zap.String("app_hash", fmt.Sprintf("%X", info.LastBlockAppHash)),
			)
			// Update our hash to match the app
			c.curState.AppHash = info.LastBlockAppHash
		}
	}

	c.logger.Info("Chain continuity validated successfully",
		zap.Int64("height", c.curState.LastBlockHeight),
		zap.String("app_hash", fmt.Sprintf("%X", c.curState.AppHash)),
	)

	return nil
}

// stateToGenesisDoc creates a GenesisDoc from the current state
func (c *Client) stateToGenesisDoc() *types.GenesisDoc {
	if c.genesisDoc == nil {
		return nil
	}

	// Copy the original genesis doc as a base
	newDoc := *c.genesisDoc
	// Update fields from current state
	newDoc.ChainID = c.curState.ChainID
	newDoc.InitialHeight = c.curState.LastBlockHeight
	newDoc.GenesisTime = c.curState.LastBlockTime
	newDoc.ConsensusParams = &c.curState.ConsensusParams
	newDoc.AppHash = c.curState.AppHash

	// Update validators from current state
	if c.curState.Validators != nil {
		validators := c.curState.Validators.Validators // slice of *types.Validator
		genesisVals := make([]types.GenesisValidator, len(validators))
		for i, v := range validators {
			genesisVals[i] = types.GenesisValidator{
				Address: v.Address,
				PubKey:  v.PubKey,
				Power:   v.VotingPower,
			}
		}
		newDoc.Validators = genesisVals
	}
	// Optionally update AppHash or AppState if needed (AppStateBytes is opaque)
	return &newDoc
}

// ExportStateToGenesisFile exports the current state as a GenesisDoc to a JSON file
func (c *Client) ExportStateToGenesisFile(path string) error {
	genDoc := c.stateToGenesisDoc()
	if genDoc == nil {
		return fmt.Errorf("no genesis doc to export")
	}
	genDoc.SaveAs(path)
	c.logger.Info("Exported chain state to genesis file", zap.String("path", path))
	return nil
}

func LoadGenesisDocFromFile(path string) (*types.GenesisDoc, error) {
	return types.GenesisDocFromFile(path)
}
