package core

import (
	"context"
	"fmt"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"go.uber.org/zap"

	"github.com/vietchain/vniccss/pkg/abci"
	bullsharktypes "github.com/vietchain/vniccss/pkg/bullshark/types"
	"github.com/vietchain/vniccss/pkg/narwhal/types"
)

// Creator handles the creation of Bullshark blocks and their execution
type Creator struct {
	logger     *zap.Logger
	abciClient *abci.Client
}

// NewCreator creates a new block creator
func NewCreator(abciClient *abci.Client, logger *zap.Logger) *Creator {
	return &Creator{
		logger:     logger,
		abciClient: abciClient,
	}
}

// CreateAndExecuteBlock creates a block from ordering result and executes it via ABCI
func (c *Creator) CreateAndExecuteBlock(
	orderingResult *bullsharktypes.OrderingResult,
) (*ExecutionResult, error) {
	return c.executeBlock(orderingResult.Block, orderingResult.OrderedTransactions)
}

// CreateEmptyBlock creates an empty block (for cases where no transactions are available)
func (c *Creator) CreateEmptyBlock(
	height bullsharktypes.BlockHeight,
	previousBlockHash types.Hash,
	round types.Round,
) (*ExecutionResult, error) {
	// Create empty block with no anchors
	block := bullsharktypes.NewBullsharkBlock(height, previousBlockHash, round, nil)
	return c.executeBlock(block, []*types.Transaction{})
}

// validateBlock performs validation on the block before execution
func (c *Creator) validateBlock(block *bullsharktypes.BullsharkBlock) error {
	if err := block.Validate(); err != nil {
		return fmt.Errorf("basic block validation failed: %w", err)
	}

	// Additional validation logic can be added here
	// For example, checking transaction validity, signature verification, etc.

	return nil
}

// executeBlock executes a block with given transactions via unified ABCI++ lifecycle
func (c *Creator) executeBlock(
	block *bullsharktypes.BullsharkBlock,
	transactions []*types.Transaction,
) (*ExecutionResult, error) {
	startTime := time.Now()

	c.logger.Info("Creating and executing block via unified ABCI++ lifecycle",
		zap.Int64("height", int64(block.Height)),
		zap.Uint64("anchor_round", uint64(block.AnchorRound)),
		zap.Int("transaction_count", len(transactions)),
	)

	// Validate the block before execution
	if err := c.validateBlock(block); err != nil {
		return nil, fmt.Errorf("block validation failed: %w", err)
	}

	// Execute transactions via ABCI
	executionResult, err := c.executeBlockTransactions(block, transactions)
	if err != nil {
		return nil, fmt.Errorf("block execution failed: %w", err)
	}

	// Finalize the block
	finalizedBlock, err := c.finalizeBlock(block, executionResult)
	if err != nil {
		return nil, fmt.Errorf("block finalization failed: %w", err)
	}

	executionTime := time.Since(startTime)

	result := &ExecutionResult{
		Block:              finalizedBlock,
		TransactionResults: executionResult.TransactionResults,
		AppHash:            executionResult.AppHash,
		ExecutionTime:      executionTime,
		GasUsed:            executionResult.GasUsed,
		Success:            true,
	}

	c.logger.Info("Successfully created and executed block",
		zap.Int64("height", int64(finalizedBlock.Height)),
		zap.Int("transactions", finalizedBlock.TransactionCount),
		zap.Duration("execution_time", executionTime),
		zap.String("app_hash", fmt.Sprintf("%X", result.AppHash)),
	)

	return result, nil
}

// executeBlockTransactions executes all transactions in the block via ABCI
func (c *Creator) executeBlockTransactions(
	block *bullsharktypes.BullsharkBlock,
	transactions []*types.Transaction,
) (*ABCIExecutionResult, error) {
	c.logger.Debug("Executing block transactions via ABCI++ lifecycle",
		zap.Int64("height", int64(block.Height)),
		zap.Int("transaction_count", len(transactions)),
	)

	// Convert transactions to byte slices for ABCI
	txBytes := make([][]byte, len(transactions))
	for i, tx := range transactions {
		txBytes[i] = tx.Data
	}

	// Get previous block hash for reference (not used in current implementation)
	_ = block.PreviousBlockHash.Bytes()

	// Step 1: PrepareProposal (as block proposer)
	prepareResp, err := c.abciClient.PrepareProposal(
		context.Background(),
		int64(block.Height),
		txBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("ABCI PrepareProposal failed: %w", err)
	}

	// Use the transactions returned by PrepareProposal (app may modify them)
	finalTxs := prepareResp.Txs
	c.logger.Debug("PrepareProposal completed",
		zap.Int("original_txs", len(txBytes)),
		zap.Int("prepared_txs", len(finalTxs)),
	)

	// Create consistent block hash before ProcessProposal
	// The block hash should be deterministic and include all block content
	blockHash := c.computeBlockHash(block, finalTxs)
	block.BlockHash = blockHash

	c.logger.Debug("Computed consistent block hash",
		zap.String("block_hash", block.BlockHash.String()),
		zap.Int("tx_count", len(finalTxs)),
		zap.Int64("height", int64(block.Height)),
	)

	// Step 2: ProcessProposal (validate the proposal)
	// Get proposer address with proper null checks
	var proposerAddr []byte
	state := c.abciClient.GetState()
	if state.Validators != nil && state.Validators.Size() > 0 {
		proposer := state.Validators.GetProposer()
		if proposer != nil {
			proposerAddr = proposer.Address
		}
	}

	// Fallback to empty address if no proposer available
	if len(proposerAddr) == 0 {
		c.logger.Warn("No proposer available, using empty address")
		proposerAddr = make([]byte, 20) // Standard address length
	}
	processResp, err := c.abciClient.ProcessProposal(
		context.Background(),
		int64(block.Height),
		block.Timestamp,
		finalTxs,
		proposerAddr,
		block.BlockHash.Bytes(), // Use the updated block hash
	)
	if err != nil {
		return nil, fmt.Errorf("ABCI ProcessProposal failed: %w", err)
	}

	// Check if proposal was accepted
	if processResp.Status != abcitypes.ResponseProcessProposal_ACCEPT {
		return nil, fmt.Errorf("proposal rejected by ProcessProposal: status=%v", processResp.Status)
	}

	c.logger.Debug("ProcessProposal accepted", zap.String("status", processResp.Status.String()))

	// Step 3: FinalizeBlock to execute all transactions
	// IMPORTANT: FinalizeBlock.Hash should be the hash of THIS block being finalized
	// ProcessProposal validates it, FinalizeBlock executes with it
	finalizeResp, err := c.abciClient.FinalizeBlock(
		context.Background(),
		int64(block.Height),
		block.Timestamp,
		finalTxs,                // Use the final transactions from PrepareProposal
		block.BlockHash.Bytes(), // Use the same block hash used in ProcessProposal
	)
	if err != nil {
		return nil, fmt.Errorf("ABCI FinalizeBlock failed: %w", err)
	}

	// Parse transaction results from ABCI response
	transactionResults := make([]*TransactionResult, len(transactions))
	totalGasUsed := int64(0)

	for i, tx := range transactions {
		var gasUsed int64
		var success bool
		var log string

		// Check if we have a corresponding result in the ABCI response
		if i < len(finalizeResp.TxResults) {
			txResult := finalizeResp.TxResults[i]
			gasUsed = txResult.GasUsed
			success = txResult.Code == 0
			log = txResult.Log
			if !success && txResult.Info != "" {
				log += " | " + txResult.Info
			}
		} else {
			// Fallback values if no result available
			gasUsed = int64(len(tx.Data) * 10)
			success = true
			log = fmt.Sprintf("Transaction %d executed", i)
		}

		totalGasUsed += gasUsed

		transactionResults[i] = &TransactionResult{
			Index:   i,
			TxHash:  tx.Hash,
			Success: success,
			GasUsed: gasUsed,
			Log:     log,
		}
	}

	// Commit the block to finalize the state
	_, err = c.abciClient.CommitBlock(context.Background())
	if err != nil {
		return nil, fmt.Errorf("ABCI Commit failed: %w", err)
	}

	// Use the app hash from the finalize response
	appHash := finalizeResp.AppHash

	result := &ABCIExecutionResult{
		TransactionResults: transactionResults,
		AppHash:            appHash,
		GasUsed:            totalGasUsed,
		Events:             extractEvents(finalizeResp.Events),
	}

	c.logger.Info("Successfully executed block via ABCI",
		zap.Int("transaction_count", len(transactionResults)),
		zap.Int64("total_gas", totalGasUsed),
		zap.String("app_hash", fmt.Sprintf("%X", appHash)),
		zap.Int("events", len(result.Events)),
	)

	return result, nil
}

// extractEvents converts ABCI events to strings
func extractEvents(abciEvents []abcitypes.Event) []string {
	events := make([]string, 0, len(abciEvents))
	for _, event := range abciEvents {
		eventStr := fmt.Sprintf("%s:", event.Type)
		for _, attr := range event.Attributes {
			eventStr += fmt.Sprintf(" %s=%s", attr.Key, attr.Value)
		}
		events = append(events, eventStr)
	}
	return events
}

// finalizeBlock finalizes the block with execution results
func (c *Creator) finalizeBlock(
	block *bullsharktypes.BullsharkBlock,
	executionResult *ABCIExecutionResult,
) (*bullsharktypes.BullsharkBlock, error) {
	// Update block with execution results
	// In a full implementation, this would update the block hash
	// to include the execution results

	return block, nil
}

// ExecutionResult represents the result of block execution
type ExecutionResult struct {
	Block              *bullsharktypes.BullsharkBlock
	TransactionResults []*TransactionResult
	AppHash            []byte
	ExecutionTime      time.Duration
	GasUsed            int64
	Success            bool
	Error              error
}

// TransactionResult represents the result of executing a single transaction
type TransactionResult struct {
	Index   int
	TxHash  types.Hash
	Success bool
	GasUsed int64
	Log     string
	Error   error
}

// ABCIExecutionResult represents the result from ABCI execution
type ABCIExecutionResult struct {
	TransactionResults []*TransactionResult
	AppHash            []byte
	GasUsed            int64
	Events             []string
}

// computeBlockHash computes a deterministic hash for the block
func (c *Creator) computeBlockHash(
	block *bullsharktypes.BullsharkBlock,
	transactions [][]byte,
) types.Hash {
	// Create a deterministic hash from block content
	// This should include all relevant block data
	hashData := fmt.Sprintf("height=%d,prev=%s,round=%d,time=%d,txcount=%d",
		block.Height,
		block.PreviousBlockHash.String(),
		block.AnchorRound,
		block.Timestamp.Unix(),
		len(transactions),
	)

	// Add transaction hashes to ensure uniqueness
	for i, tx := range transactions {
		hashData += fmt.Sprintf(",tx%d=%X", i, tx)
	}

	// Add anchor information if available
	for i, anchor := range block.SelectedAnchors {
		hashData += fmt.Sprintf(",anchor%d=%s", i, anchor.Certificate.Hash.String())
	}

	return types.NewHash([]byte(hashData))
}

// GetStats returns statistics about block creation
func (c *Creator) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"component": "block_creator",
		"status":    "operational",
	}
}
