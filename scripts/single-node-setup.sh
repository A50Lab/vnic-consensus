#!/bin/bash

# Single Node Setup Script for VNIC Consensus
# This script sets up and runs a single validator node

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  VNIC Single Node Consensus Setup${NC}"
echo -e "${GREEN}========================================${NC}"

# Configuration
CHAIN_ID="vnic-1"
VNIC_HOME="$HOME/.vnic"
VNICCSS_HOME="$HOME/.vniccss"
VALIDATOR_NAME="validator"
STAKE_AMOUNT="100000000000stake"
GENTX_AMOUNT="1000000stake"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up previous installation...${NC}"
    pkill vnicd 2>/dev/null || true
    pkill vniccss 2>/dev/null || true
    sleep 2
    rm -rf "$VNIC_HOME"
    rm -rf "$VNICCSS_HOME"
    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

# Step 1: Ask for cleanup
echo -e "${YELLOW}Do you want to clean up previous installation? (y/n)${NC}"
read -r response
if [[ "$response" =~ ^[Yy]$ ]]; then
    cleanup
fi

# Step 2: Initialize vnicd
echo -e "\n${YELLOW}Step 1: Initializing vnicd...${NC}"
vnicd init test-validator --chain-id "$CHAIN_ID" --home "$VNIC_HOME"
echo -e "${GREEN}✓ vnicd initialized${NC}"

# Step 3: Add validator key
echo -e "\n${YELLOW}Step 2: Adding validator key...${NC}"
vnicd keys add "$VALIDATOR_NAME" --home "$VNIC_HOME" --keyring-backend test
echo -e "${GREEN}✓ Validator key added${NC}"

# Step 4: Get validator address
echo -e "\n${YELLOW}Step 3: Getting validator address...${NC}"
VALIDATOR_ADDRESS=$(vnicd keys show "$VALIDATOR_NAME" -a --home "$VNIC_HOME" --keyring-backend test)
echo -e "Validator address: ${GREEN}$VALIDATOR_ADDRESS${NC}"

# Step 5: Add genesis account
echo -e "\n${YELLOW}Step 4: Adding genesis account...${NC}"
vnicd genesis add-genesis-account "$VALIDATOR_ADDRESS" "$STAKE_AMOUNT" --home "$VNIC_HOME"
echo -e "${GREEN}✓ Genesis account added${NC}"

# Step 6: Create genesis transaction
echo -e "\n${YELLOW}Step 5: Creating genesis transaction...${NC}"
vnicd genesis gentx "$VALIDATOR_NAME" "$GENTX_AMOUNT" --chain-id "$CHAIN_ID" --home "$VNIC_HOME" --keyring-backend test
echo -e "${GREEN}✓ Genesis transaction created${NC}"

# Step 7: Collect genesis transactions
echo -e "\n${YELLOW}Step 6: Collecting genesis transactions...${NC}"
vnicd genesis collect-gentxs --home "$VNIC_HOME"
echo -e "${GREEN}✓ Genesis transactions collected${NC}"

# Step 8: Update app.toml configuration
echo -e "\n${YELLOW}Step 7: Updating app.toml configuration...${NC}"
APP_TOML="$VNIC_HOME/config/app.toml"
if [ -f "$APP_TOML" ]; then
    # Backup original
    cp "$APP_TOML" "$APP_TOML.bak"
    
    # Update minimum gas prices
    sed -i '' 's/minimum-gas-prices = ""/minimum-gas-prices = "0stake"/' "$APP_TOML"
    
    echo -e "${GREEN}✓ app.toml updated${NC}"
else
    echo -e "${RED}Warning: app.toml not found${NC}"
fi

# Step 9: Start vnicd in background
echo -e "\n${YELLOW}Step 8: Starting vnicd...${NC}"
echo -e "Command: vnicd start --log_level debug --home $VNIC_HOME --with-comet=false --transport=grpc --address tcp://0.0.0.0:26658"

# Create a script to run vnicd
cat > /tmp/run_vnicd.sh << 'EOF'
#!/bin/bash
vnicd start --log_level debug \
  --home ~/.vnic \
  --with-comet=false \
  --transport=grpc \
  --address tcp://0.0.0.0:26658
EOF
chmod +x /tmp/run_vnicd.sh

# Start vnicd in background
nohup /tmp/run_vnicd.sh > vnicd.log 2>&1 &
VNICD_PID=$!
echo -e "vnicd started with PID: ${GREEN}$VNICD_PID${NC}"

# Wait for vnicd to be ready
echo -e "${YELLOW}Waiting for vnicd to be ready...${NC}"
sleep 5

# Check if vnicd is running
if ps -p $VNICD_PID > /dev/null; then
    echo -e "${GREEN}✓ vnicd is running${NC}"
else
    echo -e "${RED}✗ vnicd failed to start. Check vnicd.log for details${NC}"
    exit 1
fi

# Step 10: Build and start vniccss
echo -e "\n${YELLOW}Step 9: Building vniccss...${NC}"
if [ -f "cmd/vniccss/main.go" ]; then
    go build -o vniccss cmd/vniccss/main.go
    echo -e "${GREEN}✓ vniccss built${NC}"
else
    echo -e "${RED}Error: cmd/vniccss/main.go not found. Please run this script from the vnic-consensus directory${NC}"
    exit 1
fi

echo -e "\n${YELLOW}Step 10: Starting vniccss consensus engine...${NC}"
echo -e "Command: ./vniccss --app-addr 127.0.0.1:26658 --genesis-file $VNIC_HOME/config/genesis.json --home-dir $VNICCSS_HOME"

# Start vniccss in background
nohup ./vniccss --app-addr 127.0.0.1:26658 \
  --genesis-file "$VNIC_HOME/config/genesis.json" \
  --home-dir "$VNICCSS_HOME" > vniccss.log 2>&1 &
VNICCSS_PID=$!
echo -e "vniccss started with PID: ${GREEN}$VNICCSS_PID${NC}"

# Wait a bit for vniccss to start
sleep 3

# Check if vniccss is running
if ps -p $VNICCSS_PID > /dev/null; then
    echo -e "${GREEN}✓ vniccss is running${NC}"
else
    echo -e "${RED}✗ vniccss failed to start. Check vniccss.log for details${NC}"
    exit 1
fi

# Step 11: Display status
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  Single Node Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\nNode Information:"
echo -e "  Chain ID: ${GREEN}$CHAIN_ID${NC}"
echo -e "  Validator: ${GREEN}$VALIDATOR_NAME${NC}"
echo -e "  Validator Address: ${GREEN}$VALIDATOR_ADDRESS${NC}"
echo -e "\nServices Running:"
echo -e "  vnicd PID: ${GREEN}$VNICD_PID${NC} (ABCI App on port 26658)"
echo -e "  vniccss PID: ${GREEN}$VNICCSS_PID${NC} (Consensus on port 26657)"
echo -e "\nLog Files:"
echo -e "  vnicd: ${GREEN}vnicd.log${NC}"
echo -e "  vniccss: ${GREEN}vniccss.log${NC}"
echo -e "\n${YELLOW}To monitor logs:${NC}"
echo -e "  tail -f vnicd.log"
echo -e "  tail -f vniccss.log"
echo -e "\n${YELLOW}To stop services:${NC}"
echo -e "  kill $VNICD_PID $VNICCSS_PID"
echo -e "  or: pkill vnicd && pkill vniccss"

# Save PIDs to file for later reference
echo "$VNICD_PID" > .vnicd.pid
echo "$VNICCSS_PID" > .vniccss.pid

echo -e "\n${GREEN}Node is running! Check the logs to see consensus in action.${NC}"