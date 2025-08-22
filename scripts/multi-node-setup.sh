#!/bin/bash

# Multi-Node Setup Script for VNIC Consensus
# This script sets up and runs multiple validator nodes

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  VNIC Multi-Node Consensus Setup${NC}"
echo -e "${GREEN}========================================${NC}"

# Configuration
CHAIN_ID="vnic-multi"
NUM_NODES=3
BASE_HOME="$HOME/.vnic-multi"
STAKE_AMOUNT="100000000000stake"
GENTX_AMOUNT="1000000stake"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up previous installation...${NC}"
    
    # Kill all processes
    pkill vnicd 2>/dev/null || true
    pkill vniccss 2>/dev/null || true
    sleep 2
    
    # Remove directories
    for i in $(seq 1 $NUM_NODES); do
        rm -rf "$BASE_HOME/node$i"
        rm -rf "$HOME/.vniccss-node$i"
    done
    rm -rf "$BASE_HOME"
    
    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

# Step 1: Ask for cleanup
echo -e "${YELLOW}Do you want to clean up previous installation? (y/n)${NC}"
read -r response
if [[ "$response" =~ ^[Yy]$ ]]; then
    cleanup
fi

# Create base directory
mkdir -p "$BASE_HOME"

echo -e "\n${BLUE}Setting up $NUM_NODES nodes...${NC}"

# Arrays to store node information
declare -a NODE_IDS
declare -a VALIDATOR_ADDRESSES
declare -a NODE_NAMES

# Step 2: Initialize all nodes
echo -e "\n${YELLOW}Step 1: Initializing nodes...${NC}"
for i in $(seq 1 $NUM_NODES); do
    NODE_HOME="$BASE_HOME/node$i"
    NODE_NAME="validator$i"
    NODE_NAMES[$i]=$NODE_NAME
    
    echo -e "  Initializing node$i..."
    vnicd init "node$i" --chain-id "$CHAIN_ID" --home "$NODE_HOME" > /dev/null 2>&1
    
    # Save node ID
    NODE_ID=$(vnicd comet show-node-id --home "$NODE_HOME")
    NODE_IDS[$i]=$NODE_ID
    
    echo -e "  ${GREEN}✓ node$i initialized (ID: $NODE_ID)${NC}"
done

# Step 3: Create keys for all nodes
echo -e "\n${YELLOW}Step 2: Creating validator keys...${NC}"
for i in $(seq 1 $NUM_NODES); do
    NODE_HOME="$BASE_HOME/node$i"
    NODE_NAME="${NODE_NAMES[$i]}"
    
    echo -e "  Creating key for $NODE_NAME..."
    vnicd keys add "$NODE_NAME" --home "$NODE_HOME" --keyring-backend test > /dev/null 2>&1
    
    # Get validator address
    VALIDATOR_ADDRESS=$(vnicd keys show "$NODE_NAME" -a --home "$NODE_HOME" --keyring-backend test)
    VALIDATOR_ADDRESSES[$i]=$VALIDATOR_ADDRESS
    
    echo -e "  ${GREEN}✓ $NODE_NAME key created (Address: $VALIDATOR_ADDRESS)${NC}"
done

# Step 4: Use node1's genesis as the base and add all accounts
echo -e "\n${YELLOW}Step 3: Setting up genesis file...${NC}"
GENESIS_HOME="$BASE_HOME/node1"

# Add all validator accounts to genesis
for i in $(seq 1 $NUM_NODES); do
    echo -e "  Adding account for validator$i..."
    vnicd genesis add-genesis-account "${VALIDATOR_ADDRESSES[$i]}" "$STAKE_AMOUNT" \
        --home "$GENESIS_HOME"
done
echo -e "${GREEN}✓ All accounts added to genesis${NC}"

# Step 5: Create genesis transactions for all nodes
echo -e "\n${YELLOW}Step 4: Creating genesis transactions...${NC}"
for i in $(seq 1 $NUM_NODES); do
    NODE_HOME="$BASE_HOME/node$i"
    NODE_NAME="${NODE_NAMES[$i]}"
    
    echo -e "  Creating gentx for $NODE_NAME..."
    
    # Copy the genesis file to this node first
    if [ $i -ne 1 ]; then
        cp "$GENESIS_HOME/config/genesis.json" "$NODE_HOME/config/genesis.json"
    fi
    
    # Create gentx
    vnicd genesis gentx "$NODE_NAME" "$GENTX_AMOUNT" \
        --chain-id "$CHAIN_ID" \
        --home "$NODE_HOME" \
        --keyring-backend test > /dev/null 2>&1
    
    # Copy gentx back to node1
    if [ $i -ne 1 ]; then
        cp "$NODE_HOME/config/gentx/"*.json "$GENESIS_HOME/config/gentx/"
    fi
    
    echo -e "  ${GREEN}✓ gentx created for $NODE_NAME${NC}"
done

# Step 6: Collect all genesis transactions
echo -e "\n${YELLOW}Step 5: Collecting genesis transactions...${NC}"
vnicd genesis collect-gentxs --home "$GENESIS_HOME"
echo -e "${GREEN}✓ Genesis transactions collected${NC}"

# Step 7: Distribute final genesis to all nodes
echo -e "\n${YELLOW}Step 6: Distributing final genesis file...${NC}"
for i in $(seq 2 $NUM_NODES); do
    NODE_HOME="$BASE_HOME/node$i"
    cp "$GENESIS_HOME/config/genesis.json" "$NODE_HOME/config/genesis.json"
    echo -e "  ${GREEN}✓ Genesis copied to node$i${NC}"
done

# Step 8: Configure nodes with different ports
echo -e "\n${YELLOW}Step 7: Configuring node ports and peers...${NC}"

# Build persistent peers string
PERSISTENT_PEERS=""
for i in $(seq 1 $NUM_NODES); do
    if [ ! -z "$PERSISTENT_PEERS" ]; then
        PERSISTENT_PEERS+=","
    fi
    # P2P port for each node: 26656, 26666, 26676...
    P2P_PORT=$((26656 + (i-1)*10))
    PERSISTENT_PEERS+="${NODE_IDS[$i]}@127.0.0.1:$P2P_PORT"
done

for i in $(seq 1 $NUM_NODES); do
    NODE_HOME="$BASE_HOME/node$i"
    APP_TOML="$NODE_HOME/config/app.toml"
    CONFIG_TOML="$NODE_HOME/config/config.toml"
    
    # Calculate ports for this node
    P2P_PORT=$((26656 + (i-1)*10))
    RPC_PORT=$((26657 + (i-1)*10))
    ABCI_PORT=$((26658 + (i-1)*10))
    PPROF_PORT=$((6060 + (i-1)*10))
    GRPC_PORT=$((9090 + (i-1)))  # Unique gRPC port for each node
    
    # Update app.toml
    if [ -f "$APP_TOML" ]; then
        sed -i '' "s/minimum-gas-prices = \"\"/minimum-gas-prices = \"0stake\"/" "$APP_TOML"
    fi
    
    # Update config.toml for P2P
    if [ -f "$CONFIG_TOML" ]; then
        # Set persistent peers (exclude self)
        PEERS_FOR_NODE=""
        for j in $(seq 1 $NUM_NODES); do
            if [ $j -ne $i ]; then
                if [ ! -z "$PEERS_FOR_NODE" ]; then
                    PEERS_FOR_NODE+=","
                fi
                PEER_P2P_PORT=$((26656 + (j-1)*10))
                PEERS_FOR_NODE+="${NODE_IDS[$j]}@127.0.0.1:$PEER_P2P_PORT"
            fi
        done
        sed -i '' "s/persistent_peers = \"\"/persistent_peers = \"$PEERS_FOR_NODE\"/" "$CONFIG_TOML"
        
        # Set P2P listen address
        sed -i '' "s/laddr = \"tcp:\/\/127.0.0.1:26656\"/laddr = \"tcp:\/\/127.0.0.1:$P2P_PORT\"/" "$CONFIG_TOML"
        
        # Set RPC listen address
        sed -i '' "s/laddr = \"tcp:\/\/127.0.0.1:26657\"/laddr = \"tcp:\/\/127.0.0.1:$RPC_PORT\"/" "$CONFIG_TOML"
        
        # Set pprof listen address
        sed -i '' "s/pprof_laddr = \"localhost:6060\"/pprof_laddr = \"localhost:$PPROF_PORT\"/" "$CONFIG_TOML"
    fi
    
    echo -e "  ${GREEN}✓ node$i configured (P2P: $P2P_PORT, RPC: $RPC_PORT, ABCI: $ABCI_PORT, gRPC: $GRPC_PORT)${NC}"
done

# Step 9: Start all vnicd nodes
echo -e "\n${YELLOW}Step 8: Starting vnicd nodes...${NC}"
for i in $(seq 1 $NUM_NODES); do
    NODE_HOME="$BASE_HOME/node$i"
    ABCI_PORT=$((26658 + (i-1)*10))
    GRPC_PORT=$((9090 + (i-1)))  # Unique gRPC port for each node
    
    echo -e "  Starting vnicd for node$i on port $ABCI_PORT..."
    
    # Create run script for this node
    cat > "/tmp/run_vnicd_node$i.sh" << EOF
#!/bin/bash
vnicd start --log_level info \\
  --home "$NODE_HOME" \\
  --with-comet=false \\
  --transport=grpc \\
  --address tcp://0.0.0.0:$ABCI_PORT \\
  --grpc.address localhost:$GRPC_PORT
EOF
    chmod +x "/tmp/run_vnicd_node$i.sh"
    
    # Start vnicd in background
    nohup "/tmp/run_vnicd_node$i.sh" > "vnicd_node$i.log" 2>&1 &
    VNICD_PID=$!
    echo "vnicd_node$i_pid=$VNICD_PID" >> "$BASE_HOME/pids.txt"
    
    echo -e "  ${GREEN}✓ vnicd node$i started (PID: $VNICD_PID)${NC}"
done

# Wait for vnicd nodes to be ready
echo -e "${YELLOW}Waiting for vnicd nodes to be ready...${NC}"
sleep 5

# Step 10: Build vniccss if needed
echo -e "\n${YELLOW}Step 9: Building vniccss...${NC}"
if [ ! -f "vniccss" ]; then
    if [ -f "cmd/vniccss/main.go" ]; then
        go build -o vniccss cmd/vniccss/main.go
        echo -e "${GREEN}✓ vniccss built${NC}"
    else
        echo -e "${RED}Error: cmd/vniccss/main.go not found${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✓ vniccss already built${NC}"
fi

# Step 11: Start vniccss consensus for all nodes
echo -e "\n${YELLOW}Step 10: Starting vniccss consensus engines...${NC}"
for i in $(seq 1 $NUM_NODES); do
    NODE_HOME="$BASE_HOME/node$i"
    VNICCSS_HOME="$HOME/.vniccss-node$i"
    ABCI_PORT=$((26658 + (i-1)*10))
    CONSENSUS_PORT=$((26657 + (i-1)*10))
    P2P_PORT=$((9000 + (i-1)*10))
    
    echo -e "  Starting vniccss for node$i..."
    
    # Start vniccss in background with unique ports
    nohup ./vniccss \
        --app-addr "127.0.0.1:$ABCI_PORT" \
        --genesis-file "$NODE_HOME/config/genesis.json" \
        --home-dir "$VNICCSS_HOME" \
        --rpc-listen-addr "tcp://0.0.0.0:$CONSENSUS_PORT" \
        --p2p-listen-addr "/ip4/127.0.0.1/tcp/$P2P_PORT" > "vniccss_node$i.log" 2>&1 &
    VNICCSS_PID=$!
    echo "vniccss_node$i_pid=$VNICCSS_PID" >> "$BASE_HOME/pids.txt"
    
    echo -e "  ${GREEN}✓ vniccss node$i started (PID: $VNICCSS_PID, Consensus: $CONSENSUS_PORT, P2P: $P2P_PORT)${NC}"
done

# Wait for consensus to start
sleep 3

# Step 12: Display summary
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  Multi-Node Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\nCluster Information:"
echo -e "  Chain ID: ${GREEN}$CHAIN_ID${NC}"
echo -e "  Number of nodes: ${GREEN}$NUM_NODES${NC}"
echo -e "\nNode Details:"
for i in $(seq 1 $NUM_NODES); do
    echo -e "\n  ${BLUE}Node $i:${NC}"
    echo -e "    Validator: ${GREEN}${NODE_NAMES[$i]}${NC}"
    echo -e "    Address: ${GREEN}${VALIDATOR_ADDRESSES[$i]}${NC}"
    echo -e "    Node ID: ${GREEN}${NODE_IDS[$i]}${NC}"
    echo -e "    ABCI Port: ${GREEN}$((26658 + (i-1)*10))${NC}"
    echo -e "    Consensus Port: ${GREEN}$((26657 + (i-1)*10))${NC}"
    echo -e "    P2P Port: ${GREEN}$((9000 + (i-1)*10))${NC}"
    echo -e "    gRPC Port: ${GREEN}$((9090 + (i-1)))${NC}"
done

echo -e "\n${YELLOW}Log Files:${NC}"
for i in $(seq 1 $NUM_NODES); do
    echo -e "  Node $i: vnicd_node$i.log, vniccss_node$i.log"
done

echo -e "\n${YELLOW}To monitor logs:${NC}"
echo -e "  tail -f vniccss_node1.log"
echo -e "  tail -f vniccss_node2.log"
echo -e "  tail -f vniccss_node3.log"

echo -e "\n${YELLOW}To check consensus:${NC}"
echo -e "  grep -i 'block committed\\|block created' vniccss_node*.log"

echo -e "\n${YELLOW}To stop all services:${NC}"
echo -e "  pkill vnicd && pkill vniccss"

echo -e "\n${GREEN}Multi-node cluster is running! Check the logs to see distributed consensus in action.${NC}"