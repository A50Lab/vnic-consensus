#!/bin/bash

# Quick Test Script for VNIC Consensus
# This script provides an easy way to test single or multi-node setups

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  VNIC Consensus Quick Test${NC}"
echo -e "${BLUE}========================================${NC}"

# Check if scripts exist
if [ ! -f "scripts/single-node-setup.sh" ] || [ ! -f "scripts/multi-node-setup.sh" ] || [ ! -f "scripts/cleanup.sh" ]; then
    echo -e "${RED}Error: Setup scripts not found. Please run this from the vnic-consensus directory.${NC}"
    exit 1
fi

# Function to monitor logs
monitor_logs() {
    local mode=$1
    echo -e "\n${YELLOW}Monitoring consensus activity (press Ctrl+C to stop)...${NC}\n"
    
    if [ "$mode" = "single" ]; then
        # Single node monitoring
        tail -f vniccss.log | grep --line-buffered -E "Block (created|committed|executed)" | while read line; do
            echo -e "${GREEN}[Node]${NC} $line"
        done
    else
        # Multi-node monitoring
        tail -f vniccss_node*.log | grep --line-buffered -E "Block (created|committed|executed)" | while read line; do
            if [[ $line == *"vniccss_node1.log"* ]]; then
                echo -e "${GREEN}[Node1]${NC} ${line#*==>}"
            elif [[ $line == *"vniccss_node2.log"* ]]; then
                echo -e "${BLUE}[Node2]${NC} ${line#*==>}"
            elif [[ $line == *"vniccss_node3.log"* ]]; then
                echo -e "${YELLOW}[Node3]${NC} ${line#*==>}"
            else
                echo "$line"
            fi
        done
    fi
}

# Main menu
echo -e "\n${YELLOW}Select test mode:${NC}"
echo -e "  1) Single Node Setup"
echo -e "  2) Multi-Node Setup (3 nodes)"
echo -e "  3) Clean Everything"
echo -e "  4) Show Status"
echo -e "  5) Exit"
echo -e "\n${YELLOW}Enter your choice (1-5):${NC}"
read -r choice

case $choice in
    1)
        echo -e "\n${GREEN}Starting Single Node Setup...${NC}"
        echo "y" | ./scripts/single-node-setup.sh
        
        echo -e "\n${YELLOW}Do you want to monitor the logs? (y/n)${NC}"
        read -r monitor
        if [[ "$monitor" =~ ^[Yy]$ ]]; then
            monitor_logs "single"
        fi
        ;;
        
    2)
        echo -e "\n${GREEN}Starting Multi-Node Setup...${NC}"
        echo "y" | ./scripts/multi-node-setup.sh
        
        echo -e "\n${YELLOW}Do you want to monitor the logs? (y/n)${NC}"
        read -r monitor
        if [[ "$monitor" =~ ^[Yy]$ ]]; then
            monitor_logs "multi"
        fi
        ;;
        
    3)
        echo -e "\n${GREEN}Running Cleanup...${NC}"
        echo "y" | ./scripts/cleanup.sh
        ;;
        
    4)
        echo -e "\n${GREEN}Checking Status...${NC}"
        echo -e "\n${YELLOW}Running Processes:${NC}"
        ps aux | grep -E "vnicd|vniccss" | grep -v grep || echo "  No processes running"
        
        echo -e "\n${YELLOW}Data Directories:${NC}"
        [ -d "$HOME/.vnic" ] && echo "  ✓ $HOME/.vnic exists" || echo "  ✗ $HOME/.vnic not found"
        [ -d "$HOME/.vniccss" ] && echo "  ✓ $HOME/.vniccss exists" || echo "  ✗ $HOME/.vniccss not found"
        [ -d "$HOME/.vnic-multi" ] && echo "  ✓ $HOME/.vnic-multi exists" || echo "  ✗ $HOME/.vnic-multi not found"
        
        echo -e "\n${YELLOW}Log Files:${NC}"
        ls -la *.log 2>/dev/null || echo "  No log files found"
        
        # Check latest blocks if running
        if pgrep -f vniccss > /dev/null; then
            echo -e "\n${YELLOW}Latest Blocks:${NC}"
            if [ -f "vniccss.log" ]; then
                tail -3 vniccss.log | grep -E "Block created" || echo "  No recent blocks in single node"
            fi
            if [ -f "vniccss_node1.log" ]; then
                for i in 1 2 3; do
                    if [ -f "vniccss_node$i.log" ]; then
                        echo -e "  Node $i: $(tail -10 vniccss_node$i.log | grep -E 'height\":[0-9]+' | tail -1 || echo 'No blocks yet')"
                    fi
                done
            fi
        fi
        ;;
        
    5)
        echo -e "${GREEN}Exiting...${NC}"
        exit 0
        ;;
        
    *)
        echo -e "${RED}Invalid choice. Exiting...${NC}"
        exit 1
        ;;
esac

echo -e "\n${GREEN}Done!${NC}"