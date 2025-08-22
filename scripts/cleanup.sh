#!/bin/bash

# Cleanup Script for VNIC Consensus
# This script stops all processes and cleans up data directories

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  VNIC Consensus Cleanup Script${NC}"
echo -e "${YELLOW}========================================${NC}"

# Function to kill processes
kill_processes() {
    echo -e "\n${YELLOW}Stopping all vnicd and vniccss processes...${NC}"
    
    # Kill vnicd processes
    if pgrep -f vnicd > /dev/null; then
        echo -e "  Stopping vnicd processes..."
        pkill -f vnicd 2>/dev/null || true
        echo -e "  ${GREEN}✓ vnicd processes stopped${NC}"
    else
        echo -e "  No vnicd processes running"
    fi
    
    # Kill vniccss processes
    if pgrep -f vniccss > /dev/null; then
        echo -e "  Stopping vniccss processes..."
        pkill -f vniccss 2>/dev/null || true
        echo -e "  ${GREEN}✓ vniccss processes stopped${NC}"
    else
        echo -e "  No vniccss processes running"
    fi
    
    # Wait for processes to terminate
    sleep 2
    
    # Force kill if still running
    if pgrep -f "vnicd\|vniccss" > /dev/null; then
        echo -e "  ${YELLOW}Force killing remaining processes...${NC}"
        pkill -9 -f vnicd 2>/dev/null || true
        pkill -9 -f vniccss 2>/dev/null || true
        sleep 1
    fi
}

# Function to clean directories
clean_directories() {
    echo -e "\n${YELLOW}Cleaning up data directories...${NC}"
    
    # Single node directories
    if [ -d "$HOME/.vnic" ]; then
        echo -e "  Removing $HOME/.vnic..."
        rm -rf "$HOME/.vnic"
        echo -e "  ${GREEN}✓ Removed${NC}"
    fi
    
    if [ -d "$HOME/.vniccss" ]; then
        echo -e "  Removing $HOME/.vniccss..."
        rm -rf "$HOME/.vniccss"
        echo -e "  ${GREEN}✓ Removed${NC}"
    fi
    
    # Multi-node directories
    if [ -d "$HOME/.vnic-multi" ]; then
        echo -e "  Removing $HOME/.vnic-multi..."
        rm -rf "$HOME/.vnic-multi"
        echo -e "  ${GREEN}✓ Removed${NC}"
    fi
    
    # Remove individual node directories
    for i in {1..10}; do
        if [ -d "$HOME/.vniccss-node$i" ]; then
            echo -e "  Removing $HOME/.vniccss-node$i..."
            rm -rf "$HOME/.vniccss-node$i"
            echo -e "  ${GREEN}✓ Removed${NC}"
        fi
    done
}

# Function to clean log files
clean_logs() {
    echo -e "\n${YELLOW}Cleaning up log files...${NC}"
    
    # Remove log files in current directory
    for logfile in vnicd*.log vniccss*.log; do
        if [ -f "$logfile" ]; then
            echo -e "  Removing $logfile..."
            rm -f "$logfile"
            echo -e "  ${GREEN}✓ Removed${NC}"
        fi
    done
    
    # Remove PID files
    if [ -f ".vnicd.pid" ] || [ -f ".vniccss.pid" ]; then
        rm -f .vnicd.pid .vniccss.pid
        echo -e "  ${GREEN}✓ PID files removed${NC}"
    fi
    
    # Remove nohup.out if exists
    if [ -f "nohup.out" ]; then
        rm -f nohup.out
        echo -e "  ${GREEN}✓ nohup.out removed${NC}"
    fi
}

# Function to show running processes
show_status() {
    echo -e "\n${YELLOW}Checking for remaining processes...${NC}"
    
    if pgrep -f "vnicd\|vniccss" > /dev/null; then
        echo -e "  ${RED}Warning: Some processes are still running:${NC}"
        ps aux | grep -E "vnicd|vniccss" | grep -v grep || true
    else
        echo -e "  ${GREEN}✓ No vnicd or vniccss processes running${NC}"
    fi
}

# Main cleanup flow
main() {
    echo -e "\n${YELLOW}This will stop all VNIC processes and remove all data directories.${NC}"
    echo -e "${YELLOW}Do you want to continue? (y/n)${NC}"
    read -r response
    
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo -e "${RED}Cleanup cancelled.${NC}"
        exit 0
    fi
    
    # Perform cleanup
    kill_processes
    clean_directories
    clean_logs
    show_status
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}  Cleanup Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "\nYou can now run:"
    echo -e "  ${GREEN}./scripts/single-node-setup.sh${NC} - for single node setup"
    echo -e "  ${GREEN}./scripts/multi-node-setup.sh${NC} - for multi-node setup"
}

# Run main function
main