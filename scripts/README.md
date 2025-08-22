# VNIC Consensus Setup Scripts

This directory contains automated setup and management scripts for running VNIC consensus nodes.

## Prerequisites

- Go 1.21+ installed
- `vnicd` binary available in PATH
- `vniccss` built (will be built automatically if not present)

## Available Scripts

### 1. Single Node Setup (`single-node-setup.sh`)
Sets up and runs a single validator node for testing.

```bash
./scripts/single-node-setup.sh
```

**What it does:**
- Initializes a single validator with chain ID `vnic-1`
- Creates validator keys and genesis account
- Starts `vnicd` (ABCI app) on port 26658
- Starts `vniccss` (consensus engine) on port 26657
- Produces blocks automatically every 300ms

**Directories created:**
- `~/.vnic` - vnicd data directory
- `~/.vniccss` - vniccss consensus data

### 2. Multi-Node Setup (`multi-node-setup.sh`)
Sets up and runs a 3-node validator cluster.

```bash
./scripts/multi-node-setup.sh
```

**What it does:**
- Initializes 3 validators with chain ID `vnic-multi`
- Configures unique ports for each node
- Sets up P2P connections between nodes
- Starts all nodes in parallel

**Port Configuration:**
| Node | ABCI Port | Consensus Port | P2P Port |
|------|-----------|----------------|----------|
| Node1| 26658     | 26657          | 9000     |
| Node2| 26668     | 26667          | 9010     |
| Node3| 26678     | 26677          | 9020     |

**Directories created:**
- `~/.vnic-multi/node[1-3]` - vnicd data directories
- `~/.vniccss-node[1-3]` - vniccss consensus data

### 3. Cleanup Script (`cleanup.sh`)
Stops all processes and removes all data directories.

```bash
./scripts/cleanup.sh
```

**What it does:**
- Stops all `vnicd` and `vniccss` processes
- Removes all data directories
- Cleans up log files
- Provides status check

### 4. Quick Test Script (`quick-test.sh`)
Interactive menu for easy testing and monitoring.

```bash
./scripts/quick-test.sh
```

**Menu Options:**
1. Single Node Setup - Run single node with optional log monitoring
2. Multi-Node Setup - Run 3-node cluster with optional log monitoring
3. Clean Everything - Run cleanup script
4. Show Status - Display current system status
5. Exit

## Log Files

- **Single Node:**
  - `vnicd.log` - ABCI application logs
  - `vniccss.log` - Consensus engine logs

- **Multi-Node:**
  - `vnicd_node[1-3].log` - ABCI application logs per node
  - `vniccss_node[1-3].log` - Consensus engine logs per node

## Monitoring

### Single Node
```bash
tail -f vniccss.log
```

### Multi-Node
```bash
# Monitor all nodes
tail -f vniccss_node*.log

# Monitor specific node
tail -f vniccss_node1.log

# Check block production
grep -i "block created" vniccss_node*.log
```

## Troubleshooting

### Processes won't stop
```bash
pkill -9 vnicd
pkill -9 vniccss
```

### Port already in use
Check which process is using the port:
```bash
lsof -i :26657
```

### Reset everything
```bash
./scripts/cleanup.sh
```

## Development Workflow

1. **Clean start:**
   ```bash
   ./scripts/cleanup.sh
   ```

2. **Test single node:**
   ```bash
   ./scripts/single-node-setup.sh
   ```

3. **Test multi-node:**
   ```bash
   ./scripts/cleanup.sh
   ./scripts/multi-node-setup.sh
   ```

4. **Interactive testing:**
   ```bash
   ./scripts/quick-test.sh
   ```

## Notes

- Scripts automatically build `vniccss` if not present
- All scripts prompt for cleanup confirmation
- Logs are created in the current directory
- Use `quick-test.sh` for the easiest testing experience