# 🚀 VniCCSS - Shoal Consensus for Cosmos

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.19-blue.svg)](https://golang.org/)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/vietchain/vniccss)

> ⚠️ **PROTOCOL IMPLEMENTATION - NOT FOR PRODUCTION USE**
> 
> This is an experimental implementation of advanced consensus protocols for research and development purposes. 
> **DO NOT USE IN PRODUCTION ENVIRONMENTS.** The code is provided as-is for educational and research purposes only.

## 📖 Overview

VniCCSS is an advanced consensus engine that implements **Narwhal & Bullshark** consensus protocols enhanced with the **Shoal framework** for the Cosmos SDK ecosystem. It provides high-throughput, low-latency consensus with improved robustness and performance characteristics.

### 🎯 Key Features

- 🔥 **Narwhal & Bullshark Consensus**: DAG-based consensus with high throughput
- ⚡ **Shoal Framework Enhancement**: 40% latency reduction in normal operation, 80% with failures
- 🎛️ **Leader Reputation System**: Performance-based node selection and scoring
- 🔄 **Adaptive Timeouts**: Dynamic timeout adjustment based on network conditions  
- 🚄 **Pipelining Support**: Parallel processing of consensus operations
- 🌐 **CometBFT RPC Compatibility**: Full JSON-RPC 2.0 API compatibility
- 🛡️ **CosmJS Integration**: Ready for Cosmos ecosystem tooling
- 📊 **Performance Monitoring**: Built-in metrics and monitoring capabilities

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     VniCCSS Architecture                   │
├─────────────────────────────────────────────────────────────┤
│  🌐 JSON-RPC 2.0 Server (CometBFT Compatible)             │
├─────────────────────────────────────────────────────────────┤
│  ⚡ Shoal Framework Enhancement Layer                       │
│  ├── 🎯 Leader Reputation System                           │
│  ├── ⏱️  Adaptive Timeout Mechanisms                       │
│  ├── 🚄 Certificate Pipelining                             │
│  └── 📊 Performance Monitoring                             │
├─────────────────────────────────────────────────────────────┤
│  🔥 Bullshark Consensus Engine                             │
│  ├── ⚓ Anchor Selection                                    │
│  ├── 📋 Transaction Ordering                               │
│  └── 🏗️  Block Creation                                     │
├─────────────────────────────────────────────────────────────┤
│  🕸️  Narwhal DAG Mempool                                   │
│  ├── 📦 Batch Management                                   │
│  ├── 📜 Certificate Handling                               │
│  └── 🔗 DAG Construction                                   │
├─────────────────────────────────────────────────────────────┤
│  🔌 ABCI++ Integration                                      │
│  └── 🌍 Cosmos SDK Application                             │
└─────────────────────────────────────────────────────────────┘
```


## 📚 Documentation

### 🔬 Protocol Details

- **Narwhal**: Provides the DAG-based mempool for high-throughput transaction processing
- **Bullshark**: Implements the ordering and finalization layer on top of Narwhal
- **Shoal Framework**: Enhances Bullshark with leader reputation, adaptive timeouts, and pipelining

### 🎛️ Configuration Options

| Flag | Description | Default |
|------|-------------|---------|
| `-rpc` | Consensus engine listen address | `tcp://0.0.0.0:26656` |
| `--app-addr` | ABCI application server address | `tcp://0.0.0.0:26658` |
| `--genesis-file` | Path to genesis file | **Required** |
| `--home-dir` | Home directory for data | `$HOME/.vniccss` |

### 📊 Performance Metrics

The Shoal framework provides significant performance improvements:

- ⚡ **40% latency reduction** in failure-free scenarios
- 🛡️ **80% latency reduction** during failure recovery
- 🚄 **Enhanced throughput** through certificate pipelining
- 🎯 **Improved leader selection** via reputation system


<div align="center">

**⚠️ REMEMBER: This is an experimental protocol implementation - NOT FOR PRODUCTION USE ⚠️**

</div>
