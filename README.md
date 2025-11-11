# Raft Go Implementation

Production-ready Raft consensus library for Go with leader election, log replication, WAL durability, snapshots, and pluggable transports, plus a simulator for chaos testing.

A complete implementation of the Raft consensus algorithm in Go, based on the [Raft paper](https://raft.github.io/raft.pdf) by Diego Ongaro and John Ousterhout.

## Overview

This implementation provides a production-ready Raft consensus library with leader election, log replication, persistence, and snapshotting. The core algorithm follows the Raft paper specification with randomized election timeouts (150-300ms), heartbeat intervals (50ms), and strict persistence guarantees where all state changes are persisted before RPC replies.

Key features include:

- **Leader Election**: Randomized timeouts ensure at most one leader per term
- **Log Replication**: AppendEntries RPC with consistency checks and log truncation
- **Persistence**: Write-ahead log (WAL) with fsync guarantees for durability
- **Snapshots**: Log compaction and InstallSnapshot RPC for state recovery
- **Network Transport**: In-process and net/rpc transports for testing and deployment
- **Testing**: Simulator with partition injection, message delays, and crash/restart testing

The implementation uses binary encoding with gob for log entries and enforces the Raft commit rule where only entries from the current term can be committed once a majority of servers have replicated them.

## Building

```bash
go build ./...
```

Build binaries:

```bash
go build -o raftd ./cmd/raftd
go build -o raftctl ./cmd/raftctl
```

## Running

Start a Raft node:

```bash
./raftd -id node1 -data-dir /tmp/raft-node1 -peers "localhost:8081,localhost:8082" -address :8080
```

Use raftctl to inspect state:

```bash
./raftctl -address localhost:8080 -command state
./raftctl -address localhost:8080 -command leader
./raftctl -address localhost:8080 -command start -key foo -value bar
```