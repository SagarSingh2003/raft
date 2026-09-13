# Raft KV

A distributed key-value store built in **Go** using the **Raft consensus algorithm**.

The project is an implementation-focused exploration of distributed systems, covering leader election, log replication, fault handling, commitment of replicated entries, state-machine application, and write-ahead logging.

> **Status:** Work in progress — the core Raft implementation is actively being developed and tested.

## Architecture

```text
                         ┌──────────────────┐
                         │      Client      │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │   Raft Leader    │
                         └────────┬─────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    │             │             │
                    ▼             ▼             ▼
               ┌─────────┐   ┌─────────┐   ┌─────────┐
               │Follower │   │Follower │   │Follower │
               │  Node   │   │  Node   │   │  Node   │
               └─────────┘   └─────────┘   └─────────┘
                    │             │             │
                    └─────────────┼─────────────┘
                                  ▼
                         ┌──────────────────┐
                         │  Replicated Log  │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │   State Machine  │
                         └──────────────────┘
```

Each node communicates with the other nodes using **gRPC**.

The Raft layer is responsible for reaching consensus on the replicated log, while the state machine applies committed operations.

## Implemented Components

### Leader Election

The implementation includes:

* Randomized election timeouts
* Candidate state and term progression
* Self-voting
* `RequestVote` RPC
* Candidate log freshness checks
* Majority-based leader election
* Higher-term detection
* Candidate → follower transitions
* Election cancellation using Go contexts
* Protection against stale election responses
* Per-election vote tracking

Election lifecycle:

```text
Follower
   │
   │ election timeout
   ▼
Candidate
   │
   ├── majority received ───────► Leader
   │
   ├── higher term observed ────► Follower
   │
   └── election timeout ────────► Follower
```

### Log Replication

Leaders replicate log entries to followers using `AppendEntries`.

The implementation maintains:

* `nextIndex` per follower
* `matchIndex` per follower
* Previous log index/term validation
* Log conflict detection
* Follower log reconciliation
* Replication retries
* Heartbeats
* Leader step-down on higher terms

A heartbeat is represented by an `AppendEntries` RPC with no log entries.

### Commit Index

The leader tracks follower replication progress and advances the commit index when a log entry has been replicated to a majority of the cluster.

```text
Client operation
       │
       ▼
Leader appends entry
       │
       ▼
AppendEntries
       │
       ├────────► Follower
       ├────────► Follower
       └────────► Follower
       │
       ▼
Majority replicated
       │
       ▼
Commit index advances
       │
       ▼
State machine applies entry
```

### State Machine

Committed log entries are applied to a key-value state machine.

The implementation tracks:

* `commitIndex`
* `lastApplied`
* Pending client operations
* State-machine application results

Client operations wait for the corresponding log entry to become committed before receiving a successful response.

### Write-Ahead Logging

The project includes a JSONL-based WAL for persisting log information.

The WAL is designed around an append-only approach and includes information required to reconstruct log state, including invalidation records used during conflicting-log reconciliation.

This part of the implementation is still evolving as crash-recovery and log-conflict scenarios are tested.

## Concurrency Model

The implementation uses Go's concurrency primitives extensively:

* Goroutines
* Channels
* Mutexes
* Context cancellation
* Per-peer replication workers
* State-transition listeners

State transitions are centralized through a state listener so that entering a state can trigger the appropriate lifecycle operations.

For example:

```text
LEADER
  ├── heartbeat sender
  └── AppendEntries workers

FOLLOWER
  └── heartbeat timeout listener

CANDIDATE
  └── election + RequestVote RPCs
```

## Communication

Nodes communicate using:

* **gRPC**
* Protocol Buffers
* Persistent gRPC connection caching

The Raft service exposes RPCs for:

```text
RequestVote
AppendEntries
```

## Project Structure

```text
server/
├── node/
│   ├── node.go
│   ├── election.go
│   ├── appendentries.go
│   ├── clientOp.go
│   └── ...
│
├── raft_proto/
│   ├── raft.proto
│   └── generated Go code
│
└── state_machine/
    └── ...
```

## Running the Cluster

The cluster is configured using a YAML configuration containing the nodes and their addresses.

Example:

```yaml
nodes:
  - id: node1
    address: localhost:8001

  - id: node2
    address: localhost:8002

  - id: node3
    address: localhost:8003

  - id: node4
    address: localhost:8004

  - id: node5
    address: localhost:8005

totalNodes: 5
```

Start individual nodes using their node ID:

```bash
go run . -id=node1
go run . -id=node2
go run . -id=node3
go run . -id=node4
go run . -id=node5
```

## Testing

The project includes end-to-end testing around the Raft cluster.

The testing focus is not limited to the happy path. The goal is to progressively test distributed-system failure modes such as:

* Multiple nodes starting elections simultaneously
* Delayed RPC responses
* Failed RPCs
* Higher-term messages
* Leader changes
* Follower catch-up
* Conflicting logs
* Duplicate messages
* Node restarts
* WAL recovery

Example:

```bash
go test ./...
```

Run a specific test:

```bash
go test ./path/to/package -run TestElectionE2E -v
```

## Design Goals

This project is being developed with two goals:

### 1. Build a working distributed KV store

The immediate goal is a functional Raft-based replicated key-value store.

### 2. Learn distributed systems through implementation

Rather than treating Raft as an algorithm to reproduce from pseudocode, this project is used to explore the engineering problems that appear when the algorithm meets:

* concurrency
* asynchronous RPCs
* network failures
* stale messages
* retries
* persistent storage
* process crashes
* state-machine execution

The implementation therefore deliberately emphasizes testing and failure scenarios alongside the core algorithm.

## Current Limitations

This is **not intended to be presented as a production-ready consensus system**.

Areas still under development include:

* More comprehensive failure-injection testing
* Crash/restart recovery testing
* WAL recovery hardening
* Snapshotting
* Log compaction
* More extensive network-partition testing
* Stronger client/read consistency semantics
* Dynamic cluster membership

## Learning Roadmap

The planned evolution of the project is:

```text
                 ┌─────────────────┐
                 │ Leader Election │
                 └────────┬────────┘
                          ▼
                 ┌─────────────────┐
                 │ Log Replication │
                 └────────┬────────┘
                          ▼
                 ┌─────────────────┐
                 │ Commit + Apply  │
                 └────────┬────────┘
                          ▼
                 ┌─────────────────┐
                 │ WAL + Recovery  │
                 └────────┬────────┘
                          ▼
                 ┌─────────────────┐
                 │ Failure Testing │
                 └────────┬────────┘
                          ▼
                 ┌─────────────────┐
                 │ Snapshots /     │
                 │ Compaction      │
                 └─────────────────┘
```

## Why Raft?

Raft provides a useful framework for understanding the core problems of distributed consensus:

* How nodes agree on a leader
* How replicated logs remain consistent
* How failures are handled
* How committed state is distinguished from uncommitted state
* How persistent state affects recovery

Implementing these mechanisms from scratch provides significantly more insight into distributed systems than simply using an existing consensus library.

## Tech Stack

* **Go**
* **gRPC**
* **Protocol Buffers**
* **Concurrency:** goroutines, channels, mutexes, contexts
* **Persistence:** JSONL WAL
* **Architecture:** distributed Raft cluster + replicated key-value state machine

## Author

Built as a hands-on distributed systems project in Go, with the goal of understanding Raft not just as an algorithm, but as a concurrent, failure-prone software system.
