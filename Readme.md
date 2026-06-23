# SentinelCache

## Self-Healing Distributed In-Memory Cache

---

# 1. Overview

## One-Line Description

SentinelCache is a self-healing distributed in-memory cache built in Go that automatically replicates data, detects node failures, elects leaders, performs failover, and rebalances data across the cluster without human intervention.

---

# 2. Problem Statement

Modern applications rely heavily on caching systems such as Redis to reduce database load and improve response times.

A single cache server introduces a critical problem:

```text
Single Point of Failure
```

If the cache node crashes:

* Cached data becomes unavailable
* Application latency increases
* Database load spikes
* Service reliability decreases

Distributed cache systems solve this by:

* Partitioning data across multiple nodes
* Replicating data for fault tolerance
* Detecting failures automatically
* Recovering without human intervention

The goal of SentinelCache is to implement these distributed systems concepts from scratch and understand how systems like Redis Cluster, DynamoDB, Cassandra, and Hazelcast work internally.

---

# 3. Goals

The project should demonstrate understanding of:

## Distributed Systems

* Data Partitioning
* Consistent Hashing
* Replication
* Failure Detection
* Leader Election
* Automatic Failover
* Dynamic Rebalancing

## Backend Engineering

* Network Communication
* Concurrent Processing
* API Design
* Memory Management

## Infrastructure Engineering

* Multi-node Deployment
* Cluster Management
* Self-Healing Systems

---

# 4. Non-Goals

The project is not intended to replace Redis in production.

The following features are intentionally excluded.

## Redis Features

* Pub/Sub
* Streams
* Lua Scripts
* Transactions
* Sorted Sets
* HyperLogLog
* Bloom Filters
* ACLs

## Infrastructure

* Kubernetes
* Service Mesh
* Multi-region Replication

## Persistence

* Disk Persistence
* AOF
* RDB Snapshots

## Advanced Distributed Systems

* Gossip Protocol
* Quorum Reads/Writes
* Raft Consensus
* Paxos Consensus

These may be added in future versions.

---

# 5. Technology Stack

## Language

```text
Go
```

Reason:

* Excellent concurrency support
* Industry standard for infrastructure software
* Strong performance
* Simple deployment model

---

## Communication Model

SentinelCache uses a two-protocol design:

### Client API

```text
REST (HTTP/JSON)
```

Used by application clients to read and write cache data.

```http
POST /set
GET  /get
DELETE /delete
```

### Cluster-Internal Communication

```text
gRPC (Protocol Buffers)
```

Used for all node-to-node communication: heartbeats, replication, failover, and membership sync.

```protobuf
service ClusterService {
  rpc Heartbeat(stream HeartbeatRequest) returns (stream HeartbeatResponse);
  rpc Replicate(ReplicateRequest)        returns (ReplicateResponse);
  rpc Promote(PromoteRequest)            returns (PromoteResponse);
  rpc Join(JoinRequest)                  returns (JoinResponse);
  rpc Leave(LeaveRequest)                returns (LeaveResponse);
  rpc Election(ElectionRequest)          returns (ElectionResponse);
  rpc AnnounceLeader(AnnounceLeaderRequest) returns (AnnounceLeaderResponse);
}
```

Reason:

* Typed contracts make inter-node protocol explicit and auditable
* Bidirectional streaming suits continuous heartbeat flows
* Built-in deadlines, retries, and connection management
* Used by real distributed systems: etcd, CockroachDB, Kubernetes

---

## Deployment

```text
Docker
Docker Compose
```

Each container represents one cache node.

---

## Storage

```go
map[string]CacheEntry
```

The cache is fully in-memory. No database is used.

---

# 6. High-Level Architecture

```text
                     Leader Node (e.g. node-a)
                          │  failure detection
      ┌───────────────────┼───────────────────┐
      │   heartbeats      │                   │
      ▼                   ▼                   ▼

    Node A             Node B             Node C
  primary for        primary for        primary for
  ~1/3 of keys      ~1/3 of keys      ~1/3 of keys
  replica for        replica for        replica for
  ~1/3 of keys      ~1/3 of keys      ~1/3 of keys

      └───────────────────┼───────────────────┘
                          │
               gRPC Heartbeats / Replication
```

**Two independent roles — do not confuse them:**
- **Leader** — one node cluster-wide, responsible for failure detection and failover coordination. Elected via bully algorithm. Any node can become leader.
- **Primary/Replica** — per-key roles assigned by the consistent hash ring. Each node is primary for some keys and replica for others simultaneously.

Every node:

* Stores cache data for its assigned key ranges
* Accepts writes for any key and forwards to the correct primary
* Sends heartbeats to the leader over a persistent gRPC stream
* Can become leader if the current leader fails

---

# 7. Core Features

---

## Feature 1: Cache Engine

### Client API (REST)

```http
POST   /set
GET    /get
DELETE /delete
```

### Example

```text
SET user:1 Aryan
GET user:1
DELETE user:1
```

### Responsibilities

* Store keys
* Retrieve keys
* Delete keys

---

## Feature 2: TTL Expiration

### Example

```text
SET user:1 Aryan EX 60
```

After 60 seconds:

```text
GET user:1
nil
```

### Responsibilities

* Store expiration metadata
* Run background cleanup workers
* Automatically remove expired entries

---

## Feature 3: LRU Eviction

When memory usage exceeds the configured limit:

```text
100 MB
```

The least recently used keys are evicted.

### Responsibilities

* Track access order using a doubly linked list + hashmap (O(1) get and evict)
* Automatically evict stale entries when the limit is reached

---

# 8. Distributed Cluster

---

## Feature 4: Cluster Membership

Nodes can:

```text
Join Cluster
Leave Cluster
Crash
Recover
```

### gRPC RPCs

```protobuf
rpc Join(JoinRequest)   returns (JoinResponse);
rpc Leave(LeaveRequest) returns (LeaveResponse);
```

### Responsibilities

* Track active nodes
* Maintain cluster topology
* Synchronize membership information

---

## Feature 5: Consistent Hashing

Keys are distributed using a consistent hash ring with virtual nodes.

### Example

```text
user:1 → Node A
user:2 → Node C
user:3 → Node B
```

### Benefits

* Even key distribution
* Minimal data movement when nodes join or leave
* Efficient node scaling

### Responsibilities

* Build hash ring with virtual nodes
* Assign ownership
* Recompute ownership on topology changes

---

## Feature 6: Replication

Every key is replicated synchronously to one replica before the write is acknowledged.

### Consistency Model

```text
Synchronous replication to 1 replica.
Write is ACKed only after primary and replica both confirm.
If replication fails, the local write is rolled back and the client receives an error.
```

### Example

```text
Primary: Node A
Replica: Node B
```

When:

```text
POST /set {"key":"user:1","value":"Aryan"}
```

1. Client may hit any node — it is automatically forwarded to the primary (Node A).
2. Node A writes locally.
3. Node A replicates to Node B via `gRPC Replicate`.
4. Only after Node B confirms does Node A ACK the client.
5. If replication fails, Node A rolls back and returns HTTP 502.

### gRPC RPC

```protobuf
rpc Replicate(ReplicateRequest) returns (ReplicateResponse);
```

### Responsibilities

* Replicate writes synchronously
* Maintain replicas
* Recover from primary failures

---

# 9. Self-Healing Features

---

## Feature 7: Heartbeats

Every node sends periodic heartbeat messages to the leader over a persistent gRPC stream.

### Heartbeat Message

```protobuf
message HeartbeatRequest {
  string node_id  = 1;
  string status   = 2;
  int64  timestamp = 3;
}
```

### gRPC RPC

```protobuf
rpc Heartbeat(stream HeartbeatRequest) returns (stream HeartbeatResponse);
```

### Responsibilities

* Report node health continuously
* Detect communication failures

---

## Feature 8: Failure Detection

If heartbeats are not received within the configured threshold:

```text
Node A
↓
Heartbeat Timeout (default: 5s)
↓
Node Marked Dead
↓
Recovery Workflow Triggered
```

### Responsibilities

* Detect unhealthy nodes
* Trigger failover

---

## Feature 9: Automatic Failover

When a primary node fails:

```text
Node A (Primary) — fails
↓
Leader detects missed heartbeats
↓
Leader promotes Node B (Replica) via gRPC Promote RPC
↓
Hash ring updated
↓
Cluster resumes serving requests
```

### gRPC RPC

```protobuf
rpc Promote(PromoteRequest) returns (PromoteResponse);
```

### Responsibilities

* Promote replicas
* Update routing
* Maintain availability

---

## Feature 10: Dynamic Rebalancing

When a new node joins, the hash ring is recomputed and ownership is updated.

### Scope

New writes are immediately routed to the correct node per the updated ring. Existing keys are lazily migrated — they remain on the old node until TTL expiry or explicit eviction. This avoids migration complexity while keeping the ring correct for new traffic.

### Example

Before:

```text
Node A = 40%
Node B = 30%
Node C = 30%
```

After Node D joins:

```text
Node A = 25%
Node B = 25%
Node C = 25%
Node D = 25%
```

### Responsibilities

* Recompute hash ring on topology change
* Route new writes to correct owner
* Lazy migration of existing keys

---

# 10. Cluster Coordination

---

## Feature 11: Leader Election

One node acts as the cluster leader using a **bully election algorithm**.

### Algorithm

```text
1. Any node that detects a leader failure initiates an election.
2. It sends an ELECTION message to all nodes with a higher ID.
3. If no higher-ID node responds within a timeout, it declares itself leader.
4. If a higher-ID node responds, it takes over the election.
5. The winner broadcasts LEADER to all nodes.
```

This guarantees the highest-available node ID always becomes leader under normal network conditions — only one node can receive all-yields, so exactly one winner emerges per election.

**Known limitation:** under a network partition, two isolated groups could each elect their own leader (split-brain). This is an inherent weakness of the bully algorithm. Production systems use Raft or Paxos for partition safety; this project implements bully to demonstrate the core concept.

### Leader Responsibilities

* Manage cluster membership
* Handle failover decisions
* Coordinate rebalancing
* Maintain cluster state

### Example

```text
Leader Failure
↓
Election Initiated (Bully Algorithm)
↓
Highest Available Node Wins
↓
New Leader Broadcasts LEADER Message
↓
Cluster Resumes
```

---

# 11. Deployment Model

## Local Cluster

```text
Docker Compose

├── node-a (port 8080 REST, 9090 gRPC)  ← seed / initial leader
├── node-b (port 8081 REST, 9091 gRPC)
└── node-c (port 8082 REST, 9092 gRPC)
```

Every container runs one SentinelCache node. Docker simulates distributed machines on a single host.

Cluster status is visible on any node via:

```bash
curl localhost:8080/cluster/status
```

---

# 12. Demonstration Scenario

## Scenario 1 — Normal Write (any node, transparent forwarding)

```bash
# Hit node-b even though node-a owns this key — it gets forwarded automatically
curl -X POST localhost:8081/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"user:1","value":"Aryan","ttl":300}'
```

Flow:

```text
Client → POST /set to node-b
↓
node-b checks ring: primary for "user:1" is node-a
↓
node-b forwards request to node-a (HTTP proxy)
↓
node-a writes locally, replicates to node-c via gRPC Replicate
↓
Only after node-c confirms: node-a ACKs node-b, node-b ACKs client
```

```bash
# Read from any node — replica serves it directly
curl localhost:8082/get/user:1
# → {"key":"user:1","value":"Aryan","served_by":"node-c"}
```

---

## Scenario 2 — Leader Failure (election + ring update)

```bash
docker stop node-a   # node-a is the initial leader
```

What actually happens in the code:

```text
Heartbeat streams from node-b and node-c to node-a break
↓
After 3 consecutive stream failures (~6s): onLeaderDead() fires
↓
Bully election starts — node-c contacts node-b (higher ID wins)
↓
node-c wins, broadcasts AnnounceLeader to all peers
↓
All nodes call ring.Remove("node-a") — node-a's key range reroutes
↓
node-c starts failure detector, becomes new leader
↓
Cluster healthy — total recovery ~8 seconds, no manual intervention
```

```bash
# Verify new leader
curl localhost:8082/cluster/status
# → {"leader_id":"node-c", "peers":[{"id":"node-a","status":"dead"},{"id":"node-b","status":"healthy"}]}

# Data written before the failure is still served by the replica
curl localhost:8082/get/user:1
# → {"key":"user:1","value":"Aryan","served_by":"node-c"}
```

---

## Scenario 3 — Non-Leader Node Failure (detect + promote)

```bash
docker stop node-b   # node-b is a follower
```

```text
node-c (leader) failure detector: no heartbeat from node-b for 5s
↓
onDead callback fires: BroadcastNodeDeath("node-b")
↓
Leader calls ring.Remove("node-b") on all nodes via Promote RPC
↓
node-b's key range reroutes to its replica automatically
↓
Cluster healthy
```

---

# 13. Future Enhancements

## Version 2

* Monitoring Dashboard (node health, leader, key distribution, failover events)
* Gossip Protocol (replace leader-centric heartbeats)
* Prometheus Metrics
* OpenTelemetry Tracing

## Version 3

* Quorum Reads/Writes
* Persistence (WAL or RDB snapshots)
* Raft Consensus (replace bully election)

---

# 14. Resume Summary

Built a self-healing distributed in-memory cache in Go supporting:

* Consistent Hashing with virtual nodes (150 vnodes, MD5, binary search)
* Transparent Request Forwarding — any node accepts writes, automatically proxied to the ring-assigned primary
* Synchronous Replication (primary + 1 replica, write rolled back on failure)
* gRPC-based bidirectional Heartbeat failure detection (5 s timeout)
* Automatic Failover via replica promotion and ring rebalancing
* Bully Algorithm Leader Election (highest-ID wins, term-guarded, double-election race fixed)
* TTL Expiration with lazy + background active cleanup
* LRU Eviction (O(1) doubly linked list + hashmap)

Deployed as a 3-node cluster using Docker Compose. Entire self-healing sequence observable via `docker stop`.
