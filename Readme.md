# SentinelCache

**A self-healing distributed in-memory cache built from scratch in Go.**

Consistent hashing, synchronous gRPC replication, heartbeat-based failure detection, automatic failover, and bully-algorithm leader election — all wired together and demonstrable with a single `docker compose up`.

---

## Features

| Feature | What it does |
|---|---|
| **LRU Cache Engine** | O(1) get/set/delete via doubly linked list + hashmap |
| **TTL Expiration** | Lazy expiry on read + active background cleanup every second |
| **Consistent Hashing** | 150 virtual nodes per physical node, MD5, binary search O(log N) |
| **Request Forwarding** | Any node accepts any read or write — automatically proxied to the ring-assigned primary |
| **Synchronous Replication** | Write ACKed only after primary + replica both confirm; rolled back on failure |
| **Heartbeat Detection** | Persistent bidirectional gRPC stream; node marked dead after 5 s silence |
| **Automatic Failover** | Leader broadcasts ring update to all peers via `Promote` RPC |
| **Leader Election** | Bully algorithm — highest-available node ID wins; double-election race guarded |

---

## Why I Built This

I've used Redis and distributed systems tooling for years, but I never fully understood what actually happens when:

* A cache node crashes
* A leader disappears
* Data must be rerouted
* Replicas are promoted
* A new leader is elected

SentinelCache was built to understand these distributed systems concepts by implementing them from scratch rather than treating Redis Cluster as a black box.

The goal wasn't to build another Redis replacement.

The goal was to build a system that could:

1. Distribute keys across nodes
2. Replicate data
3. Detect failures
4. Elect leaders
5. Recover automatically

and then observe every step happening in real time.

---

## Demo

Start the cluster:

```bash
docker compose up --build
```

Kill the leader:

```bash
docker compose stop node-a
```

Watch the cluster recover automatically:

```text
[node-c] leader appears dead — triggering election
[node-c] starting bully election (term=1, dead=node-a)
[node-c] no higher peers — winning election immediately
[node-c] *** WON ELECTION (term=1) — new leader ***
[node-c] removed dead leader node-a from ring
[node-c] *** LEADER — failure detector started ***
```

Verify the new leader:

```bash
curl -s localhost:8082/cluster/status | jq '{leader_id,node_id}'
```

```json
{
  "leader_id": "node-c",
  "node_id": "node-c"
}
```

No manual intervention required.

---

## Architecture Overview

```text
                    Leader (e.g. node-a)
                         │  failure detection
     ┌───────────────────┼───────────────────┐
     │   ♥ heartbeats    │                   │
     ▼                   ▼                   ▼

   Node A             Node B             Node C

 primary for        primary for        primary for
 ~1/3 of keys      ~1/3 of keys      ~1/3 of keys
 replica for        replica for        replica for
 another ~1/3      another ~1/3      another ~1/3

     └──────────── gRPC Replication ──────────┘
```

---

## Project Stats

| Metric                 | Value                 |
| ---------------------- | --------------------- |
| Language               | Go                    |
| Cluster Size           | 3 Nodes               |
| Replication Factor     | 2                     |
| Virtual Nodes          | 150 per physical node |
| Client Communication   | REST                  |
| Internal Communication | gRPC                  |
| Heartbeat Interval     | 1 second              |
| Failure Timeout        | 5 seconds             |
| Leader Election        | Bully Algorithm       |
| Cache Type             | In-Memory             |
| Eviction Strategy      | LRU                   |
| Expiration Strategy    | TTL                   |

---

## Key Engineering Highlights

* Built a distributed cache from scratch in Go
* Consistent hashing with 150 virtual nodes
* Transparent request forwarding
* Synchronous gRPC replication with rollback
* Bidirectional heartbeat streams
* Automatic failover
* Bully-algorithm leader election
* O(1) LRU cache implementation
* TTL expiration with active cleanup
* Fully Dockerized multi-node cluster

---


## Quick Start

**Prerequisites:** Docker, Docker Compose, `curl`. Optional: [`jq`](https://stedolan.github.io/jq/) for pretty JSON output.

```bash
git clone https://github.com/aryan-mishra/sentinel-cache
cd sentinel-cache
docker compose up --build
```

Three nodes start: node-a (leader, port 8080), node-b (8081), node-c (8082). Wait ~3 seconds for all nodes to join, then open a second terminal and follow the walkthrough below.

---

## Hands-On Walkthrough

Every feature, step by step. Copy-paste the commands — no thinking required.

### 0. Verify the cluster is healthy

```bash
curl -s localhost:8080/cluster/status | jq .
```

Expected output:
```json
{
  "leader_id": "node-a",
  "node_id":   "node-a",
  "key_count": 0,
  "peers": [
    { "id": "node-b", "status": "healthy" },
    { "id": "node-c", "status": "healthy" }
  ]
}
```

All three nodes are up. node-a is the initial leader (it's the seed node — no `SEED_ADDR` env var set).

---

### 1. Basic SET / GET / DELETE

```bash
# Write a key
curl -s -X POST localhost:8080/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"user:1","value":"alice"}' | jq .
```
```json
{
  "ok":         true,
  "written_by": "node-a",
  "primary":    "node-a",
  "replica":    "node-c"
}
```

> The response tells you which node owns this key (`primary`) and which holds the replica (`replica`). Different keys route to different nodes based on the consistent hash ring.

```bash
# Read it back
curl -s localhost:8080/get/user:1 | jq .
```
```json
{ "key": "user:1", "value": "alice", "served_by": "node-a" }
```

```bash
# Delete it
curl -s -X DELETE localhost:8080/delete/user:1 | jq .

# Confirm it's gone (returns 404)
curl -s localhost:8080/get/user:1 | jq .
```
```json
{ "error": "key not found", "served_by": "node-a" }
```

---

### 2. TTL Expiration

```bash
# Write a key that expires in 5 seconds
curl -s -X POST localhost:8080/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"session:tmp","value":"expires-soon","ttl":5}' | jq .

# Key is there immediately
curl -s localhost:8080/get/session:tmp | jq .
# → {"key":"session:tmp","value":"expires-soon","served_by":"node-a"}

# Wait 6 seconds
sleep 6

# Key is gone — expired
curl -s localhost:8080/get/session:tmp | jq .
# → {"error":"key not found","served_by":"node-a"}
```

> Two expiry mechanisms are running simultaneously: **lazy expiry** (checked on every `GET`) and **active cleanup** (a background goroutine that scans every second). Even keys no one reads get cleaned up.

---

### 3. Consistent Hashing — See Key Distribution

Write several keys and watch them route to different nodes based on the hash ring:

```bash
for key in user:1 user:2 user:3 order:100 order:200 session:abc; do
  echo -n "$key → "
  curl -s -X POST localhost:8080/set \
    -H 'Content-Type: application/json' \
    -d "{\"key\":\"$key\",\"value\":\"test\"}" \
    | jq -r '"primary=\(.primary)  replica=\(.replica)"'
done
```

Example output (yours will vary — ring positions are deterministic by MD5 hash):
```
user:1   → primary=node-a  replica=node-c
user:2   → primary=node-c  replica=node-b
user:3   → primary=node-b  replica=node-a
order:100 → primary=node-c  replica=node-a
order:200 → primary=node-a  replica=node-b
session:abc → primary=node-b  replica=node-c
```

> Each node owns roughly 1/3 of the key space. The ring uses **150 virtual nodes per physical node** — without virtual nodes, a 3-node ring would distribute keys very unevenly.

---

### 4. Request Forwarding — Write to Any Node

Pick a key whose `primary` is not node-b (check from step 3). Write it to node-b anyway:

```bash
# user:1 is owned by node-a. POST to node-b (port 8081) — it will forward to node-a.
curl -s -X POST localhost:8081/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"user:1","value":"forwarded-write"}' | jq .
```
```json
{
  "ok":         true,
  "written_by": "node-a",
  "primary":    "node-a",
  "replica":    "node-c"
}
```

> `written_by` is `node-a` even though the request hit node-b on port 8081. node-b checked the ring, saw it didn't own `user:1`, and transparently proxied the request to node-a. The client never needs to know which node owns which key.

---

### 5. Synchronous Replication + Read From Any Node

Write a key, then read it back from a different node:

```bash
# Write user:1 — note the "replica" field in the response (e.g. node-b)
curl -s -X POST localhost:8080/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"user:1","value":"replicated-value"}' | jq .
```
```json
{ "ok": true, "written_by": "node-a", "primary": "node-a", "replica": "node-b" }
```

```bash
# Read from ANY node — even one that doesn't own the key.
# If the node doesn't have it, it transparently fetches from the primary.
curl -s localhost:8082/get/user:1 | jq .
```
```json
{ "key": "user:1", "value": "replicated-value", "served_by": "node-a" }
```

> Two things happened here:
> 1. **Synchronous replication** — the write was not acknowledged until the replica (node-b above) confirmed it received the data via the `gRPC Replicate` RPC. If replication had failed, node-a would have rolled back the local write and returned HTTP 502.
> 2. **Read forwarding** — you read from node-c (8082), which is neither the primary nor the replica for this key. node-c didn't have it locally, so it forwarded the GET to the primary. `served_by` shows which node actually answered.
>
> You can also read directly from the replica node (the `replica` value from the write response) — it has the data locally, so `served_by` will be that replica.

---

### 6. Failure Detection + Failover — Kill a Follower Node

With the cluster running, stop node-b:

```bash
docker compose stop node-b
```

Watch node-a's logs (in your first terminal or a new one):
```bash
docker compose logs -f node-a
```

After ~5 seconds you'll see:
```
[node-a] *** FAILOVER: node-b is dead ***
[node-a] failover: broadcasting death of node-b to all peers
```

Verify node-b is marked dead:
```bash
curl -s localhost:8080/cluster/status | jq .peers
```
```json
[
  { "id": "node-b", "status": "dead" },
  { "id": "node-c", "status": "healthy" }
]
```

Keys that were on node-b still work — they now route to the next node clockwise on the ring:
```bash
# Any key previously owned by node-b now routes to node-a or node-c automatically
curl -s localhost:8080/get/user:3 | jq .
# → served_by has changed to the next ring node — node-b's replica
```

Bring node-b back up:
```bash
docker compose start node-b
```

---

### 7. Leader Election — Kill the Leader

This is the most interesting scenario. Kill node-a, which is the current leader:

```bash
docker compose stop node-a
```

Watch the election unfold in node-c's logs:
```bash
docker compose logs -f node-c
```

```
[node-c] heartbeat stream lost — failure 1/3
[node-c] heartbeat stream lost — failure 2/3
[node-c] heartbeat stream lost — failure 3/3
[node-c] leader appears dead — triggering election
[node-c] starting bully election (term=1, dead=node-a)
[node-c] no higher peers — winning election immediately
[node-c] *** WON ELECTION (term=1) — new leader ***
[node-c] removed dead leader node-a from ring
[node-c] *** LEADER — failure detector started ***
[node-c] ♥ heartbeat from node-b
```

After ~8 seconds, verify node-c is the new leader:
```bash
curl -s localhost:8081/cluster/status | jq '{leader_id,node_id}'
```
```json
{ "leader_id": "node-c", "node_id": "node-b" }
```

```bash
curl -s localhost:8082/cluster/status | jq '{leader_id,node_id}'
```
```json
{ "leader_id": "node-c", "node_id": "node-c" }
```

Data written before node-a died is still accessible (it was replicated to node-c before the failure):
```bash
curl -s localhost:8081/get/user:1 | jq .
# → {"key":"user:1","value":"replicated-value","served_by":"node-c"}
```

Bring node-a back up (it rejoins as a follower — node-c stays leader):
```bash
docker compose start node-a
```

---

### 8. LRU Eviction — Memory Limits

The cache is capped at **100 MB**. When the limit is reached, the least recently used key is evicted. Test this by running the local binary with a tiny limit:

```bash
make build

# Start a single node with a 30-byte memory cap
NODE_ID=test REST_ADDR=:9000 GRPC_ADDR=:9999 go run ./cmd/node &

# Fill it up
curl -s -X POST localhost:9000/set -H 'Content-Type: application/json' \
  -d '{"key":"a","value":"1234567890"}' # 11 bytes
curl -s -X POST localhost:9000/set -H 'Content-Type: application/json' \
  -d '{"key":"b","value":"1234567890"}' # 11 bytes — total 22 bytes

# Access 'a' to mark it recently used
curl -s localhost:9000/get/a

# Add 'c' — this must evict 'b' (least recently used, not 'a')
curl -s -X POST localhost:9000/set -H 'Content-Type: application/json' \
  -d '{"key":"c","value":"123456789012"}' # 13 bytes — pushes over 30

curl -s localhost:9000/get/b  # → 404 (evicted)
curl -s localhost:9000/get/a  # → 200 (survived — was recently accessed)
```

---

## Architecture

```
                    Leader (e.g. node-a)
                         │  failure detection
     ┌───────────────────┼───────────────────┐
     │   ♥ heartbeats    │                   │
     ▼                   ▼                   ▼

   Node A             Node B             Node C

 primary for        primary for        primary for
 ~1/3 of keys      ~1/3 of keys      ~1/3 of keys
 replica for        replica for        replica for
 another ~1/3      another ~1/3      another ~1/3

     └──────────── gRPC Replication ──────────┘
```

**Two independent roles — do not confuse them:**

- **Leader** — one node cluster-wide. Runs the failure detector, coordinates failover. Elected via bully algorithm. Any node can become leader.
- **Primary / Replica** — per-key roles assigned by the consistent hash ring. Each node is simultaneously primary for some keys and replica for others.

**Communication split:**

| Traffic | Protocol | Why |
|---|---|---|
| Client ↔ Node | REST (Gin) | Human-readable, easy to `curl` |
| Node ↔ Node | gRPC (protobuf) | Typed contracts, bidirectional streaming, what etcd/CockroachDB use |

---

## How the Algorithms Work

### Consistent Hashing

The hash ring has `150 × N` virtual nodes (N = number of physical nodes). Each virtual node is `MD5(nodeID + strconv(i))`, sorted on a uint32 ring. A `GET` does binary search in O(log N) to find the first virtual node clockwise from `MD5(key)`. `GetReplica(key, 2)` walks clockwise past the first virtual node to find a *distinct* physical node — that becomes the replica.

150 virtual nodes keeps load variance below ~10% even with 3 nodes. With 1 virtual node per physical node you'd see 3× variance.

### LRU Cache

`container/list` (doubly linked list) + `map[string]*list.Element`. Every `Set` pushes to the front; every `Get` calls `MoveToFront`. Eviction pops from the back. Both are O(1). A `sync.Mutex` (not `RWMutex`) guards the whole thing — `Get` mutates LRU order so there's no such thing as a "read-only" operation.

### Heartbeat & Failure Detection

Each follower maintains a **persistent bidirectional gRPC stream** to the leader. The sender ticks every second. The detector on the leader records `lastSeen[nodeID] = time.Now()` on each tick. A background goroutine checks every second — if `now - lastSeen[id] > 5s`, `onDead(id)` fires outside the lock (to prevent deadlock). After 3 consecutive stream failures the sender fires `onLeaderDead`, triggering election.

### Bully Election

When a follower detects the leader is dead:
1. It sends `Election` RPC to every peer with a higher node ID concurrently.
2. Each peer: if the candidate's ID is higher → `yield=true`. If own ID is higher → `yield=false`, start own election.
3. If all higher peers yield (or are unreachable) → win. Call `AnnounceLeader` to all peers.
4. All peers update their ring (`Remove(deadLeader)`) and set the new leader.

Race guard: if a node is already leader when it receives an `Election` RPC, it refuses without starting a new election. Without this, a node that just won could receive a delayed election message, start a second election with itself as the "dead leader", and accidentally remove itself from its own ring.

---

## gRPC Contract

All node-to-node communication is defined in `proto/cluster.proto`:

```protobuf
service ClusterService {
  rpc Heartbeat(stream HeartbeatRequest) returns (stream HeartbeatResponse);
  rpc Replicate(ReplicateRequest)           returns (ReplicateResponse);
  rpc Promote(PromoteRequest)               returns (PromoteResponse);
  rpc Join(JoinRequest)                     returns (JoinResponse);
  rpc Leave(LeaveRequest)                   returns (LeaveResponse);
  rpc Election(ElectionRequest)             returns (ElectionResponse);
  rpc AnnounceLeader(AnnounceLeaderRequest) returns (AnnounceLeaderResponse);
}
```

Regenerate Go code after editing the proto:
```bash
make proto
```

---

## Development

### Prerequisites

- Go 1.21+
- Docker + Docker Compose
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (only needed if editing `.proto`)

### Run locally (no Docker)

```bash
# Terminal 1 — seed node (becomes leader)
NODE_ID=node-a REST_ADDR=:8080 GRPC_ADDR=:9090 go run ./cmd/node

# Terminal 2
NODE_ID=node-b REST_ADDR=:8081 GRPC_ADDR=:9091 SEED_ADDR=:9090 go run ./cmd/node

# Terminal 3
NODE_ID=node-c REST_ADDR=:8082 GRPC_ADDR=:9092 SEED_ADDR=:9090 go run ./cmd/node
```

> **Note:** use `go build -o bin/node ./cmd/node && ./bin/node` instead of `go run` if you want `kill $PID` to work correctly. `go run` wraps the binary in a subprocess — `kill` hits the wrapper, not the node.

### Run tests

```bash
go test ./...
```

Tests cover:
- `internal/cache` — LRU eviction, TTL expiry, overwrite, delete (5 tests)
- `internal/cluster` — Ring distribution, node join/leave, replica routing (5 tests)
- `internal/election` — No-higher-peers win, dead peers ignored, concurrent election guard, term increment, dead leader ID propagation (5 tests)
- `internal/replication` — Real in-process gRPC servers: write propagation, delete propagation, non-primary no-op (3 tests)

### Project structure

```
sentinel-cache/
├── cmd/node/main.go              ← binary entry point, wires everything together
├── internal/
│   ├── cache/
│   │   ├── engine.go             ← LRU cache (SET/GET/DELETE + eviction)
│   │   ├── ttl.go                ← background TTL cleanup goroutine
│   │   └── engine_test.go
│   ├── cluster/
│   │   ├── node.go               ← node identity, peer tracking, leader state
│   │   ├── ring.go               ← consistent hash ring (150 vnodes, MD5)
│   │   ├── membership.go         ← JoinCluster() — dials seed, bootstraps ring
│   │   └── ring_test.go
│   ├── api/
│   │   └── handler.go            ← Gin REST handlers (set/get/delete/status + forwarding)
│   ├── grpc/
│   │   └── server.go             ← gRPC server implementing all ClusterService RPCs
│   ├── heartbeat/
│   │   ├── sender.go             ← follower side: persistent stream to leader
│   │   └── detector.go           ← leader side: lastSeen tracking, onDead callback
│   ├── replication/
│   │   └── replicator.go         ← primary → replica write forwarding via gRPC
│   ├── failover/
│   │   └── failover.go           ← BroadcastNodeDeath: ring update + Promote RPC fan-out
│   └── election/
│       ├── bully.go              ← bully algorithm: Start(), higherPeers(), becomeLeader()
│       └── bully_test.go
├── proto/
│   └── cluster.proto             ← gRPC service + message definitions
├── proto/gen/                    ← generated Go code (gitignored — run `make proto`)
├── Makefile
├── Dockerfile                    ← multi-stage: builder → alpine final image
├── docker-compose.yml            ← 3-node local cluster
└── DEVLOG.md                     ← build journal: every file, decision, and concept explained
```

---

## Non-Goals

This is a learning project — not a production Redis replacement.

Intentionally excluded:
- Persistence (AOF, RDB snapshots, WAL)
- TLS / mTLS
- Kubernetes / service mesh
- Quorum reads/writes
- Gossip protocol (uses leader-centric heartbeats instead)
- Raft/Paxos (uses bully algorithm — simpler, demonstrable, known tradeoffs)

---

## Design Tradeoffs

### Why Bully Algorithm instead of Raft?

The Bully algorithm is significantly simpler to implement and easier to demonstrate in a local multi-node environment.

**Pros**

* Simple implementation
* Easy to visualize
* Small code surface area
* Great for learning distributed systems

**Cons**

* Not partition-safe
* Can lead to split-brain scenarios
* Not suitable for production-grade consensus

A production-grade system would likely use Raft.

---

### Why gRPC Internally and REST Externally?

REST is used for client-facing APIs because:

* Easy to test with curl
* Human-readable
* Familiar interface
* Great developer experience

gRPC is used for node-to-node communication because:

* Strong contracts via protobuf
* Streaming support
* Lower serialization overhead
* Better fit for heartbeats and replication

---

### Why Consistent Hashing?

Traditional modulo hashing causes almost all keys to move when nodes are added or removed.

Example:

```text
hash(key) % 3
```

Adding a fourth node changes ownership for nearly every key.

Consistent hashing limits movement to roughly:

```text
1 / N
```

of keys, making scaling and failover practical.

---

### Why Synchronous Replication?

SentinelCache uses synchronous replication:

```text
Client
  ↓
Primary Write
  ↓
Replica Write
  ↓
ACK
```

The write is only acknowledged after both primary and replica confirm success.

**Pros**

* Stronger consistency
* Simpler recovery
* No replica lag

**Cons**

* Higher write latency
* Reduced availability during replica failures

---

### Why a Single Replica?

Each key is stored on:

```text
1 Primary
1 Replica
```

This keeps the implementation focused on distributed systems fundamentals without introducing quorum logic.

A production system would likely support:

```text
1 Primary
N Replicas
```

with configurable replication factors.

---


## Future Enhancements

**v2**
- Monitoring dashboard (live node health, key distribution, failover events)
- Connection pooling in the replicator (currently dials per write)
- Prometheus metrics endpoint
- gRPC connection pool

**v3**
- Raft consensus (replace bully — partition-safe)
- Quorum reads/writes
- Persistence (WAL)

---

## Known Limitations

| Limitation | Detail |
|---|---|
| No TLS | All traffic is plaintext. Fine inside a Docker network; not for internet exposure. |
| Connection-per-write | The replicator opens a new TCP connection for each replicated write. Should be a persistent pool in production. |
| Bully split-brain | Under a network partition, two isolated groups can each elect a leader. Raft/Paxos required for partition safety. |
| Lazy ring cleanup | When a node dies, existing cached data on that node is lost. Surviving replicas serve the data; no active migration. |
| Single replica | Each key has exactly one replica. Losing both primary and replica simultaneously loses that key range. |

---
