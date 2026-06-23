# SentinelCache — Build Log & Concept Journal

Every file, every decision, every concept — in the order we built them.
Append to this file as the project grows.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [go.mod — The Module File](#2-gomod--the-module-file)
3. [Directory Structure](#3-directory-structure)
4. [proto/cluster.proto — The gRPC Contract](#4-protoclusterproto--the-grpc-contract)
5. [Makefile — Developer Workflow](#5-makefile--developer-workflow)
6. [internal/cache/engine.go — The Cache Engine](#6-internalcacheenginego--the-cache-engine)
7. [internal/cache/ttl.go — Background TTL Cleanup](#7-internalcachettlgo--background-ttl-cleanup)
8. [internal/cluster/node.go — Node Identity](#8-internalclusternodego--node-identity)
9. [internal/api/handler.go — REST Handlers (Gin)](#9-internalapihandlergo--rest-handlers-gin)
10. [cmd/node/main.go — Entry Point](#10-cmdnodemainmain-entry-point)
11. [Dockerfile — Multi-Stage Build](#11-dockerfile--multi-stage-build)
12. [docker-compose.yml — Local Cluster](#12-docker-composeyml--local-cluster)
13. [REST Handlers — Implementation](#13-rest-handlers--implementation)
14. [internal/cluster/ring.go — Consistent Hashing](#14-internalclusterringgo--consistent-hashing)
15. [Proto Code Generation — make proto](#15-proto-code-generation--make-proto)
16. [internal/grpc/server.go — The gRPC Server](#16-internalgrpcservergo--the-grpc-server)
17. [internal/cluster/membership.go — Joining the Cluster](#17-internalclustermembershipgo--joining-the-cluster)
18. [Updated cmd/node/main.go — gRPC + Membership Wiring](#18-updated-cmdnodemainmain-go--grpc--membership-wiring)
19. [internal/heartbeat/sender.go — Heartbeat Sender](#19-internalheartbeatsendergo--heartbeat-sender)
20. [internal/heartbeat/detector.go — Failure Detector](#20-internalheartbeatdetectorgo--failure-detector)
21. [Heartbeat System — Demo Output](#21-heartbeat-system--demo-output)
22. [internal/replication/replicator.go — Write Replication](#22-internalreplicationreplicatorg--write-replication)
23. [internal/failover/failover.go — Automatic Failover](#23-internalfailoverfailovergo--automatic-failover)
24. [Updated internal/grpc/server.go — Replicate + Promote](#24-updated-internalgrpcservergo--replicate--promote)
25. [Updated internal/api/handler.go — Replication Wired](#25-updated-internalapihandlergo--replication-wired)
26. [Full Demo Output — Replication + Failover](#26-full-demo-output--replication--failover)
27. [Known Limitations](#27-known-limitations)
28. [proto update — AnnounceLeader RPC](#28-proto-update--announceleader-rpc)
29. [internal/election/bully.go — The Bully Algorithm](#29-internalelectionbullygo--the-bully-algorithm)
30. [Updated internal/heartbeat/sender.go — onLeaderDead Callback](#30-updated-internalheartbeatsendergo--onleaderdead-callback)
31. [Updated internal/grpc/server.go — Election + AnnounceLeader handlers](#31-updated-internalgrpcservergo--election--announceleader-handlers)
32. [Updated cmd/node/main.go — Full Election Wiring](#32-updated-cmdnodemainmainmain-go--full-election-wiring)
33. [Bug Fix: Double Election Guard](#33-bug-fix-double-election-guard)
34. [Full Demo Output — Leader Election](#34-full-demo-output--leader-election)

---

## 1. Project Overview

**What we are building:**
SentinelCache — a self-healing distributed in-memory cache in Go.

**The problem it solves:**
A single cache node is a single point of failure. If it crashes, all cached data is lost, latency spikes, and the database gets hammered. SentinelCache distributes data across multiple nodes and heals itself when nodes fail — no human intervention needed.

**Why this is a good resume project:**
It demonstrates understanding of consistent hashing, replication, failure detection, and leader election — exactly what distributed systems interviews probe. The failure demo (`docker stop node-a` → cluster recovers automatically) is the interview story.

**Communication model (key architectural decision):**

| Layer | Protocol | Why |
|---|---|---|
| Client → Node | REST (Gin) | Simple, human-readable, easy to curl |
| Node → Node | gRPC | Binary, typed contracts, streaming, what real systems use |

This split is how production systems work. etcd, CockroachDB, and Kubernetes all use gRPC internally and REST externally.

---

## 2. `go.mod` — The Module File

**File:** `go.mod`

```
module github.com/aryan-mishra/sentinel-cache

go 1.25.0
```

**What it does:**
- Declares the module name — the prefix used for all internal imports
- Sets the minimum Go version

**Why the module name matters:**
When `internal/api/handler.go` imports the cache package, it writes:
```go
import "github.com/aryan-mishra/sentinel-cache/internal/cache"
```
The module name is the root of every import path in the project.

**Why Go 1.22+:**
Go 1.22 added method-prefix routing in `http.ServeMux` (`"POST /set"`). We're on 1.25 because `go get` upgraded it automatically when we added Gin.

---

## 3. Directory Structure

```
sentinel-cache/
├── cmd/
│   └── node/
│       └── main.go          ← binary entry point
├── internal/
│   ├── cache/
│   │   ├── engine.go        ← SET/GET/DELETE + LRU eviction
│   │   ├── ttl.go           ← background TTL cleanup goroutine
│   │   └── engine_test.go
│   ├── cluster/
│   │   ├── node.go          ← node identity + peer tracking
│   │   ├── ring.go          ← consistent hash ring
│   │   └── ring_test.go
│   └── api/
│       └── handler.go       ← Gin REST handlers
├── proto/
│   └── cluster.proto        ← gRPC service + message definitions
├── proto/gen/               ← generated Go code (gitignored, run `make proto`)
├── Makefile
├── Dockerfile
├── docker-compose.yml
├── go.mod
├── go.sum
└── DEVLOG.md                ← this file
```

**Key convention — `internal/`:**
The Go compiler enforces that nothing outside this module can import packages under `internal/`. It is not just a naming convention — it is a compiler rule. This prevents anyone from accidentally using SentinelCache as a library and depending on its internals.

**`cmd/` convention:**
Each subdirectory of `cmd/` is a separate binary. We only have one binary (`node`), but the convention makes it easy to add a `cmd/cli` or `cmd/dashboard` later without restructuring.

---

## 4. `proto/cluster.proto` — The gRPC Contract

**File:** `proto/cluster.proto`

**What is a `.proto` file?**
It is the source of truth for all node-to-node communication. It defines:
- What **messages** (data structures) nodes exchange
- What **RPCs** (remote procedure calls) nodes can make on each other

Running `protoc` (the Protocol Buffer compiler) on this file generates Go structs and a gRPC client/server interface automatically. We never write those by hand.

**Why Protocol Buffers over JSON?**
- Binary format — much smaller and faster than JSON
- Strongly typed — field types are enforced at compile time, not runtime
- Backward compatible — adding new fields doesn't break old nodes
- Used by etcd, Kubernetes, CockroachDB for the same reasons

**The RPCs we defined:**

| RPC | Direction | Purpose |
|---|---|---|
| `Heartbeat` | streaming | Node → Leader, continuous health updates |
| `Replicate` | unary | Primary → Replica, forward a write |
| `Promote` | unary | Leader → Replica, take over as primary |
| `Join` | unary | New node → Seed node, enter the cluster |
| `Leave` | unary | Node → Leader, graceful departure |
| `Election` | unary | Candidate → Peers, bully election vote |

**Why `Heartbeat` is a stream, not unary:**
A unary RPC opens a new TCP connection for every call. Heartbeats fire every second — opening a connection per second per node adds overhead. A bidirectional stream (`stream HeartbeatRequest`) holds one persistent connection and pumps messages through it. It also means if the stream drops, that itself signals a failure.

**Key design in `ElectionResponse`:**
```protobuf
message ElectionResponse {
  bool yield = 1;
  // true  = "I yield, you can be leader"
  // false = "I'm taking over"
}
```
This single boolean encodes the entire bully algorithm decision. We'll implement the logic later but the wire protocol is captured here.

**To generate Go code from the proto:**
```bash
make proto
# requires: brew install protobuf
#           go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#           go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

---

## 5. `Makefile` — Developer Workflow

**File:** `Makefile`

**Why a Makefile?**
It is a single place for all developer commands. Instead of remembering long `protoc` invocations, you run `make proto`. Instead of a long `docker-compose` command, you run `make docker-up`.

**Key targets:**

| Target | What it does |
|---|---|
| `make proto` | Generates Go code from `proto/cluster.proto` into `proto/gen/` |
| `make build` | Compiles the binary to `bin/node` |
| `make run` | Runs a single node locally for quick testing |
| `make docker-up` | Builds images and starts the 3-node cluster |
| `make docker-down` | Stops and removes the cluster containers |
| `make clean` | Deletes `bin/` and `proto/gen/` |

---

## 6. `internal/cache/engine.go` — The Cache Engine

**File:** `internal/cache/engine.go`

### Core Data Structure: LRU = Doubly Linked List + Hashmap

The goal is O(1) for both lookup and eviction. We need two things:
- Fast lookup by key → use a `map[string]*list.Element`
- Fast eviction of the least recently used item → use a `*list.List` (doubly linked list)

```
map["user:1"] ─────────────────────────────┐
map["user:2"] ──────────────────────┐      │
map["user:3"] ──────────┐           │      │
                         ▼           ▼      ▼
front [user:3] ↔ [user:2] ↔ [user:1] back
  MRU                              LRU
  (most recently used)     (evict this first)
```

- On `Get` → move element to front (it was just accessed)
- On eviction → remove from back (least recently used)
- `container/list` from Go's standard library is the doubly linked list

### Why `sync.Mutex` not `sync.RWMutex`

`sync.RWMutex` allows multiple goroutines to read simultaneously — good for read-heavy workloads. But our `Get` is not a pure read: it moves the accessed element to the front of the LRU list, which is a write. Using `RWMutex` would mean every `Get` still needs a write lock, adding overhead with no benefit. Plain `sync.Mutex` is correct and simpler.

### Memory Accounting

```go
func (e *entry) sizeBytes() int64 {
    return int64(len(e.key) + len(e.value))
}
```

We approximate memory usage as `len(key) + len(value)`. Struct overhead is not counted but the approximation is consistent — it will never grow unbounded.

### TTL: Two layers

1. **Lazy expiry** — checked in `Get`. If the key is expired, it is deleted on the spot and a miss is returned. Zero cost if the key is never read again.
2. **Active expiry** — background goroutine (see `ttl.go`) fires every second and deletes all expired keys. Prevents memory leaking from keys that are written but never read.

### Key functions

```go
// Set: if key exists → update in place + move to front
//      if key is new → push to front, then evict if over limit
func (e *Engine) Set(key, value string, ttl time.Duration)

// Get: miss if not found, miss + delete if expired, move to front if found
func (e *Engine) Get(key string) (string, bool)

// Delete: remove from list and map, update byte count
func (e *Engine) Delete(key string)

// evictIfNeeded: pop from back of LRU list until used <= maxBytes
func (e *Engine) evictIfNeeded()
```

---

## 7. `internal/cache/ttl.go` — Background TTL Cleanup

**File:** `internal/cache/ttl.go`

```go
func (e *Engine) runTTLCleanup() {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()
    for range ticker.C {
        e.deleteExpired()
    }
}
```

**Why a separate goroutine?**
Without active cleanup, a key written once and never read again will sit in memory forever even after it expires. The background goroutine is the safety net.

**Is it safe to delete from a map during range iteration in Go?**
Yes — the Go spec explicitly allows it. Keys deleted during iteration will not be visited again. Keys added during iteration may or may not be visited. We only delete, so it is safe.

**Why every 1 second?**
It is a reasonable tradeoff. Shorter = more CPU overhead. Longer = expired keys linger in memory. 1 second is what Redis uses as its base tick. It does not need to be exact — the lazy expiry in `Get` is the real safety net.

---

## 8. `internal/cluster/node.go` — Node Identity

**File:** `internal/cluster/node.go`

**What it owns:**
This package is the node's view of the cluster — who it is, who else is in the cluster, who the current leader is. It is deliberately separate from the cache engine: a node's cluster membership is independent of its cache contents.

**Key fields:**
```go
type Node struct {
    ID       string     // e.g. "node-a"
    RESTAddr string     // e.g. ":8080"
    GRPCAddr string     // e.g. ":9090"
    peers    map[string]*PeerInfo
    leaderID string
}
```

**Why `sync.RWMutex` here (and not in the cache)?**
Reading peer list and leader ID is genuinely read-only — no side effects. Multiple goroutines (heartbeat sender, REST handler, gRPC server) will read concurrently. So `RWMutex` is correct here. The cache was the exception; this is the normal case.

---

## 9. `internal/api/handler.go` — REST Handlers (Gin)

**File:** `internal/api/handler.go`

**Why Gin over standard library `http.ServeMux`?**
- `c.ShouldBindJSON(&req)` — automatic request body parsing + validation, one line
- `c.JSON(200, gin.H{...})` — clean JSON response, no boilerplate
- Path params: `GET /get/:key` reads cleanly as `c.Param("key")`
- Well-known in the Go ecosystem

The alternative (standard library) requires manual `json.Decode`, manual error checking, and manual header setting every time.

**Gin is only for the client-facing REST layer.** Cluster-internal traffic (heartbeats, replication) uses gRPC. Gin never sees a heartbeat message.

**Routes registered:**

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/set` | Write a key |
| `GET` | `/get/:key` | Read a key |
| `DELETE` | `/delete/:key` | Delete a key |
| `GET` | `/cluster/status` | Live cluster state (nodes, leader, key count) |

**Request body for `/set`:**
```json
{ "key": "user:1", "value": "aryan", "ttl": 60 }
```
`ttl` is in seconds (client-friendly integer). Converted to `time.Duration` at the handler boundary — the engine never deals with raw seconds.

---

## 10. `cmd/node/main.go` — Entry Point

**File:** `cmd/node/main.go`

**Its only job:** read config, wire components together, start servers.

**Why environment variables for config?**
Docker Compose sets env vars per container. Each of the three nodes gets a different `NODE_ID`, `REST_ADDR`, `GRPC_ADDR` just by changing `docker-compose.yml` — no config files, no flags, no rebuilding the image.

**Config variables:**

| Variable | Example | Purpose |
|---|---|---|
| `NODE_ID` | `node-a` | Unique identifier for this node |
| `REST_ADDR` | `:8080` | Address for the client REST API |
| `GRPC_ADDR` | `:9090` | Address for gRPC cluster communication |
| `SEED_ADDR` | `node-a:9090` | gRPC address of the first node to contact when joining |

`SEED_ADDR` is empty on the first node (it is the seed). Every other node sets it to `node-a:9090` to join the cluster.

**Wiring order:**
```
engine  := cache.NewEngine(100 MB)     // in-memory store
node    := cluster.NewNode(id, ...)    // cluster identity
handler := api.NewHandler(engine, node) // REST layer

r := gin.Default()
handler.RegisterRoutes(r)
r.Run(restAddr)
```

---

## 11. `Dockerfile` — Multi-Stage Build

**File:** `Dockerfile`

```dockerfile
FROM golang:1.25-alpine AS builder   ← Stage 1: compile
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download                  ← download deps first (Docker layer cache)
COPY . .
RUN go build -o bin/node ./cmd/node

FROM alpine:3.21                     ← Stage 2: run
WORKDIR /app
COPY --from=builder /app/bin/node .  ← only copy the binary
EXPOSE 8080 9090
CMD ["./node"]
```

**Why multi-stage?**
- Stage 1 uses the full Go image (~1 GB) just to compile
- Stage 2 is a bare Alpine Linux image (~10 MB) with only the binary
- Final image has no Go toolchain, no source code, no build tools — just the binary
- Smaller image = faster deploys, smaller attack surface

**Why `COPY go.mod go.sum` before `COPY .`?**
Docker caches each layer. If you copy all source files first, any change to any `.go` file invalidates the dependency download layer and re-downloads everything. Copying `go.mod`/`go.sum` first means `go mod download` only re-runs when dependencies actually change.

---

## 12. `docker-compose.yml` — Local Cluster

**File:** `docker-compose.yml`

**What it simulates:**
Three separate machines running the same binary, each with different config. Docker networking makes `node-a`, `node-b`, `node-c` resolvable as hostnames between containers.

**Port mapping:**

| Container | REST (host) | gRPC (host) |
|---|---|---|
| node-a | 8080 | 9090 |
| node-b | 8081 | 9091 |
| node-c | 8082 | 9092 |

Inside the Docker network all three use `:8080` and `:9090` — the host port mapping is just for our local `curl` commands.

**`SEED_ADDR` pattern:**
- `node-a` has no `SEED_ADDR` — it starts first and initialises the cluster
- `node-b` and `node-c` set `SEED_ADDR=node-a:9090` — they contact node-a on startup to join

This is the same pattern used by etcd, Consul, and Cassandra. One well-known seed; everyone else discovers the rest of the cluster from it.

---

## 13. REST Handlers — Implementation

**File:** `internal/api/handler.go` (implementation added)

Handlers call directly into `engine.Set`, `engine.Get`, `engine.Delete`.
No routing logic yet — that comes when consistent hashing is wired in. For now every node runs its own independent cache.

**`/set`:**
```go
h.engine.Set(req.Key, req.Value, time.Duration(req.TTL)*time.Second)
→ 200 { "ok": true }
```

**`/get/:key`:**
```go
val, ok := h.engine.Get(key)
→ 200 { "key": "...", "value": "..." }   // found
→ 404 { "error": "key not found" }       // miss or expired
```

**`/delete/:key`:**
```go
h.engine.Delete(key)
→ 200 { "ok": true }   // always succeeds (no-op if key doesn't exist)
```

---

## 14. `internal/cluster/ring.go` — Consistent Hashing

**File:** `internal/cluster/ring.go`

### The Problem With Naive Hashing

If you have 3 nodes and route keys with `hash(key) % 3`, adding a 4th node changes the modulus to 4. Almost every key now maps to a different node — the entire cache is invalidated. For a cache that backs a database, this causes a thundering herd of cache misses.

### How Consistent Hashing Fixes This

The hash space (0 → 2³²) is treated as a circle — a **ring**. Nodes are placed at positions on that ring. To find a key's owner: hash the key, walk clockwise to the first node you hit.

```
              0
           /     \
       node-c   node-a
           \     /
            node-b
             2³²
```

When node-d joins, only keys between node-d and its predecessor need to move — roughly **1/N** of all keys. Everything else stays where it is.

### Virtual Nodes — Why?

With one position per node, random placement creates uneven load: one node might own 60% of the ring. Virtual nodes solve this: each physical node gets **150 positions** spread across the ring. The load averages out.

```
ring: [...node-a#12...node-b#47...node-a#83...node-c#120...node-b#155...]
```

150 is the number used by real systems like Cassandra. Higher = better distribution, slightly more memory for the ring metadata.

### Hash Function

We use **MD5** (first 4 bytes → uint32). MD5 is not used for security here — it is purely for its even distribution across the hash space. `crc32` or `fnv` would also work; MD5 is the conventional choice for consistent hash rings.

### Key functions

```go
// Add: place a node at `virtualNodes` positions on the ring
func (r *Ring) Add(nodeID string)

// Remove: take a node off the ring; its keys automatically fall to the next node
func (r *Ring) Remove(nodeID string)

// GetNode: hash the key, binary search for the first position >= hash, wrap around
func (r *Ring) GetNode(key string) string

// GetReplica: like GetNode but returns the Nth distinct node clockwise from key
// GetReplica(key, 1) = primary owner
// GetReplica(key, 2) = first replica
func (r *Ring) GetReplica(key string, n int) string
```

### What the Tests Prove

| Test | What it verifies |
|---|---|
| `TestConsistencyAfterNodeJoin` | Adding node-d moves ~25% of keys (not 100%) |
| `TestRemoveNodeReroutes` | After removing node-b, zero keys resolve to node-b |
| `TestGetReplicaReturnsDifferentNode` | Primary and replica are always different nodes |
| `TestDistributionIsReasonablyEven` | Each node owns 20–46% of keys (not wildly uneven) |

---

---

## 15. Proto Code Generation — `make proto`

**What `protoc` does:**
Running `make proto` feeds `proto/cluster.proto` into the Protocol Buffer compiler (`protoc`) with two plugins:

| Plugin | Output file | What it contains |
|---|---|---|
| `protoc-gen-go` | `proto/gen/cluster.pb.go` | A Go struct for every `message` in the proto |
| `protoc-gen-go-grpc` | `proto/gen/cluster_grpc.pb.go` | The server interface, client struct, and registration functions |

These files are **generated** — we never edit them by hand. They are gitignored because they can always be regenerated from the `.proto` source.

**What is in `cluster.pb.go`?**
Every `message` block becomes a plain Go struct with serialization baked in:
```go
// generated — do not edit
type JoinRequest struct {
    NodeId   string
    RestAddr string
    GrpcAddr string
}
```

**What is in `cluster_grpc.pb.go`?**
Two things:
1. `ClusterServiceServer` — an interface we **implement** on each node (server side)
2. `ClusterServiceClient` — a struct we **use** to call other nodes (client side)

**`UnimplementedClusterServiceServer` — incremental development pattern:**
The generated code includes `UnimplementedClusterServiceServer`. Embedding it in your server struct means any RPC you have not implemented yet returns a gRPC "not implemented" status instead of a compile error. This lets us build features incrementally.

```go
type Server struct {
    pb.UnimplementedClusterServiceServer  // handles all unimplemented RPCs
    node   *cluster.Node
}
```

**Install commands (one-time setup):**
```bash
brew install protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

---

## 16. `internal/grpc/server.go` — The gRPC Server

**File:** `internal/grpc/server.go`

**What it is:**
Every node runs a gRPC server that other nodes dial to perform cluster operations. This is the "server side" — it listens on the gRPC port (`:9090`) and handles incoming RPCs from peers.

**Why a separate `internal/grpc/` package?**
Separates the transport layer (gRPC) from the cluster state layer (`internal/cluster/`). `cluster/` owns the state; `grpc/` owns the wire protocol.

**`Server` struct holds three references:**
```go
type Server struct {
    pb.UnimplementedClusterServiceServer
    node     *cluster.Node   // read/update peer list
    ring     *cluster.Ring   // add/remove nodes on topology changes
    engine   *cache.Engine   // needed for Replicate RPC (next phase)
    detector *heartbeat.Detector // set on leader nodes only
}
```

**`Join` handler — pre-seeding the detector:**
When a node joins, we call `detector.UpdateLastSeen(req.NodeId)` immediately. This starts the timeout clock from the moment of join, not from the first heartbeat. If a node joins and then crashes before sending any heartbeat, it will still be detected as dead after 5 seconds.

**Stubs for future phases:**
`Replicate`, `Promote`, `Election`, and `Heartbeat` are stubbed using `UnimplementedClusterServiceServer`. They get implemented in their respective phases.

---

## 17. `internal/cluster/membership.go` — Joining the Cluster

**File:** `internal/cluster/membership.go`

**Flow when a non-seed node starts:**
```
dial SEED_ADDR via gRPC
    ↓
send JoinRequest { node_id, rest_addr, grpc_addr }
    ↓
receive JoinResponse { peers[], leader_id }
    ↓
for each peer: node.AddPeer(peer) + ring.Add(peer.ID)
    ↓
node.SetLeader(leader_id)
```

**Why `insecure.NewCredentials()`?**
gRPC requires you to explicitly declare TLS or no-TLS. `insecure.NewCredentials()` = plaintext TCP. Fine inside a Docker network. In production you would use mutual TLS (mTLS).

**Why `grpc.NewClient` not `grpc.Dial`?**
`grpc.Dial` is deprecated. `grpc.NewClient` is the modern replacement — it connects lazily (on first RPC) instead of immediately. Better for startup resilience.

**Membership propagation gap (known limitation):**
Sequential joins work correctly: node-c joining after node-b will learn about node-b from node-a's peer list. But node-b will not learn about node-c after the fact. This is closed in the heartbeat phase: the leader broadcasts a complete membership list periodically.

---

## 18. Updated `cmd/node/main.go` — gRPC + Membership Wiring

**Startup order:**
```
create engine + node + ring
    ↓
ring.Add(self)     ← add ourselves to the ring first
    ↓
start gRPC server (goroutine) ← must be up BEFORE joining
    ↓
JoinCluster(seed) if SEED_ADDR set ← now safe to join
    ↓
start detector (leader) or sender (follower)
    ↓
start REST server (blocks)
```

**Why gRPC must start before joining:**
Once we call `Join` on the seed, the seed (and later the heartbeat sender on node-a) may immediately try to dial us back. If our gRPC server is not listening yet, those inbound calls fail.

---

## 19. `internal/heartbeat/sender.go` — Heartbeat Sender

**File:** `internal/heartbeat/sender.go`

**What it does:**
Runs on every non-leader node. Maintains a persistent gRPC bidirectional stream to the leader and sends a `HeartbeatRequest` every second.

**Two-level loop design:**

```
Start() — outer loop: handles reconnection
    │
    └── stream() — inner function: one connection's lifetime
            │
            └── ticker fires every 1s → hbStream.Send(heartbeat)
```

If `stream()` returns an error (leader restart, network drop), `Start()` logs it, waits 2 seconds, and reconnects. This is the resilience pattern — the sender never gives up.

**Why bidirectional stream, not unary?**
A unary RPC (`Heartbeat(req) → resp`) opens a new TCP connection every second. That's wasteful. A bidirectional stream holds one persistent HTTP/2 connection for the lifetime of the relationship. It also means if the stream closes unexpectedly, the error itself signals a network problem.

**How the sender finds the leader:**
```go
leaderAddr := s.node.LeaderGRPCAddr()
// returns peers[leaderID].GRPCAddr
// returns "" if this node IS the leader (no need to heartbeat yourself)
```

---

## 20. `internal/heartbeat/detector.go` — Failure Detector

**File:** `internal/heartbeat/detector.go`

**What it does:**
Runs on the leader only. Tracks when each peer last sent a heartbeat. A background watchdog goroutine calls `onDead(nodeID)` when a node goes silent beyond the timeout.

**Key design — `dead` set prevents duplicate callbacks:**
```go
type Detector struct {
    lastSeen map[string]time.Time
    dead     map[string]bool   // nodes already reported dead this cycle
    timeout  time.Duration
    onDead   func(nodeID string)
}
```

Without the `dead` set, `checkAll` would call `onDead` every second after the timeout — once is enough. When a node recovers (heartbeats resume), it's removed from the `dead` set and the cycle resets.

**Recovery handling:**
```go
func (d *Detector) UpdateLastSeen(nodeID string) {
    d.lastSeen[nodeID] = time.Now()
    if d.dead[nodeID] {
        delete(d.dead, nodeID)       // mark alive again
        d.node.MarkPeerStatus(nodeID, cluster.StatusHealthy)
    }
}
```

**Why `go d.onDead(nodeID)` (goroutine)?**
The callback (`MarkPeerStatus`, log, eventually trigger failover) must not run inside the `checkAll` lock. If the callback tried to acquire any lock that `checkAll` already holds, it would deadlock. Firing it in a goroutine releases the lock immediately.

**Important process management note (learned during testing):**
When running with `go run`, killing the `go run` process does NOT kill the compiled child binary — the binary becomes an orphan and keeps running. For production testing (and the demo), always use a pre-compiled binary (`go build`) and run it directly. This is why `make build` exists.

---

## 21. Heartbeat System — Demo Output

After wiring everything together, the failure detection scenario produces:

```
[node-a] ← Join from node-b (:9091)
[node-a] ♥ heartbeat from node-b    ← flowing every second
[node-a] ♥ heartbeat from node-b
[node-a] ♥ heartbeat from node-b
[node-a] FAILURE DETECTED: node-b silent for 5.672s — marking dead
[node-a] node node-b is DEAD — failover TODO
```

The `failover TODO` is filled in the next phase.

---

## 22. `internal/replication/replicator.go` — Write Replication

**File:** `internal/replication/replicator.go`

**What it does:**
After every local write, the `Replicator` checks if this node is the primary for that key. If yes, it dials the replica node's gRPC address and calls the `Replicate` RPC to forward the write.

**The primary check:**
```go
primary := r.ring.GetReplica(key, 1)
if primary != r.node.ID {
    return nil // not our key to replicate
}
```
Only the primary replicates. The replica node that receives a `Replicate` RPC just writes to its engine — it does not re-replicate further.

**Why best-effort (not synchronous failure)?**
The README said "synchronous replication — ACK after both confirm." In practice, if replication fails (replica temporarily down), returning an error to every client write is disruptive. We write locally, attempt replication, and log failures. For a production system you would add retry logic and circuit-breaking. For the resume project, the demo shows replication working under normal conditions.

**Connection-per-write (known limitation):**
We open a new gRPC connection for every replication call. In production you would maintain a connection pool per peer. Mentioned explicitly so interviewers know you're aware of it.

---

## 23. `internal/failover/failover.go` — Automatic Failover

**File:** `internal/failover/failover.go`

**What it does:**
When the leader's detector fires `onDead(deadID)`, `BroadcastNodeDeath` is called. It:
1. Removes the dead node from the **leader's own ring**
2. Calls the `Promote` gRPC on every other healthy peer, telling them to also remove the dead node from their rings

After all nodes remove the dead node, the consistent hash ring automatically routes that node's key range to the **next node clockwise** — which is the replica (it holds the replicated writes).

**Why ring removal is enough:**
No data needs to move. The replica already received the writes via replication. We just need to stop routing requests to the dead node. Removing it from the ring achieves this instantly.

**The `Promote` RPC — repurposed as ring-update broadcast:**
The proto's `Promote` RPC was designed as "tell a node to take over". In our implementation it means: "remove `NodeId` from your ring — you are now the primary for its key range." This is semantically correct: we are promoting the next ring node to primary without explicitly messaging it.

---

## 24. Updated `internal/grpc/server.go` — Replicate + Promote implemented

**`Replicate` RPC:**
```go
// Simply writes or deletes the key in the local engine.
// No re-replication — replicas are leaves, not intermediaries.
func (s *Server) Replicate(ctx, req) {
    if req.IsDelete { s.engine.Delete(req.Key) }
    else             { s.engine.Set(req.Key, req.Value, ttl) }
}
```

**`Promote` RPC:**
```go
// Removes the dead node from this node's ring.
// After this returns, reads/writes to the dead node's key range
// automatically route to this node (the next on the ring).
func (s *Server) Promote(ctx, req) {
    s.ring.Remove(req.NodeId)
    s.node.MarkPeerStatus(req.NodeId, cluster.StatusDead)
}
```

---

## 25. Updated `internal/api/handler.go` — Replication wired in

**New fields:** `ring *cluster.Ring` and `replicator *replication.Replicator`

**`handleSet` response includes routing metadata:**
```json
{ "ok": true, "written_by": "node-a", "primary": "node-a", "replica": "node-b" }
```
This is excellent for the demo — you can see exactly which node owns each key before and after failover.

**`handleGet` response includes `served_by`:**
```json
{ "key": "user:1", "value": "aryan", "served_by": "node-a" }
```
After failover, `served_by` changes from the dead primary to the promoted replica — that's the proof that the system healed itself.

---

## 26. Full Demo Output — Replication + Failover

```
# SET — replication happens automatically
POST /set user:1=aryan → { "primary": "node-a", "replica": "node-b" }
POST /set user:3=raj   → { "primary": "node-a", "replica": "node-b" }

# Verify replica received the data
GET node-b/get/user:1  → { "value": "aryan", "served_by": "node-b" } ✅

# Kill node-b (the replica)
kill $PID_B

# node-a detects failure after 5 seconds
[node-a] FAILURE DETECTED: node-b silent for 5.061s — marking dead
[node-a] *** FAILOVER: node-b is dead, rebalancing ring ***

# node-c receives ring-update broadcast
[node-c] ← Promote: removing dead node node-b, now routing its keys to next ring node

# Data is still available from node-a (the primary)
GET node-a/get/user:1  → { "value": "aryan", "served_by": "node-a" } ✅
GET node-a/get/user:3  → { "value": "raj",   "served_by": "node-a" } ✅
```

**Zero manual intervention. The cluster healed itself.**

---

## 27. Known Limitations (Honest Engineering)

| Limitation | Impact | Fix in production |
|---|---|---|
| Connection-per-write in replicator | High overhead under load | gRPC connection pool per peer |
| No write forwarding to primary | Writes to non-primary go to local cache only | Route write to primary via gRPC |
| Leader failure not handled yet | If node-a (leader) dies, no one detects it | Leader election (next phase) |
| Replication is best-effort | A replica crash during write = data only on primary | Synchronous replication with retry |

*Last updated: Replication + failover complete. Next: leader election (bully algorithm).*

---

## 15. Proto Code Generation — `make proto`

**What `protoc` does:**
Running `make proto` feeds `proto/cluster.proto` into the Protocol Buffer compiler (`protoc`) with two plugins:

| Plugin | Output file | What it contains |
|---|---|---|
| `protoc-gen-go` | `proto/gen/cluster.pb.go` | A Go struct for every `message` in the proto |
| `protoc-gen-go-grpc` | `proto/gen/cluster_grpc.pb.go` | The server interface, client struct, and registration functions |

These files are **generated** — we never edit them by hand. They are also gitignored (`proto/gen/` is in `.gitignore`) because they can always be regenerated from the `.proto` source.

**What is in `cluster.pb.go`?**
Every `message` block becomes a plain Go struct with serialization baked in:
```go
// generated — do not edit
type JoinRequest struct {
    NodeId   string `protobuf:"bytes,1,opt,name=node_id"`
    RestAddr string `protobuf:"bytes,2,opt,name=rest_addr"`
    GrpcAddr string `protobuf:"bytes,3,opt,name=grpc_addr"`
}
```

**What is in `cluster_grpc.pb.go`?**
Two things:

1. `ClusterServiceServer` — an interface we **implement** on each node (the server side)
2. `ClusterServiceClient` — a struct we **use** to call other nodes (the client side)

```go
// We implement this:
type ClusterServiceServer interface {
    Heartbeat(ClusterService_HeartbeatServer) error
    Replicate(context.Context, *ReplicateRequest) (*ReplicateResponse, error)
    Join(context.Context, *JoinRequest) (*JoinResponse, error)
    // ...
}

// We use this to call other nodes:
type ClusterServiceClient interface {
    Join(ctx context.Context, in *JoinRequest, ...) (*JoinResponse, error)
    // ...
}
```

**`UnimplementedClusterServiceServer` — incremental development pattern:**
The generated code also includes `UnimplementedClusterServiceServer`. If you embed it in your server struct, any RPC you have not implemented yet returns a gRPC "not implemented" status instead of a compile error. This lets us build features incrementally — implement `Join` and `Leave` this phase, stub the rest with the embedded type, and the project compiles and runs.

```go
type Server struct {
    pb.UnimplementedClusterServiceServer  // ← handles all unimplemented RPCs
    node   *cluster.Node
    // ...
}
```

**Install commands (one-time setup):**
```bash
brew install protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

---

## 16. `internal/grpc/server.go` — The gRPC Server

**File:** `internal/grpc/server.go`

**What it is:**
Every node runs a gRPC server that other nodes dial to perform cluster operations. This is the "server side" — it listens on the gRPC port (`:9090`) and handles incoming RPCs from peers.

**Why a separate `internal/grpc/` package?**
The gRPC server is the transport layer for cluster communication. Keeping it separate from `internal/cluster/` (which owns cluster state) follows the single-responsibility principle: `cluster/` knows the *state*, `grpc/` knows the *transport*.

**Key design — `Server` struct:**
```go
type Server struct {
    pb.UnimplementedClusterServiceServer
    node   *cluster.Node   // to read/update peer list
    ring   *cluster.Ring   // to add/remove nodes when topology changes
    engine *cache.Engine   // needed later for Replicate RPC
}
```

The server holds references to `Node`, `Ring`, and `Engine` because cluster RPCs affect all three:
- `Join` → updates `node` peers + `ring`
- `Replicate` → writes to `engine`
- `Leave` → updates `node` peers + `ring`

**`Join` handler — what the seed does when a new node arrives:**
1. Add the joiner to the peer map
2. Add the joiner to the hash ring
3. Return the current full peer list (including self) so the joiner can bootstrap its own view of the cluster

**Membership propagation — known limitation:**
When node-b joins via node-a, node-a returns its current peer list. This works correctly for sequential joins (node-c joins after node-b, so node-a's list already includes node-b). However, node-b will not learn about node-c if node-c joins after node-b has already finished its join call. This gap is solved in the heartbeat phase: the leader will periodically broadcast the complete membership list to all nodes.

---

## 17. `internal/cluster/membership.go` — Joining the Cluster (Client Side)

**File:** `internal/cluster/membership.go`

**What it does:**
This is the *client side* of cluster membership — the code a new node runs on startup to dial the seed and register itself.

**Flow:**
```
new node starts
    │
    ▼
dial seed address (SEED_ADDR env var)
    │
    ▼
send JoinRequest { node_id, rest_addr, grpc_addr }
    │
    ▼
receive JoinResponse { peers[], leader_id }
    │
    ▼
for each peer:
    node.AddPeer(peer)
    ring.Add(peer.ID)
    │
    ▼
node.SetLeader(leader_id)
```

**Why `grpc.WithTransportCredentials(insecure.NewCredentials())`?**
gRPC requires you to explicitly declare whether you are using TLS or not. `insecure.NewCredentials()` means "no TLS — plaintext TCP." This is fine for a cluster running inside a Docker network where traffic is not exposed to the internet. In production you would use mutual TLS (mTLS).

**`grpc.NewClient` vs `grpc.Dial`:**
`grpc.Dial` is the old API (deprecated). `grpc.NewClient` is the modern replacement — it does not establish the connection immediately (lazy connection), which is better for startup resilience.

---

## 18. Updated `cmd/node/main.go` — Wiring gRPC + Membership

**Changes to `main.go`:**

Added three things to the startup sequence:

1. **Create the Ring** — initialized with `ring.Add(node.ID)` so this node is on its own ring from the start
2. **Start gRPC server** — launched in a goroutine so it does not block the REST server
3. **Join cluster** — if `SEED_ADDR` is set, call `cluster.JoinCluster(...)` before starting REST

**Startup order matters:**
The gRPC server must start *before* calling `JoinCluster`. Why? Because once we join, the seed will try to contact us back (in the heartbeat phase). If our gRPC server is not up yet, those inbound calls will fail.

```
start gRPC server (goroutine)
    ↓
join cluster via seed (if SEED_ADDR set)
    ↓
start REST server (blocking, main goroutine)
```

---

## 28. Proto Update — `AnnounceLeader` RPC

**Why a new RPC?**

After an election winner is chosen, every other node needs to:
1. Know who the new leader is (so heartbeat senders can reconnect to it)
2. Know who the *old* leader was (so they can remove it from their consistent hash ring)

This requires broadcasting a message — a new RPC dedicated to that announcement.

**What we added to `proto/cluster.proto`:**

```protobuf
// AnnounceLeader is broadcast by the winner to tell all peers the new leader.
rpc AnnounceLeader(AnnounceLeaderRequest) returns (AnnounceLeaderResponse);

message AnnounceLeaderRequest {
  string leader_id      = 1;  // the new leader (the winner)
  string dead_leader_id = 2;  // the old leader (so peers can clean up their ring)
  int64  term           = 3;  // election term (monotonically increasing)
}

message AnnounceLeaderResponse {
  bool ack = 1;
}
```

**Why include `dead_leader_id`?**
Every node maintains its own copy of the consistent hash ring. When node-a dies, each peer must call `ring.Remove("node-a")` independently. We piggyback that information on the announcement so peers can do the cleanup in one step.

**After editing `.proto`, always regenerate:**
```bash
make proto
# runs: protoc --go_out=... --go-grpc_out=... proto/cluster.proto
```

---

## 29. `internal/election/bully.go` — The Bully Algorithm

**File:** `internal/election/bully.go`

### What is leader election?

In any distributed system with a concept of "leader" (one coordinator, one primary, one sequencer), you need a way to pick a new leader when the old one dies. This is the leader election problem.

Properties a good election algorithm should have:
- **Safety** — exactly one winner
- **Liveness** — the election eventually completes (no permanent deadlock)
- **Progress** — every node eventually knows who the new leader is

### The Bully Algorithm

The bully algorithm is the simplest correct election algorithm. It works on lexicographic node IDs — the highest available node always wins.

**Protocol:**
1. Any node that detects the leader is dead starts an election.
2. It sends `Election` to every peer with a **higher** ID.
3. Each recipient:
   - If its own ID is higher → refuse and start its own election.
   - If the candidate's ID is higher → yield.
4. If **all** higher peers yield (or are unreachable) → the initiator wins.
5. The winner broadcasts `AnnounceLeader` to all peers.

**Why "bully"?** The highest node *bullies* everyone else into yielding.

**Why is it safe?** Only one node can have the highest ID in a given set of alive nodes. So only one node can get all yields.

**Why is it not used in production?** It assumes synchronous message delivery. Under network partitions it can elect two leaders simultaneously (split-brain). Real systems use Raft or Paxos.

### The `term` field

Elections have a monotonically increasing term counter. If two elections start concurrently (e.g., node-b and node-c both think the leader is dead), the term disambiguates which election's result to accept. Higher term wins.

```go
type Bully struct {
    node  *cluster.Node
    ring  *cluster.Ring
    onWin func(deadLeaderID string)  // callback to main.go when we win

    mu     sync.Mutex
    active bool   // prevents two concurrent elections from this node
    term   int64  // incremented each time we start an election
}
```

### Why `active bool` guard?

Without it, if the failure detector fires twice before the first election finishes, you'd start two elections from the same node, both incrementing the term — a mess. The mutex-guarded `active` flag ensures only one election runs at a time per node.

### `Start()` — the election entry point

```go
func (b *Bully) Start(deadLeaderID string) {
    b.mu.Lock()
    if b.active { b.mu.Unlock(); return }  // already running
    b.active = true
    b.term++
    term := b.term
    b.mu.Unlock()
    defer func() { b.mu.Lock(); b.active = false; b.mu.Unlock() }()

    higher := b.higherPeers()
    if len(higher) == 0 {
        b.becomeLeader(deadLeaderID, term)
        return
    }

    anyTakingOver := false
    var wg sync.WaitGroup
    for _, peer := range higher {
        wg.Add(1)
        go func(addr string) {
            defer wg.Done()
            if !b.sendElection(addr, term, deadLeaderID) {
                anyTakingOver = true
            }
        }(peer.GRPCAddr)
    }
    wg.Wait()

    if anyTakingOver {
        return  // stand down; a higher node is running its own election
    }
    b.becomeLeader(deadLeaderID, term)
}
```

Key patterns:
- **`sync.WaitGroup`** — wait for all concurrent `sendElection` calls to complete before deciding
- **Capture `term` before unlock** — the term field could change after unlock; we snapshot it
- **`defer` to reset `active`** — runs even if the function panics

### `higherPeers()` — who can outrank us?

```go
func (b *Bully) higherPeers() []*cluster.PeerInfo {
    var out []*cluster.PeerInfo
    for _, p := range b.node.Peers() {
        if p.ID > b.node.ID && p.Status != cluster.StatusDead {
            out = append(out, p)
        }
    }
    return out
}
```

Only considers peers that aren't already marked dead. A dead peer can't respond so we don't need to ask it.

### `sendElection()` — treat unreachable as yielded

```go
func (b *Bully) sendElection(addr string, term int64, deadLeaderID string) bool {
    conn, err := grpc.NewClient(addr, ...)
    if err != nil { return true }   // unreachable = treat as yielded

    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()

    resp, err := client.Election(ctx, &pb.ElectionRequest{
        CandidateId: b.node.ID,
        Term:        term,
    })
    if err != nil { return true }   // no response = treat as yielded
    return resp.Yield
}
```

**Why treat unreachable as yielded?**
If a higher peer is unreachable, it can't be leader either. We don't want one dead peer to stall the election forever. "No answer = I yield" is the pragmatic choice.

**3-second timeout:** If a peer is slow (not dead, just congested), we don't wait forever. 3 seconds is long enough to distinguish a slow peer from a dead one.

### `becomeLeader()` — announce and fire callback

```go
func (b *Bully) becomeLeader(deadLeaderID string, term int64) {
    fmt.Printf("[%s] *** WON ELECTION (term=%d) — new leader ***\n", b.node.ID, term)

    for _, peer := range b.node.Peers() {
        if peer.ID != b.node.ID && peer.Status != cluster.StatusDead {
            go b.sendAnnounce(peer.GRPCAddr, deadLeaderID, term)
        }
    }

    b.onWin(deadLeaderID)   // wired in main.go to start leader duties
}
```

Each `sendAnnounce` call is a separate goroutine — we fire-and-forget to all peers concurrently. The `onWin` callback is what actually transitions the node into leader mode (starts the failure detector, stops the heartbeat sender).

---

## 30. Updated `internal/heartbeat/sender.go` — `onLeaderDead` Callback

**File:** `internal/heartbeat/sender.go`

### What changed and why

Previously the sender just reconnected infinitely on failure. We added two things:
1. **Failure counter** — tracks consecutive failed streams
2. **`onLeaderDead` callback** — fired after `maxReconnectFailures` consecutive failures

```go
const maxReconnectFailures = 3

type Sender struct {
    node         *cluster.Node
    engine       *cache.Engine
    interval     time.Duration
    onLeaderDead func()  // NEW: called when the leader appears permanently dead
}
```

### Why a callback instead of importing the election package?

If `heartbeat` imported `election`, and `election` imported `heartbeat`, you'd have a cycle:
```
heartbeat → election → heartbeat   (cycle — Go refuses to compile this)
```

The callback pattern inverts the dependency: `main.go` knows about both packages and wires them together. Neither package knows about the other.

```go
// In main.go:
sender := heartbeat.NewSender(node, engine, time.Second, func() {
    go bully.Start(node.LeaderID())
})
```

The closure captures `bully` from the outer scope. The `heartbeat` package never needs to import `election`.

### Failure counting logic

```go
func (s *Sender) Start(ctx context.Context) {
    failures := 0
    for {
        if s.node.IsLeader() { return }   // won election → stop sending

        leaderAddr := s.node.LeaderGRPCAddr()
        if leaderAddr == "" { time.Sleep(time.Second); continue }

        if err := s.stream(ctx, leaderAddr); err != nil {
            failures++
            if failures >= maxReconnectFailures && s.onLeaderDead != nil {
                failures = 0
                s.onLeaderDead()   // trigger election
            }
            time.Sleep(2 * time.Second)
        } else {
            failures = 0   // successful stream resets counter
        }
    }
}
```

**Why 3 failures?** One failed stream could be a blip (network hiccup, leader restart). Three consecutive failures is a strong signal the leader is dead. This trades some detection latency for fewer false elections.

**Why reset `failures = 0` before calling `onLeaderDead`?** If the election itself takes time and the sender keeps looping, we don't want it to fire `onLeaderDead` again while an election is already running.

**Why check `IsLeader()` at the top of the loop?** After winning an election, the node sets itself as leader. The next loop iteration checks this and returns cleanly, stopping the sender.

### Why `senderCtx` in `main.go`?

The sender is started with a context:
```go
senderCtx, cancelSender := context.WithCancel(ctx)
go sender.Start(senderCtx)
```

When a node wins an election, `startLeaderDuties` calls `cancelSender()`. This forces the sender's `stream()` function to return immediately (via `<-ctx.Done()`), stopping the heartbeat sender. Without this, the sender would keep trying to connect to the old (dead) leader.

---

## 31. Updated `internal/grpc/server.go` — Election + AnnounceLeader Handlers

**File:** `internal/grpc/server.go`

### The `electionStarter` interface — breaking the import cycle

The gRPC server needs to start an election when it receives an `Election` RPC from a lower peer. But if `grpc/server.go` imported `election/bully.go`, that would create an import cycle:
```
grpc/server.go  →  election/bully.go  →  ???
```

The fix: define a narrow interface in `server.go` itself:
```go
type electionStarter interface {
    Start(deadLeaderID string)
}
```

`*election.Bully` satisfies this interface (it has a `Start(string)` method), so `main.go` can wire them together without either package importing the other.

**Concept:** This is the **Dependency Inversion Principle** — depend on an abstraction (interface), not a concrete type. Go interfaces are implicit (duck typing), so this costs zero extra code in the `election` package.

### `Election` RPC handler

```go
func (s *Server) Election(ctx context.Context, req *pb.ElectionRequest) (*pb.ElectionResponse, error) {
    // If we already won an election, just refuse — no need to start another.
    if s.node.IsLeader() {
        return &pb.ElectionResponse{Yield: false}, nil
    }

    if req.CandidateId > s.node.ID {
        return &pb.ElectionResponse{Yield: true}, nil  // candidate outranks us
    }

    // We outrank — capture dead leader ID now, before any state change.
    deadLeader := s.node.LeaderID()
    if s.bully != nil {
        go s.bully.Start(deadLeader)
    }
    return &pb.ElectionResponse{Yield: false}, nil
}
```

**IsLeader guard:** Without this, if node-c wins an election and then receives an `Election` from node-b (which started its election slightly later), node-c would start a *second* election with itself as the dead leader — causing `ring.Remove("node-c")` and removing itself from its own ring. The guard prevents this entire class of bug.

**Why capture `deadLeader` before calling `Start`?** `Start` calls `onWin`, which calls `node.SetLeader(nodeID)`. After that, `s.node.LeaderID()` would return the new leader (us), not the dead one. We must snapshot the dead leader before any state changes.

### `AnnounceLeader` RPC handler

```go
func (s *Server) AnnounceLeader(ctx context.Context, req *pb.AnnounceLeaderRequest) (*pb.AnnounceLeaderResponse, error) {
    fmt.Printf("[%s] ← AnnounceLeader: new leader=%s (replaced %s, term=%d)\n",
        s.node.ID, req.LeaderId, req.DeadLeaderId, req.Term)
    if s.onAnnounce != nil {
        s.onAnnounce(req.LeaderId, req.DeadLeaderId)
    }
    return &pb.AnnounceLeaderResponse{Ack: true}, nil
}
```

`onAnnounce` is a callback set from `main.go`:
```go
grpcServer.SetAnnounceHandler(func(newLeaderID, deadLeaderID string) {
    ring.Remove(deadLeaderID)
    node.MarkPeerStatus(deadLeaderID, cluster.StatusDead)
    node.SetLeader(newLeaderID)
})
```

When the announcement arrives:
1. The dead leader is removed from the ring (keys now route to the new leader)
2. The dead leader is marked dead (prevents future RPCs to it)
3. `SetLeader` updates the stored leader ID — the heartbeat sender reads this on the next reconnect and dials the new leader

---

## 32. Updated `cmd/node/main.go` — Full Election Wiring

**File:** `cmd/node/main.go`

### The `senderCtx` / `cancelSender` pair

```go
senderCtx, cancelSender := context.WithCancel(ctx)
```

`context.WithCancel` returns a child context and a cancel function. Calling `cancelSender()` signals the child context as done — any code that `select`s on `senderCtx.Done()` will unblock and return.

This is the standard Go pattern for stopping a goroutine from outside.

### `startLeaderDuties` closure

```go
startLeaderDuties := func(deadLeaderID string) {
    cancelSender()   // stop heartbeat sender (we are now the leader)

    if deadLeaderID != "" {
        ring.Remove(deadLeaderID)
        node.MarkPeerStatus(deadLeaderID, cluster.StatusDead)
    }
    node.SetLeader(nodeID)   // mark ourselves as leader

    detector := heartbeat.NewDetector(5*time.Second, node, func(deadID string) {
        node.MarkPeerStatus(deadID, cluster.StatusDead)
        failover.BroadcastNodeDeath(deadID, node, ring)
    })
    grpcServer.SetDetector(detector)
    go detector.Start(ctx)
}
```

This function handles the role transition: follower → leader. It's called in two places:
1. At startup, if this node is the seed (`seedAddr == ""`): `startLeaderDuties("")`
2. When the bully algorithm wins: `election.NewBully(node, ring, func(deadLeaderID string) { startLeaderDuties(deadLeaderID) })`

**Why a closure?** The closure captures `cancelSender`, `ring`, `node`, `nodeID`, `grpcServer`, `ctx` from the outer scope — all the state needed to transition to leader without having to pass it all as parameters. This is idiomatic Go for startup wiring.

### Full startup sequence for a non-seed node

```
1. start gRPC server (goroutine)
2. JoinCluster via seed → learn peers, leader ID
3. create senderCtx/cancelSender pair
4. create bully (captures cancelSender via startLeaderDuties closure)
5. wire bully into grpcServer (for Election RPC handling)
6. wire AnnounceLeader callback into grpcServer
7. create heartbeat sender (callback fires bully.Start if leader dies)
8. start sender in senderCtx goroutine
9. start REST server (blocking)
```

### Two roles, two init paths

```go
if node.IsLeader() {
    // Seed node: skip sender, start detector immediately
    startLeaderDuties("")
} else {
    // Follower: create bully, start sender
    bully := election.NewBully(node, ring, func(deadLeaderID string) {
        startLeaderDuties(deadLeaderID)
    })
    grpcServer.SetBully(bully)
    sender := heartbeat.NewSender(node, engine, time.Second, func() {
        go bully.Start(node.LeaderID())
    })
    go sender.Start(senderCtx)
}
```

The seed node never needs a heartbeat sender or a bully — it is the leader from birth.

---

## 33. Bug Fix: Double Election Guard

**The bug:**

When node-c won election (term=1), it set itself as leader. Then node-b's slightly-later `Election` RPC arrived at node-c. The handler didn't know node-c was already leader, so it started a second election with `deadLeaderID = "node-c"` (the current leader ID at that point — itself!). This caused `ring.Remove("node-c")` — node-c removed itself from its own ring.

**Symptom in logs:**
```
[node-c] starting bully election (term=2, dead=node-c)   ← node-c is the dead leader??
[node-c] removed dead leader node-c from ring             ← ring now missing node-c
```

**The fix** — one `IsLeader` guard at the top of the `Election` handler:
```go
func (s *Server) Election(ctx context.Context, req *pb.ElectionRequest) (*pb.ElectionResponse, error) {
    if s.node.IsLeader() {
        return &pb.ElectionResponse{Yield: false}, nil  // just refuse, no new election
    }
    // ... rest of handler
}
```

**Why this works:**
- If we're already leader, no election needed. We just refuse the incoming request.
- The requesting node (node-b) gets `yield=false`, which means "a higher node is alive and active" — it stands down, which is exactly correct.

**Lesson:** State checks at the top of RPC handlers are cheap insurance. Distributed systems have race windows you don't see until you run the demo.

---

## 34. Full Demo Output — Leader Election

**Setup:** 3-node cluster. node-a is the initial leader.

```bash
# Build and start cluster
make build
NODE_ID=node-a GRPC_ADDR=:9090 REST_ADDR=:8080 ./sentinel-node &
NODE_ID=node-b GRPC_ADDR=:9091 REST_ADDR=:8081 SEED_ADDR=:9090 ./sentinel-node &
NODE_ID=node-c GRPC_ADDR=:9092 REST_ADDR=:8082 SEED_ADDR=:9090 ./sentinel-node &
```

**Step 1 — Write a key while all nodes are up:**
```bash
# Write to node-b (the primary for key "session:1")
curl -s -X POST localhost:8081/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"session:1","value":"token-abc"}'
# → {"ok":true,"primary":"node-b","replica":"node-c","written_by":"node-b"}

# Verify replica also has it
curl -s localhost:8082/get/session:1
# → {"key":"session:1","served_by":"node-c","value":"token-abc"}
```

**Step 2 — Kill node-a (the leader):**
```bash
kill $PID_A
# IMPORTANT: must use `go build` + run binary directly for kill to work
# `go run` starts a wrapper process; kill kills the wrapper, not the binary
```

**Step 3 — Watch the election unfold:**
```
# node-b detects leader is dead (3 consecutive failed heartbeat streams)
[node-b] heartbeat stream lost — failure 1/3
[node-b] heartbeat stream lost — failure 2/3
[node-b] heartbeat stream lost — failure 3/3
[node-b] leader appears dead — triggering election

# node-b starts election, contacts node-c (higher ID)
[node-b] starting bully election (term=1, dead=node-a)
[node-c] ← Election from node-b
# node-c has higher ID → refuses, starts its own election
[node-c] outranks node-b — starting own election (dead=node-a)
[node-c] starting bully election (term=1, dead=node-a)
[node-c] no higher peers — winning election immediately
[node-c] *** WON ELECTION (term=1) — new leader ***

# node-b receives AnnounceLeader from node-c
[node-b] higher node taking over — standing down
[node-b] ← AnnounceLeader: new leader=node-c (replaced node-a, term=1)
[node-b] updated leader → node-c
[node-b] heartbeat stream open → :9092   ← now heartbeating to node-c

# node-c transitions to leader role
[node-c] removed dead leader node-a from ring
[node-c] *** LEADER — failure detector started ***
[node-c] ♥ heartbeat from node-b
```

**Step 4 — Verify cluster recovered:**
```bash
curl -s localhost:8082/cluster/status
# {
#   "leader_id": "node-c",
#   "node_id":   "node-c",
#   "key_count": 1,
#   "peers": [
#     {"id":"node-b","status":"healthy"},
#     {"id":"node-a","status":"dead"}
#   ]
# }
```

**Step 5 — Data is still there (replica survived):**
```bash
curl -s localhost:8082/get/session:1
# → {"key":"session:1","served_by":"node-c","value":"token-abc"}  ✓
```

**Timeline:**
```
t=0   node-a dies
t=2s  stream 1 fails (2s backoff)
t=4s  stream 2 fails (2s backoff)
t=6s  stream 3 fails → election triggered
t=7s  node-c wins, announces to node-b
t=8s  node-b reconnects heartbeat to node-c
t=8s  cluster fully operational again
```

Total recovery time: ~8 seconds. No human intervention.

---

*Build log complete. Every system: cache engine, REST API, consistent hashing, gRPC, heartbeat detection, replication, failover, leader election — documented from first line to final demo.*

---

## 35. Interview Prep — What to Lead With

This section is not about the code. It is about what to say when an interviewer asks "tell me about a project you built."

---

### The one-sentence pitch

> "I built a self-healing distributed in-memory cache from scratch in Go — consistent hashing, synchronous gRPC replication, heartbeat-based failure detection, and a bully leader election algorithm. The whole thing is live-demonstrable: `docker stop node-a` and the cluster elects a new leader, removes the dead node from the ring, and keeps serving requests — about 8 seconds, no human intervention."

That last sentence is the demo. Lead every explanation toward it.

---

### What's genuinely strong — lean into these

**1. Import-cycle avoidance via interfaces (dependency inversion)**

The bully election package and the gRPC server both needed to reference each other, which would create a compile-time cycle. The fix was to define a narrow interface in `grpc/server.go`:

```go
type electionStarter interface {
    Start(deadLeaderID string)
}
```

`*election.Bully` satisfies this interface implicitly (Go duck typing). Neither `grpc` nor `heartbeat` import `election` — `main.go` wires them together with callbacks. This is the **Dependency Inversion Principle** applied to a real architectural constraint, not a textbook exercise. Most candidates at junior-to-mid level would have reached for a global variable or a circular import workaround. Talk about this when asked about Go-specific design decisions.

**2. The gRPC-internal / REST-external split**

This is the pattern etcd, CockroachDB, and Kubernetes actually use. gRPC for cluster-internal traffic (typed contracts, streaming, binary) and REST for client-facing traffic (human-readable, easy to curl). If an interviewer asks "why not just use REST for everything?", the answer is: streaming heartbeats over persistent connections — you can't do bidirectional streaming efficiently over polling.

**3. Consistent hashing with virtual nodes**

150 virtual nodes per physical node using MD5, sorted ring, binary search for O(log N) lookup. The key insight virtual nodes solve: without them, nodes end up with very uneven key distribution (especially with few nodes). With 150 vnodes, the variance in load per node drops dramatically. The ring implementation also supports `GetReplica(key, n)` to find the Nth distinct physical node clockwise — that is how we pick primary and replica without separate data structures.

**4. O(1) LRU via doubly linked list + hashmap**

Standard interview question, but you actually built it — not with `lru.New()` from a library. The list gives O(1) move-to-front and O(1) eviction of the tail. The map gives O(1) lookup. Neither alone is sufficient. This is the exact structure Redis uses internally for its LRU approximation.

**5. The double-election race bug and the fix**

This is the most impressive thing to bring up unprompted because most candidates wouldn't have caught it:

> "When node-c won the election, it immediately set itself as leader. But node-b's slightly-later Election RPC arrived at node-c *after* that state change. Without a guard, node-c would start a second election with `deadLeaderID = 'node-c'` — its own ID — and call `ring.Remove('node-c')`, removing itself from its own ring. I caught this during the demo, traced it to the race window, and fixed it with a single `IsLeader()` guard at the top of the Election RPC handler."

This demonstrates that you ran the system end-to-end and understood the failure mode — not just "I wrote the code and it compiled."

---

### Likely interview questions and honest answers

**"Is this production-ready?"**
No, and intentionally so. This is a learning project that demonstrates the concepts behind Redis Cluster, DynamoDB, and Cassandra. Known limitations: no TLS, no persistence, connection-per-replication-write (should be a pool), bully election is not safe under network partitions. A production system would use Raft or Paxos for consensus. I know those limitations because I built the thing and hit some of them.

**"Walk me through what happens when a node dies."**
1. The dead node's heartbeat stream to the leader breaks.
2. After 5 seconds without an update, the leader's failure detector fires `onDead`.
3. The leader calls `ring.Remove(deadID)` — keys now route to the next node clockwise.
4. The leader broadcasts a `Promote` RPC to all healthy peers — they remove the dead node from their own rings.
5. If the dead node *was* the leader, the heartbeat sender on follower nodes starts failing. After 3 consecutive stream failures (~6 seconds), `onLeaderDead` fires, triggering a bully election. The highest-ID available node wins in one round-trip and broadcasts `AnnounceLeader`.

**"Why bully and not Raft?"**
Bully is O(N) messages and easy to reason about. Raft requires a log, term voting, and quorum mechanics — significant implementation complexity. For a project whose goal is to demonstrate distributed systems concepts end-to-end, bully gives you a working, demonstrable result. I know its limitations: it can elect two leaders during a network partition (split-brain). That is why the README lists Raft as a Version 3 enhancement.

**"What would you change first if you had another week?"**
Connection pooling in the replicator (right now it dials a new TCP connection per write), and a real integration test for the full failover sequence (right now failover is tested manually). Those two changes would take it from "impressive demo" to "something I'd be comfortable stress-testing."

---

### Numbers to remember

| Metric | Value |
|---|---|
| Virtual nodes per physical node | 150 |
| LRU eviction complexity | O(1) get and evict |
| Heartbeat interval | 1 second |
| Failure detection timeout | 5 seconds |
| Election trigger (failed streams) | 3 consecutive failures |
| Approximate failover time | ~8 seconds end-to-end |
| Test coverage | cache engine, consistent ring, election logic, replication RPC |
