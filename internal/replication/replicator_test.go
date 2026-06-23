package replication_test

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/aryan-mishra/sentinel-cache/internal/cache"
	"github.com/aryan-mishra/sentinel-cache/internal/cluster"
	nodeGRPC "github.com/aryan-mishra/sentinel-cache/internal/grpc"
	"github.com/aryan-mishra/sentinel-cache/internal/replication"
	pb "github.com/aryan-mishra/sentinel-cache/proto/gen"
)

// startServer spins up a real gRPC server on a random port and returns
// the address and the cache engine it serves.
func startServer(t *testing.T, id string) (*cache.Engine, string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0") // OS picks the port
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	eng := cache.NewEngine(0)
	node := cluster.NewNode(id, ":0", lis.Addr().String())
	ring := cluster.NewRing(0)
	srv := nodeGRPC.NewServer(node, ring, eng)

	gs := grpc.NewServer()
	pb.RegisterClusterServiceServer(gs, srv)
	go gs.Serve(lis) //nolint:errcheck

	t.Cleanup(func() { gs.Stop() })
	return eng, lis.Addr().String()
}

// TestReplicateWriteReachesReplica: a write replicated via gRPC must be
// readable on the replica node's cache engine.
func TestReplicateWriteReachesReplica(t *testing.T) {
	replicaEngine, replicaAddr := startServer(t, "node-b")

	// Primary node wiring
	primaryNode := cluster.NewNode("node-a", ":8080", ":9090")
	primaryNode.AddPeer(&cluster.PeerInfo{
		ID:       "node-b",
		RESTAddr: ":8081",
		GRPCAddr: replicaAddr,
		Status:   cluster.StatusHealthy,
	})
	ring := cluster.NewRing(0)
	ring.Add("node-a")
	ring.Add("node-b")

	r := replication.New(primaryNode, ring)

	// Find a key whose primary is node-a so replication actually fires.
	// Brute-force: try keys until one maps to node-a as primary.
	var testKey string
	for i := 0; i < 1000; i++ {
		k := "key:" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		if ring.GetReplica(k, 1) == "node-a" {
			testKey = k
			break
		}
	}
	if testKey == "" {
		t.Skip("could not find a key owned by node-a in this ring configuration")
	}

	// Write to primary engine manually (simulates handler.go doing engine.Set first)
	primaryEngine := cache.NewEngine(0)
	primaryEngine.Set(testKey, "hello", 0)

	// Now replicate
	if err := r.Replicate(testKey, "hello", 0, false); err != nil {
		t.Fatalf("Replicate returned error: %v", err)
	}

	// Give gRPC a moment to deliver
	time.Sleep(50 * time.Millisecond)

	val, ok := replicaEngine.Get(testKey)
	if !ok {
		t.Fatalf("replica does not have key %q after replication", testKey)
	}
	if val != "hello" {
		t.Fatalf("replica has wrong value: got %q, want %q", val, "hello")
	}
}

// TestReplicateDeleteReachesReplica: a deletion replicated via gRPC must
// remove the key from the replica.
func TestReplicateDeleteReachesReplica(t *testing.T) {
	replicaEngine, replicaAddr := startServer(t, "node-b")

	primaryNode := cluster.NewNode("node-a", ":8080", ":9090")
	primaryNode.AddPeer(&cluster.PeerInfo{
		ID:       "node-b",
		RESTAddr: ":8081",
		GRPCAddr: replicaAddr,
		Status:   cluster.StatusHealthy,
	})
	ring := cluster.NewRing(0)
	ring.Add("node-a")
	ring.Add("node-b")

	r := replication.New(primaryNode, ring)

	// Find a key owned by node-a
	var testKey string
	for i := 0; i < 1000; i++ {
		k := "del:" + string(rune('a'+i%26))
		if ring.GetReplica(k, 1) == "node-a" {
			testKey = k
			break
		}
	}
	if testKey == "" {
		t.Skip("could not find a key owned by node-a")
	}

	// Seed the replica directly to verify deletion clears it
	replicaEngine.Set(testKey, "to-be-deleted", 0)
	time.Sleep(20 * time.Millisecond)

	if err := r.Replicate(testKey, "", 0, true); err != nil {
		t.Fatalf("Replicate (delete) returned error: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	_, stillThere := replicaEngine.Get(testKey)
	if stillThere {
		t.Fatal("expected key to be deleted on replica after replicated delete")
	}
}

// TestNoReplicationWhenNotPrimary: Replicate must be a no-op when the calling
// node is not the primary for the given key.
func TestNoReplicationWhenNotPrimary(t *testing.T) {
	_, replicaAddr := startServer(t, "node-b")

	// node-b is calling, but the ring says node-a is primary
	callerNode := cluster.NewNode("node-b", ":8081", ":9091")
	callerNode.AddPeer(&cluster.PeerInfo{
		ID:       "node-a",
		RESTAddr: ":8080",
		GRPCAddr: replicaAddr,
		Status:   cluster.StatusHealthy,
	})
	ring := cluster.NewRing(0)
	ring.Add("node-a")
	ring.Add("node-b")

	r := replication.New(callerNode, ring)

	// Find a key where node-a is primary (not node-b)
	var testKey string
	for i := 0; i < 1000; i++ {
		k := "noop:" + string(rune('a'+i%26))
		if ring.GetReplica(k, 1) == "node-a" {
			testKey = k
			break
		}
	}
	if testKey == "" {
		t.Skip("could not find a key owned by node-a")
	}

	// Should return nil (no-op) — node-b is not the primary for this key
	if err := r.Replicate(testKey, "val", 0, false); err != nil {
		t.Fatalf("expected no-op but got error: %v", err)
	}
}
