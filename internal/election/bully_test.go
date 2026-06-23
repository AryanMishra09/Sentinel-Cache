package election

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aryan-mishra/sentinel-cache/internal/cluster"
)

// helpers ────────────────────────────────────────────────────────────────────

func makeNode(id string) *cluster.Node {
	return cluster.NewNode(id, ":0", ":0")
}

func addPeer(n *cluster.Node, id string) {
	n.AddPeer(&cluster.PeerInfo{
		ID:       id,
		RESTAddr: ":0",
		GRPCAddr: ":0",
		Status:   cluster.StatusHealthy,
	})
}

// tests ──────────────────────────────────────────────────────────────────────

// TestNoHigherPeers: when this node has the highest ID it should win immediately
// without sending any RPCs.
func TestNoHigherPeers(t *testing.T) {
	node := makeNode("node-z")
	addPeer(node, "node-a")
	addPeer(node, "node-b")
	ring := cluster.NewRing(0)

	var won int32
	bully := NewBully(node, ring, func(deadLeaderID string) {
		atomic.StoreInt32(&won, 1)
	})

	bully.Start("node-leader")

	if atomic.LoadInt32(&won) != 1 {
		t.Fatal("expected bully to win when it has the highest ID")
	}
}

// TestDeadPeersIgnored: dead peers should not count as "higher" peers.
// If the only higher peer is dead, we should still win.
func TestDeadPeersIgnored(t *testing.T) {
	node := makeNode("node-b")
	ring := cluster.NewRing(0)

	// node-c has a higher ID but is dead
	node.AddPeer(&cluster.PeerInfo{
		ID:       "node-c",
		RESTAddr: ":0",
		GRPCAddr: "127.0.0.1:19999", // nothing listening here
		Status:   cluster.StatusDead,
	})

	var won int32
	bully := NewBully(node, ring, func(_ string) {
		atomic.StoreInt32(&won, 1)
	})

	bully.Start("node-a")

	if atomic.LoadInt32(&won) != 1 {
		t.Fatal("expected win: the only higher peer is marked dead")
	}
}

// TestOnlyOneElectionRunsAtATime: concurrent Start calls must not run two
// elections in parallel — the active guard should drop the second.
func TestOnlyOneElectionRunsAtATime(t *testing.T) {
	node := makeNode("node-z")
	ring := cluster.NewRing(0)

	var callCount int32
	bully := NewBully(node, ring, func(_ string) {
		atomic.AddInt32(&callCount, 1)
		time.Sleep(50 * time.Millisecond) // hold the election open briefly
	})

	// Fire three concurrent elections
	done := make(chan struct{}, 3)
	for i := 0; i < 3; i++ {
		go func() {
			bully.Start("dead-leader")
			done <- struct{}{}
		}()
	}
	for i := 0; i < 3; i++ {
		<-done
	}

	if n := atomic.LoadInt32(&callCount); n != 1 {
		t.Fatalf("expected exactly 1 election win, got %d", n)
	}
}

// TestTermIncrementsEachElection: each new election should have a strictly
// higher term than the previous one.
func TestTermIncrementsEachElection(t *testing.T) {
	node := makeNode("node-z")
	ring := cluster.NewRing(0)

	bully := NewBully(node, ring, func(_ string) {})

	bully.Start("dead-1")
	term1 := bully.term

	bully.Start("dead-2")
	term2 := bully.term

	if term2 <= term1 {
		t.Fatalf("term should increase: term1=%d term2=%d", term1, term2)
	}
}

// TestDeadLeaderIDPassedToCallback: the dead leader ID provided to Start must
// flow through to the onWin callback unchanged.
func TestDeadLeaderIDPassedToCallback(t *testing.T) {
	node := makeNode("node-z")
	ring := cluster.NewRing(0)

	var gotDeadID string
	bully := NewBully(node, ring, func(deadLeaderID string) {
		gotDeadID = deadLeaderID
	})

	bully.Start("node-a")

	if gotDeadID != "node-a" {
		t.Fatalf("expected dead leader 'node-a', got %q", gotDeadID)
	}
}
