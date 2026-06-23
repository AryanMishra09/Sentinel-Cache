package cluster

import (
	"fmt"
	"testing"
)

func TestGetNodeBasic(t *testing.T) {
	r := NewRing(0)
	r.Add("node-a")
	r.Add("node-b")
	r.Add("node-c")

	// Every key must resolve to one of the three nodes.
	nodes := map[string]bool{"node-a": true, "node-b": true, "node-c": true}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("user:%d", i)
		got := r.GetNode(key)
		if !nodes[got] {
			t.Fatalf("key %q mapped to unknown node %q", key, got)
		}
	}
}

func TestConsistencyAfterNodeJoin(t *testing.T) {
	r := NewRing(150)
	r.Add("node-a")
	r.Add("node-b")
	r.Add("node-c")

	// Record which node owns each key before adding node-d.
	const total = 1000
	before := make(map[string]string, total)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key:%d", i)
		before[key] = r.GetNode(key)
	}

	r.Add("node-d")

	// Count how many keys moved after adding node-d.
	moved := 0
	for key, oldNode := range before {
		if r.GetNode(key) != oldNode {
			moved++
		}
	}

	// With consistent hashing, roughly 1/4 of keys should move (1/N where N=4).
	// We allow a generous 10–40% window to account for variance.
	pct := float64(moved) / float64(total) * 100
	t.Logf("keys moved after adding node-d: %d/%d (%.1f%%)", moved, total, pct)
	if pct < 10 || pct > 40 {
		t.Errorf("expected ~25%% of keys to move, got %.1f%%", pct)
	}
}

func TestRemoveNodeReroutes(t *testing.T) {
	r := NewRing(150)
	r.Add("node-a")
	r.Add("node-b")
	r.Add("node-c")

	// After removing node-b, no key should resolve to node-b.
	r.Remove("node-b")

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("user:%d", i)
		got := r.GetNode(key)
		if got == "node-b" {
			t.Fatalf("key %q still resolves to removed node-b", key)
		}
	}
}

func TestGetReplicaReturnsDifferentNode(t *testing.T) {
	r := NewRing(150)
	r.Add("node-a")
	r.Add("node-b")
	r.Add("node-c")

	key := "user:1"
	primary := r.GetReplica(key, 1)
	replica := r.GetReplica(key, 2)

	if primary == "" || replica == "" {
		t.Fatal("expected both primary and replica to be non-empty")
	}
	if primary == replica {
		t.Fatalf("primary and replica should be different nodes, both got %q", primary)
	}
}

func TestDistributionIsReasonablyEven(t *testing.T) {
	r := NewRing(150)
	r.Add("node-a")
	r.Add("node-b")
	r.Add("node-c")

	counts := map[string]int{}
	const total = 3000
	for i := 0; i < total; i++ {
		n := r.GetNode(fmt.Sprintf("key:%d", i))
		counts[n]++
	}

	// Each node should own roughly 1000 keys (33%). Allow ±15%.
	for node, count := range counts {
		pct := float64(count) / float64(total) * 100
		t.Logf("%s owns %d keys (%.1f%%)", node, count, pct)
		if pct < 20 || pct > 46 {
			t.Errorf("node %s has uneven distribution: %.1f%%", node, pct)
		}
	}
}
