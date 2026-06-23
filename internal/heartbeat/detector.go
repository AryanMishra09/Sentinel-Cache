package heartbeat

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aryan-mishra/sentinel-cache/internal/cluster"
)

// Detector runs on the leader node.
// It tracks the last time a heartbeat was received from each peer.
// If a node exceeds the timeout, onDead is called once per death event.
// If that node later recovers and sends heartbeats again, it is treated
// as alive — onDead will fire again if it times out a second time.
type Detector struct {
	mu       sync.Mutex
	lastSeen map[string]time.Time
	dead     map[string]bool // nodes already reported as dead this cycle
	timeout  time.Duration
	node     *cluster.Node
	onDead   func(nodeID string)
}

func NewDetector(timeout time.Duration, node *cluster.Node, onDead func(string)) *Detector {
	return &Detector{
		lastSeen: make(map[string]time.Time),
		dead:     make(map[string]bool),
		timeout:  timeout,
		node:     node,
		onDead:   onDead,
	}
}

// UpdateLastSeen records that nodeID sent a heartbeat right now.
// Called by the gRPC server's Heartbeat handler on every received message.
func (d *Detector) UpdateLastSeen(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastSeen[nodeID] = time.Now()
	// If this node was previously dead, it has recovered.
	if d.dead[nodeID] {
		delete(d.dead, nodeID)
		fmt.Printf("[%s] node %s has RECOVERED\n", d.node.ID, nodeID)
		d.node.MarkPeerStatus(nodeID, cluster.StatusHealthy)
	}
}

// Start runs the watchdog loop forever. Call in a goroutine.
func (d *Detector) Start(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.checkAll()
		}
	}
}

// checkAll scans all known nodes and fires onDead for any that have timed out.
func (d *Detector) checkAll() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	for nodeID, last := range d.lastSeen {
		elapsed := now.Sub(last)
		if elapsed > d.timeout && !d.dead[nodeID] {
			d.dead[nodeID] = true
			fmt.Printf("[%s] FAILURE DETECTED: %s silent for %v — marking dead\n",
				d.node.ID, nodeID, elapsed.Round(time.Millisecond))
			// Fire the callback outside the lock to avoid deadlocks if the
			// callback itself tries to acquire any lock.
			nodeID := nodeID // capture for goroutine
			go d.onDead(nodeID)
		}
	}
}
