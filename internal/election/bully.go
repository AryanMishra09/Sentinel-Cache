package election

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aryan-mishra/sentinel-cache/internal/cluster"
	pb "github.com/aryan-mishra/sentinel-cache/proto/gen"
)

// Bully implements the bully leader election algorithm.
//
// When triggered, it sends Election RPCs to all peers with a higher node ID.
// If every higher peer yields (or is unreachable), this node wins and calls onWin.
// If any higher peer refuses to yield, that peer runs its own election instead.
//
// Node ID comparison is lexicographic: "node-c" > "node-b" > "node-a".
// The highest-available node always wins.
type Bully struct {
	node  *cluster.Node
	ring  *cluster.Ring
	onWin func(deadLeaderID string) // called by the winner with the old leader's ID

	mu     sync.Mutex
	active bool      // true while an election is in progress
	term   int64     // monotonically increasing; prevents stale election messages
}

func NewBully(node *cluster.Node, ring *cluster.Ring, onWin func(string)) *Bully {
	return &Bully{node: node, ring: ring, onWin: onWin}
}

// Start initiates an election because deadLeaderID is believed to be dead.
// Safe to call multiple times concurrently — only one election runs at a time.
func (b *Bully) Start(deadLeaderID string) {
	b.mu.Lock()
	if b.active {
		b.mu.Unlock()
		fmt.Printf("[%s] election already in progress, skipping\n", b.node.ID)
		return
	}
	b.active = true
	b.term++
	term := b.term
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.active = false
		b.mu.Unlock()
	}()

	fmt.Printf("[%s] starting bully election (term=%d, dead=%s)\n",
		b.node.ID, term, deadLeaderID)

	// Collect peers with a strictly higher node ID.
	higher := b.higherPeers()

	if len(higher) == 0 {
		// No higher peer exists — we win immediately.
		fmt.Printf("[%s] no higher peers — winning election immediately\n", b.node.ID)
		b.becomeLeader(deadLeaderID, term)
		return
	}

	// Send Election to every higher peer concurrently.
	// Each one responds: yield=true (I yield) or yield=false (I'm taking over).
	anyTakingOver := false
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, peer := range higher {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			yield := b.sendElection(addr, term, deadLeaderID)
			if !yield {
				mu.Lock()
				anyTakingOver = true
				mu.Unlock()
			}
		}(peer.GRPCAddr)
	}

	wg.Wait()

	if anyTakingOver {
		// A higher node is alive and running its own election.
		fmt.Printf("[%s] higher node taking over — standing down\n", b.node.ID)
		return
	}

	// All higher peers yielded or were unreachable — we win.
	b.becomeLeader(deadLeaderID, term)
}

// ── Internal helpers ─────────────────────────────────────────────────────────

func (b *Bully) higherPeers() []*cluster.PeerInfo {
	var out []*cluster.PeerInfo
	for _, p := range b.node.Peers() {
		if p.ID > b.node.ID && p.Status != cluster.StatusDead {
			out = append(out, p)
		}
	}
	return out
}

// sendElection dials addr and sends an Election RPC.
// Returns true if the peer yields (or is unreachable — treat as yielded).
func (b *Bully) sendElection(addr string, term int64, deadLeaderID string) bool {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return true // unreachable = treat as yielded
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client := pb.NewClusterServiceClient(conn)
	resp, err := client.Election(ctx, &pb.ElectionRequest{
		CandidateId: b.node.ID,
		Term:        term,
	})
	if err != nil {
		return true // no response = treat as yielded
	}
	return resp.Yield
}

func (b *Bully) becomeLeader(deadLeaderID string, term int64) {
	fmt.Printf("[%s] *** WON ELECTION (term=%d) — new leader ***\n", b.node.ID, term)

	// Announce victory to all live peers.
	for _, peer := range b.node.Peers() {
		if peer.ID != b.node.ID && peer.Status != cluster.StatusDead {
			go b.sendAnnounce(peer.GRPCAddr, deadLeaderID, term)
		}
	}

	// Fire the callback — wired in main.go to set up leader responsibilities.
	b.onWin(deadLeaderID)
}

func (b *Bully) sendAnnounce(addr, deadLeaderID string, term int64) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client := pb.NewClusterServiceClient(conn)
	_, _ = client.AnnounceLeader(ctx, &pb.AnnounceLeaderRequest{
		LeaderId:     b.node.ID,
		DeadLeaderId: deadLeaderID,
		Term:         term,
	})
}
