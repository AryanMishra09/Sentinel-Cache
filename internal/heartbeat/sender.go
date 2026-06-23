package heartbeat

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aryan-mishra/sentinel-cache/internal/cache"
	"github.com/aryan-mishra/sentinel-cache/internal/cluster"
	pb "github.com/aryan-mishra/sentinel-cache/proto/gen"
)

const maxReconnectFailures = 3

// Sender runs on every non-leader node.
// It maintains a persistent gRPC stream to the leader, sending one
// HeartbeatRequest per second. If the stream breaks, it reconnects.
// After maxReconnectFailures consecutive failures, onLeaderDead is called
// to trigger a leader election.
type Sender struct {
	node         *cluster.Node
	engine       *cache.Engine
	interval     time.Duration
	onLeaderDead func() // called when the leader appears to be permanently down
}

func NewSender(node *cluster.Node, engine *cache.Engine, interval time.Duration, onLeaderDead func()) *Sender {
	return &Sender{node: node, engine: engine, interval: interval, onLeaderDead: onLeaderDead}
}

// Start runs the send loop forever. Call in a goroutine.
func (s *Sender) Start(ctx context.Context) {
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if s.node.IsLeader() {
			// This node won an election and is now the leader — stop sending.
			return
		}

		leaderAddr := s.node.LeaderGRPCAddr()
		if leaderAddr == "" {
			time.Sleep(time.Second)
			continue
		}

		if err := s.stream(ctx, leaderAddr); err != nil {
			failures++
			fmt.Printf("[%s] heartbeat stream lost (%v) — failure %d/%d\n",
				s.node.ID, err, failures, maxReconnectFailures)

			if failures >= maxReconnectFailures && s.onLeaderDead != nil {
				fmt.Printf("[%s] leader appears dead — triggering election\n", s.node.ID)
				failures = 0
				s.onLeaderDead()
			}
			time.Sleep(2 * time.Second)
		} else {
			failures = 0 // successful stream resets the counter
		}
	}
}

// stream opens one gRPC connection to the leader and pumps heartbeats until
// the context is cancelled or the stream breaks.
func (s *Sender) stream(ctx context.Context, leaderAddr string) error {
	conn, err := grpc.NewClient(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial leader %s: %w", leaderAddr, err)
	}
	defer conn.Close()

	client := pb.NewClusterServiceClient(conn)
	hbStream, err := client.Heartbeat(ctx)
	if err != nil {
		return fmt.Errorf("open heartbeat stream: %w", err)
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	fmt.Printf("[%s] heartbeat stream open → %s\n", s.node.ID, leaderAddr)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := hbStream.Send(&pb.HeartbeatRequest{
				NodeId:   s.node.ID,
				Status:   "healthy",
				Timestamp: time.Now().Unix(),
				KeyCount: int64(s.engine.KeyCount()),
			})
			if err != nil {
				return fmt.Errorf("send heartbeat: %w", err)
			}
		}
	}
}
