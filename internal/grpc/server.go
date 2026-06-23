package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/aryan-mishra/sentinel-cache/internal/cache"
	"github.com/aryan-mishra/sentinel-cache/internal/cluster"
	"github.com/aryan-mishra/sentinel-cache/internal/heartbeat"
	pb "github.com/aryan-mishra/sentinel-cache/proto/gen"
)

// electionStarter is a narrow interface so server.go does not import the
// election package (which would create a cycle through main.go).
type electionStarter interface {
	Start(deadLeaderID string)
}

// Server implements ClusterServiceServer — the gRPC server every node runs
// to accept incoming cluster calls from peers.
type Server struct {
	pb.UnimplementedClusterServiceServer
	node     *cluster.Node
	ring     *cluster.Ring
	engine   *cache.Engine
	detector *heartbeat.Detector  // set on leader nodes only
	bully    electionStarter       // set on follower nodes only
	onAnnounce func(newLeaderID, deadLeaderID string) // wired in main.go
}

func NewServer(node *cluster.Node, ring *cluster.Ring, engine *cache.Engine) *Server {
	return &Server{node: node, ring: ring, engine: engine}
}

func (s *Server) SetDetector(d *heartbeat.Detector)                    { s.detector = d }
func (s *Server) SetBully(b electionStarter)                           { s.bully = b }
func (s *Server) SetAnnounceHandler(fn func(newID, deadID string))     { s.onAnnounce = fn }

// Listen starts the gRPC server on addr and blocks until it stops.
func (s *Server) Listen(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc listen %s: %w", addr, err)
	}
	gs := grpc.NewServer()
	pb.RegisterClusterServiceServer(gs, s)
	fmt.Printf("[%s] gRPC server listening on %s\n", s.node.ID, addr)
	return gs.Serve(lis)
}

// ── Membership RPCs ──────────────────────────────────────────────────────────

// Join registers a new node into the cluster.
// The joining node learns about all existing peers from the response.
func (s *Server) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	fmt.Printf("[%s] ← Join from %s (%s)\n", s.node.ID, req.NodeId, req.GrpcAddr)

	// Pre-seed the detector so the timeout clock starts from join time,
	// not from the first heartbeat. If the node never sends a heartbeat,
	// it will be detected as dead after the timeout regardless.
	if s.detector != nil {
		s.detector.UpdateLastSeen(req.NodeId)
	}

	// Add the joiner to our state.
	s.node.AddPeer(&cluster.PeerInfo{
		ID:       req.NodeId,
		RESTAddr: req.RestAddr,
		GRPCAddr: req.GrpcAddr,
		Status:   cluster.StatusHealthy,
	})
	s.ring.Add(req.NodeId)

	// Build response: all known peers + ourselves.
	peers := s.node.Peers()
	pbPeers := make([]*pb.NodeInfo, 0, len(peers)+1)
	for _, p := range peers {
		pbPeers = append(pbPeers, &pb.NodeInfo{
			NodeId:   p.ID,
			RestAddr: p.RESTAddr,
			GrpcAddr: p.GRPCAddr,
			Status:   string(p.Status),
		})
	}
	// Include self so the joiner can add us to its ring.
	pbPeers = append(pbPeers, &pb.NodeInfo{
		NodeId:   s.node.ID,
		RestAddr: s.node.RESTAddr,
		GrpcAddr: s.node.GRPCAddr,
		Status:   string(cluster.StatusHealthy),
	})

	return &pb.JoinResponse{
		Peers:    pbPeers,
		LeaderId: s.node.LeaderID(),
	}, nil
}

// Leave removes a node from the cluster gracefully.
func (s *Server) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	fmt.Printf("[%s] ← Leave from %s\n", s.node.ID, req.NodeId)
	s.node.RemovePeer(req.NodeId)
	s.ring.Remove(req.NodeId)
	return &pb.LeaveResponse{Success: true}, nil
}

// ── Stubs — implemented in later phases ─────────────────────────────────────

// Replicate writes or deletes a key on this node as instructed by the primary.
func (s *Server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	if req.IsDelete {
		s.engine.Delete(req.Key)
	} else {
		ttl := time.Duration(req.TtlSeconds) * time.Second
		s.engine.Set(req.Key, req.Value, ttl)
	}
	fmt.Printf("[%s] ← Replicated key=%q isDelete=%v\n", s.node.ID, req.Key, req.IsDelete)
	return &pb.ReplicateResponse{Success: true}, nil
}

// Promote is sent by the leader during failover.
// It means: "remove the dead node from your ring so you start serving its keys."
func (s *Server) Promote(ctx context.Context, req *pb.PromoteRequest) (*pb.PromoteResponse, error) {
	fmt.Printf("[%s] ← Promote: removing dead node %s, now routing its keys to next ring node\n",
		s.node.ID, req.NodeId)
	s.ring.Remove(req.NodeId)
	s.node.MarkPeerStatus(req.NodeId, cluster.StatusDead)
	return &pb.PromoteResponse{Success: true}, nil
}

// Election implements the bully algorithm server side.
// If the candidate has a higher ID than us → we yield.
// If we have a higher ID → we refuse and start our own election.
func (s *Server) Election(ctx context.Context, req *pb.ElectionRequest) (*pb.ElectionResponse, error) {
	fmt.Printf("[%s] ← Election from %s\n", s.node.ID, req.CandidateId)

	// If we already won an election, just refuse — no need to start another.
	if s.node.IsLeader() {
		return &pb.ElectionResponse{Yield: false}, nil
	}

	if req.CandidateId > s.node.ID {
		// Candidate outranks us — yield.
		return &pb.ElectionResponse{Yield: true}, nil
	}

	// We outrank the candidate — refuse and start our own election.
	// Capture the dead leader ID BEFORE any state changes so we remove
	// the right node from the ring.
	deadLeader := s.node.LeaderID()
	fmt.Printf("[%s] outranks %s — starting own election (dead=%s)\n",
		s.node.ID, req.CandidateId, deadLeader)
	if s.bully != nil {
		go s.bully.Start(deadLeader)
	}
	return &pb.ElectionResponse{Yield: false}, nil
}

// AnnounceLeader is called by the election winner to notify us of the new leader.
func (s *Server) AnnounceLeader(ctx context.Context, req *pb.AnnounceLeaderRequest) (*pb.AnnounceLeaderResponse, error) {
	fmt.Printf("[%s] ← AnnounceLeader: new leader=%s (replaced %s, term=%d)\n",
		s.node.ID, req.LeaderId, req.DeadLeaderId, req.Term)
	if s.onAnnounce != nil {
		s.onAnnounce(req.LeaderId, req.DeadLeaderId)
	}
	return &pb.AnnounceLeaderResponse{Ack: true}, nil
}

// Heartbeat is a bidirectional stream. The sender pumps HeartbeatRequests;
// we ACK each one and record the timestamp in the detector.
func (s *Server) Heartbeat(stream pb.ClusterService_HeartbeatServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil // sender closed the stream cleanly
		}
		if err != nil {
			return err // network error — the sender will reconnect
		}

		fmt.Printf("[%s] ♥ heartbeat from %s\n", s.node.ID, req.NodeId)
		if s.detector != nil {
			s.detector.UpdateLastSeen(req.NodeId)
		}

		if err := stream.Send(&pb.HeartbeatResponse{Ack: true}); err != nil {
			return err
		}
	}
}
