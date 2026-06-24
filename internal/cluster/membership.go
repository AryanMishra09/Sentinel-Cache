package cluster

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/aryan-mishra/sentinel-cache/proto/gen"
)

const (
	joinMaxAttempts = 15
	joinRetryDelay  = 2 * time.Second
)

// JoinCluster dials the seed node, registers this node, and bootstraps the
// local peer list and hash ring from the response.
//
// It retries up to joinMaxAttempts times with joinRetryDelay between each
// attempt — the seed may not be ready yet when Docker starts this container.
//
// The gRPC server must be running before calling this — once we are registered,
// the seed (and eventually other nodes) will start dialing us back.
func JoinCluster(node *Node, ring *Ring, seedAddr string) error {
	var lastErr error
	for attempt := 1; attempt <= joinMaxAttempts; attempt++ {
		if err := tryJoin(node, ring, seedAddr); err != nil {
			lastErr = err
			fmt.Printf("[%s] join attempt %d/%d failed (%v) — retrying in %s\n",
				node.ID, attempt, joinMaxAttempts, err, joinRetryDelay)
			time.Sleep(joinRetryDelay)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to join cluster after %d attempts: %w", joinMaxAttempts, lastErr)
}

func tryJoin(node *Node, ring *Ring, seedAddr string) error {
	conn, err := grpc.NewClient(seedAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial seed %s: %w", seedAddr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := pb.NewClusterServiceClient(conn)
	resp, err := client.Join(ctx, &pb.JoinRequest{
		NodeId:   node.ID,
		RestAddr: node.RESTAddr, // advertise address — what peers should dial
		GrpcAddr: node.GRPCAddr, // advertise address
	})
	if err != nil {
		return fmt.Errorf("join rpc: %w", err)
	}

	// Bootstrap our view of the cluster from the seed's response.
	for _, p := range resp.Peers {
		if p.NodeId == node.ID {
			continue // skip ourselves
		}
		node.AddPeer(&PeerInfo{
			ID:       p.NodeId,
			RESTAddr: p.RestAddr,
			GRPCAddr: p.GrpcAddr,
			Status:   Status(p.Status),
		})
		ring.Add(p.NodeId)
	}
	node.SetLeader(resp.LeaderId)

	fmt.Printf("[%s] joined cluster via %s — %d peers known, leader=%q\n",
		node.ID, seedAddr, len(resp.Peers)-1, resp.LeaderId)

	// Announce ourselves to every peer we just learned about.
	//
	// The seed only knows about nodes that were already in the cluster when
	// we joined. Those earlier nodes have never heard of us, so their rings
	// are missing our entry. We call Join on each of them so they add us.
	//
	// Example: node-b joins first (ring: node-a, node-b). Later node-c joins
	// and learns about node-b from the seed response. node-c then calls Join
	// on node-b → node-b adds node-c → all three rings are now consistent.
	for _, p := range resp.Peers {
		if p.NodeId == node.ID {
			continue
		}
		go announceToExistingPeer(node, p.GrpcAddr)
	}

	return nil
}

// announceToExistingPeer dials an already-running peer and sends a JoinRequest
// on behalf of this node. The peer's Join handler will add this node to its
// ring and peer list, keeping all rings consistent as new nodes arrive.
func announceToExistingPeer(node *Node, peerGRPCAddr string) {
	conn, err := grpc.NewClient(peerGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := pb.NewClusterServiceClient(conn)
	// Response is ignored — we just need the peer to register us.
	_, _ = client.Join(ctx, &pb.JoinRequest{
		NodeId:   node.ID,
		RestAddr: node.RESTAddr,
		GrpcAddr: node.GRPCAddr,
	})
	fmt.Printf("[%s] announced to existing peer %s\n", node.ID, peerGRPCAddr)
}
