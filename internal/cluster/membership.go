package cluster

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/aryan-mishra/sentinel-cache/proto/gen"
)

// JoinCluster dials the seed node, registers this node, and bootstraps the
// local peer list and hash ring from the response.
//
// The gRPC server must be running before calling this — once we are registered,
// the seed (and eventually other nodes) will start dialing us back.
func JoinCluster(node *Node, ring *Ring, seedAddr string) error {
	// insecure.NewCredentials() = plaintext TCP, no TLS.
	// Fine inside a Docker network; use mTLS in production.
	conn, err := grpc.NewClient(seedAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial seed %s: %w", seedAddr, err)
	}
	defer conn.Close()

	client := pb.NewClusterServiceClient(conn)
	resp, err := client.Join(context.Background(), &pb.JoinRequest{
		NodeId:   node.ID,
		RestAddr: node.RESTAddr,
		GrpcAddr: node.GRPCAddr,
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
	return nil
}
