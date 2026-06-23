package failover

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aryan-mishra/sentinel-cache/internal/cluster"
	pb "github.com/aryan-mishra/sentinel-cache/proto/gen"
)

// BroadcastNodeDeath is called by the leader when a node is detected as dead.
// It removes the dead node from the leader's own ring, then tells every other
// healthy peer to do the same via the Promote RPC.
//
// After all nodes remove the dead node from their rings, reads and writes to
// that node's key range automatically route to the next node clockwise (the
// replica), which already holds the replicated data.
func BroadcastNodeDeath(deadID string, node *cluster.Node, ring *cluster.Ring) {
	fmt.Printf("[%s] failover: broadcasting death of %s to all peers\n", node.ID, deadID)

	// Update the leader's own ring first.
	ring.Remove(deadID)

	// Notify every other healthy peer so their rings also update.
	for _, peer := range node.Peers() {
		if peer.ID == deadID {
			continue
		}
		if peer.Status == cluster.StatusDead {
			continue
		}
		go notifyPeer(node.ID, peer.GRPCAddr, deadID)
	}
}

// notifyPeer dials a single peer and tells it to remove the dead node from its ring.
func notifyPeer(selfID, peerAddr, deadID string) {
	conn, err := grpc.NewClient(peerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("[%s] failover: could not dial %s: %v\n", selfID, peerAddr, err)
		return
	}
	defer conn.Close()

	client := pb.NewClusterServiceClient(conn)
	_, err = client.Promote(context.Background(), &pb.PromoteRequest{
		NodeId: deadID,
	})
	if err != nil {
		fmt.Printf("[%s] failover: Promote rpc to %s failed: %v\n", selfID, peerAddr, err)
	}
}
