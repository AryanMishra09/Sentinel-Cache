package replication

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aryan-mishra/sentinel-cache/internal/cluster"
	pb "github.com/aryan-mishra/sentinel-cache/proto/gen"
)

// Replicator forwards writes from the primary to the replica node.
// It is called after every local write. If this node is not the primary
// for the key, the call is a no-op.
type Replicator struct {
	node *cluster.Node
	ring *cluster.Ring
}

func New(node *cluster.Node, ring *cluster.Ring) *Replicator {
	return &Replicator{node: node, ring: ring}
}

// Replicate replicates a write or delete to the key's replica node.
// Best-effort: errors are returned to the caller for logging but do not
// roll back the local write.
func (r *Replicator) Replicate(key, value string, ttl time.Duration, isDelete bool) error {
	// Only the primary for this key should replicate.
	primary := r.ring.GetReplica(key, 1)
	if primary != r.node.ID {
		return nil
	}

	replicaID := r.ring.GetReplica(key, 2)
	if replicaID == "" || replicaID == r.node.ID {
		return nil // single-node cluster or no distinct replica
	}

	replicaAddr := r.node.PeerGRPCAddr(replicaID)
	if replicaAddr == "" {
		return fmt.Errorf("no gRPC address for replica node %s", replicaID)
	}

	conn, err := grpc.NewClient(replicaAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial replica %s: %w", replicaAddr, err)
	}
	defer conn.Close()

	client := pb.NewClusterServiceClient(conn)
	_, err = client.Replicate(context.Background(), &pb.ReplicateRequest{
		Key:        key,
		Value:      value,
		TtlSeconds: int64(ttl.Seconds()),
		IsDelete:   isDelete,
	})
	if err != nil {
		return fmt.Errorf("replicate rpc to %s: %w", replicaID, err)
	}

	fmt.Printf("[%s] replicated %q → %s\n", r.node.ID, key, replicaID)
	return nil
}
