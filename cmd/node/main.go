package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/aryan-mishra/sentinel-cache/internal/api"
	"github.com/aryan-mishra/sentinel-cache/internal/cache"
	"github.com/aryan-mishra/sentinel-cache/internal/cluster"
	"github.com/aryan-mishra/sentinel-cache/internal/election"
	"github.com/aryan-mishra/sentinel-cache/internal/failover"
	nodeGRPC "github.com/aryan-mishra/sentinel-cache/internal/grpc"
	"github.com/aryan-mishra/sentinel-cache/internal/heartbeat"
	"github.com/aryan-mishra/sentinel-cache/internal/replication"
)

func main() {
	nodeID   := env("NODE_ID",   "node-a")
	restAddr := env("REST_ADDR", ":8080")  // bind address — what this process listens on
	grpcAddr := env("GRPC_ADDR", ":9090")  // bind address
	seedAddr := env("SEED_ADDR", "")

	// Advertise addresses — what OTHER nodes use to reach this one.
	// In Docker these must be "service-name:port" (e.g. "node-b:9090").
	// Locally they default to the bind address (fine for single-machine testing).
	advREST := env("ADVERTISE_REST_ADDR", restAddr)
	advGRPC := env("ADVERTISE_GRPC_ADDR", grpcAddr)

	fmt.Printf("[%s] starting  bind-rest=%s  bind-grpc=%s  adv-rest=%s  adv-grpc=%s  seed=%q\n",
		nodeID, restAddr, grpcAddr, advREST, advGRPC, seedAddr)

	engine := cache.NewEngine(100 * 1024 * 1024)
	// Node stores the ADVERTISE addresses — these are what peers dial.
	node := cluster.NewNode(nodeID, advREST, advGRPC)
	ring := cluster.NewRing(0)
	ring.Add(nodeID)

	if seedAddr == "" {
		node.SetLeader(nodeID)
		fmt.Printf("[%s] seed node — initialised as leader\n", nodeID)
	}

	grpcServer := nodeGRPC.NewServer(node, ring, engine)
	go func() {
		if err := grpcServer.Listen(grpcAddr); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] gRPC server error: %v\n", nodeID, err)
			os.Exit(1)
		}
	}()

	if seedAddr != "" {
		if err := cluster.JoinCluster(node, ring, seedAddr); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] failed to join cluster: %v\n", nodeID, err)
			os.Exit(1)
		}
	}

	ctx := context.Background()

	// cancelSender lets us stop the heartbeat sender when this node wins an election.
	senderCtx, cancelSender := context.WithCancel(ctx)

	// startLeaderDuties is called either at startup (if seed) or after winning election.
	// It starts the failure detector and stops the heartbeat sender.
	startLeaderDuties := func(deadLeaderID string) {
		cancelSender() // stop sending heartbeats (we are now the leader)

		if deadLeaderID != "" {
			ring.Remove(deadLeaderID)
			node.MarkPeerStatus(deadLeaderID, cluster.StatusDead)
			fmt.Printf("[%s] removed dead leader %s from ring\n", nodeID, deadLeaderID)
		}
		node.SetLeader(nodeID)

		detector := heartbeat.NewDetector(5*time.Second, node, func(deadID string) {
			node.MarkPeerStatus(deadID, cluster.StatusDead)
			fmt.Printf("[%s] *** FAILOVER: %s is dead ***\n", nodeID, deadID)
			failover.BroadcastNodeDeath(deadID, node, ring)
		})
		grpcServer.SetDetector(detector)
		go detector.Start(ctx)
		fmt.Printf("[%s] *** LEADER — failure detector started ***\n", nodeID)
	}

	// AnnounceLeader handler: called when a peer wins an election and tells us.
	grpcServer.SetAnnounceHandler(func(newLeaderID, deadLeaderID string) {
		if deadLeaderID != "" {
			ring.Remove(deadLeaderID)
			node.MarkPeerStatus(deadLeaderID, cluster.StatusDead)
		}
		node.SetLeader(newLeaderID)
		// heartbeat sender's loop reads LeaderGRPCAddr() on every reconnect,
		// so it will automatically dial the new leader after the current stream drops.
		fmt.Printf("[%s] updated leader → %s\n", nodeID, newLeaderID)
	})

	if node.IsLeader() {
		startLeaderDuties("")
	} else {
		// Create the bully before the sender — sender's callback references it.
		bully := election.NewBully(node, ring, func(deadLeaderID string) {
			startLeaderDuties(deadLeaderID)
		})
		grpcServer.SetBully(bully)

		sender := heartbeat.NewSender(node, engine, time.Second, func() {
			go bully.Start(node.LeaderID())
		})
		go sender.Start(senderCtx)
		fmt.Printf("[%s] heartbeat sender started → leader=%s\n", nodeID, node.LeaderID())
	}

	replicator := replication.New(node, ring)
	handler    := api.NewHandler(engine, node, ring, replicator)

	r := gin.Default()
	handler.RegisterRoutes(r)

	fmt.Printf("[%s] REST API ready on %s\n", nodeID, restAddr)
	if err := r.Run(restAddr); err != nil {
		fmt.Fprintf(os.Stderr, "[%s] REST server error: %v\n", nodeID, err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
