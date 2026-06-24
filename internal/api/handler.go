package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/aryan-mishra/sentinel-cache/internal/cache"
	"github.com/aryan-mishra/sentinel-cache/internal/cluster"
	"github.com/aryan-mishra/sentinel-cache/internal/replication"
)

type Handler struct {
	engine     *cache.Engine
	node       *cluster.Node
	ring       *cluster.Ring
	replicator *replication.Replicator
}

func NewHandler(
	engine *cache.Engine,
	node *cluster.Node,
	ring *cluster.Ring,
	replicator *replication.Replicator,
) *Handler {
	return &Handler{engine: engine, node: node, ring: ring, replicator: replicator}
}

// RegisterRoutes wires all REST endpoints onto the given Gin router.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.POST("/set", h.handleSet)
	r.GET("/get/:key", h.handleGet)
	r.DELETE("/delete/:key", h.handleDelete)
	r.GET("/cluster/status", h.handleStatus)
}

// ── Request types ────────────────────────────────────────────────────────────

type setRequest struct {
	Key   string `json:"key"   binding:"required"`
	Value string `json:"value" binding:"required"`
	TTL   int64  `json:"ttl"` // seconds; 0 = no expiry
}

// ── Forwarding ───────────────────────────────────────────────────────────────

const forwardedHeader = "X-SentinelCache-Forwarded"

// forwardTo proxies the request to another node's REST address using the given
// body bytes and writes the target's response back to the client verbatim.
//
// The body MUST be passed in explicitly: by the time we decide to forward, the
// handler has already consumed c.Request.Body (via ShouldBindJSON), so it can no
// longer be re-read. Pass nil for requests with no body (e.g. DELETE).
func (h *Handler) forwardTo(c *gin.Context, targetAddr string, body []byte) bool {
	url := "http://" + targetAddr + c.Request.URL.Path

	req, err := http.NewRequest(c.Request.Method, url, bytes.NewReader(body))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create forward request"})
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	// Mark the request so the receiving node knows not to forward it again.
	// Prevents infinite forwarding loops when rings are temporarily inconsistent.
	req.Header.Set(forwardedHeader, "1")

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{
			"error":        "failed to forward to primary",
			"forwarded_to": targetAddr,
			"detail":       err.Error(),
		})
		return false
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	c.Data(resp.StatusCode, "application/json", respBody)
	return true
}

// ── Handlers ─────────────────────────────────────────────────────────────────

func (h *Handler) handleSet(c *gin.Context) {
	// Read the raw body ONCE. We need the parsed key to decide routing, but we
	// also need the original bytes to forward — and ShouldBindJSON would drain
	// the body, leaving nothing to forward. So we read bytes and parse from them.
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "could not read request body"})
		return
	}

	var req setRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Key == "" || req.Value == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key and value are required"})
		return
	}

	// If this node is not the primary for this key, forward to the primary —
	// but only if this request hasn't already been forwarded. The header guard
	// prevents infinite loops when two nodes have temporarily inconsistent rings.
	primary := h.ring.GetReplica(req.Key, 1)
	if primary != h.node.ID && c.GetHeader(forwardedHeader) == "" {
		primaryAddr := h.node.PeerRESTAddr(primary)
		if primaryAddr == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":   "primary node unavailable",
				"primary": primary,
			})
			return
		}
		fmt.Printf("[%s] forwarding SET %q → %s (%s)\n", h.node.ID, req.Key, primary, primaryAddr)
		h.forwardTo(c, primaryAddr, bodyBytes)
		return
	}

	// This node is the primary. Write locally first.
	ttl := time.Duration(req.TTL) * time.Second
	h.engine.Set(req.Key, req.Value, ttl)

	// Synchronous replication: the write is only ACKed after the replica confirms.
	// If replication fails, roll back the local write and return an error.
	replicaID := h.ring.GetReplica(req.Key, 2)
	if replicaID != "" && replicaID != h.node.ID {
		if err := h.replicator.Replicate(req.Key, req.Value, ttl, false); err != nil {
			h.engine.Delete(req.Key) // roll back
			c.JSON(http.StatusBadGateway, gin.H{
				"error":   "replication failed — write rolled back",
				"replica": replicaID,
				"detail":  err.Error(),
			})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":         true,
		"written_by": h.node.ID,
		"primary":    primary,
		"replica":    replicaID,
	})
}

func (h *Handler) handleGet(c *gin.Context) {
	key := c.Param("key")

	// Serve locally if we have it. This node may be the primary OR the replica
	// for the key — either way the data is here.
	if val, ok := h.engine.Get(key); ok {
		// served_by shows which node answered — after failover this changes from
		// the dead primary to the promoted replica.
		c.JSON(http.StatusOK, gin.H{
			"key":       key,
			"value":     val,
			"served_by": h.node.ID,
		})
		return
	}

	// Not here. If this node isn't the primary and the request hasn't already
	// been forwarded, fetch from the primary so reads work against ANY node.
	primary := h.ring.GetReplica(key, 1)
	if primary != h.node.ID && c.GetHeader(forwardedHeader) == "" {
		primaryAddr := h.node.PeerRESTAddr(primary)
		if primaryAddr != "" {
			fmt.Printf("[%s] forwarding GET %q → %s (%s)\n", h.node.ID, key, primary, primaryAddr)
			h.forwardTo(c, primaryAddr, nil)
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{
		"error":     "key not found",
		"served_by": h.node.ID,
	})
}

func (h *Handler) handleDelete(c *gin.Context) {
	key := c.Param("key")

	// Forward to primary if this node doesn't own the key (first hop only).
	primary := h.ring.GetReplica(key, 1)
	if primary != h.node.ID && c.GetHeader(forwardedHeader) == "" {
		primaryAddr := h.node.PeerRESTAddr(primary)
		if primaryAddr == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":   "primary node unavailable",
				"primary": primary,
			})
			return
		}
		fmt.Printf("[%s] forwarding DELETE %q → %s (%s)\n", h.node.ID, key, primary, primaryAddr)
		h.forwardTo(c, primaryAddr, nil)
		return
	}

	h.engine.Delete(key)

	if err := h.replicator.Replicate(key, "", 0, true); err != nil {
		fmt.Printf("[%s] replication warning for delete %q: %v\n", h.node.ID, key, err)
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) handleStatus(c *gin.Context) {
	peers := h.node.Peers()
	peerList := make([]gin.H, 0, len(peers))
	for _, p := range peers {
		peerList = append(peerList, gin.H{
			"id":        p.ID,
			"rest_addr": p.RESTAddr,
			"grpc_addr": p.GRPCAddr,
			"status":    string(p.Status),
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"node_id":   h.node.ID,
		"leader_id": h.node.LeaderID(),
		"key_count": h.engine.KeyCount(),
		"peers":     peerList,
	})
}
