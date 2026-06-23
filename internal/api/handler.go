package api

import (
	"bytes"
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

// forwardTo proxies the current request to another node's REST address and
// writes its response back to the client verbatim. Returns false if forwarding
// fails (caller should return after checking).
func (h *Handler) forwardTo(c *gin.Context, targetAddr string) bool {
	url := "http://" + targetAddr + c.Request.URL.Path

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not read request body"})
		return false
	}

	req, err := http.NewRequest(c.Request.Method, url, bytes.NewReader(body))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create forward request"})
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{
			"error":       "failed to forward to primary",
			"forwarded_to": targetAddr,
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
	var req setRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// If this node is not the primary for this key, forward to the primary.
	// This makes writes correct regardless of which node the client hits.
	primary := h.ring.GetReplica(req.Key, 1)
	if primary != h.node.ID {
		primaryAddr := h.node.PeerRESTAddr(primary)
		if primaryAddr == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":   "primary node unavailable",
				"primary": primary,
			})
			return
		}
		fmt.Printf("[%s] forwarding SET %q → %s (%s)\n", h.node.ID, req.Key, primary, primaryAddr)
		h.forwardTo(c, primaryAddr)
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
	val, ok := h.engine.Get(key)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{
			"error":     "key not found",
			"served_by": h.node.ID,
		})
		return
	}
	// served_by shows which node answered — after failover this changes from
	// the dead primary to the promoted replica.
	c.JSON(http.StatusOK, gin.H{
		"key":       key,
		"value":     val,
		"served_by": h.node.ID,
	})
}

func (h *Handler) handleDelete(c *gin.Context) {
	key := c.Param("key")

	// Forward to primary if this node doesn't own the key.
	primary := h.ring.GetReplica(key, 1)
	if primary != h.node.ID {
		primaryAddr := h.node.PeerRESTAddr(primary)
		if primaryAddr == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":   "primary node unavailable",
				"primary": primary,
			})
			return
		}
		fmt.Printf("[%s] forwarding DELETE %q → %s (%s)\n", h.node.ID, key, primary, primaryAddr)
		h.forwardTo(c, primaryAddr)
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
