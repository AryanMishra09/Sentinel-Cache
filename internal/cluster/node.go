package cluster

import "sync"

type Status string

const (
	StatusHealthy    Status = "healthy"
	StatusDead       Status = "dead"
	StatusRecovering Status = "recovering"
)

type PeerInfo struct {
	ID       string
	RESTAddr string
	GRPCAddr string
	Status   Status
}

// Node represents this process's identity and its view of the cluster.
type Node struct {
	ID       string
	RESTAddr string
	GRPCAddr string

	mu       sync.RWMutex
	peers    map[string]*PeerInfo
	leaderID string
}

func NewNode(id, restAddr, grpcAddr string) *Node {
	return &Node{
		ID:       id,
		RESTAddr: restAddr,
		GRPCAddr: grpcAddr,
		peers:    make(map[string]*PeerInfo),
	}
}

func (n *Node) AddPeer(p *PeerInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[p.ID] = p
}

func (n *Node) RemovePeer(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.peers, id)
}

func (n *Node) Peers() []*PeerInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]*PeerInfo, 0, len(n.peers))
	for _, p := range n.peers {
		out = append(out, p)
	}
	return out
}

func (n *Node) SetLeader(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.leaderID = id
}

func (n *Node) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

func (n *Node) IsLeader() bool {
	return n.LeaderID() == n.ID
}

// PeerGRPCAddr returns the gRPC address of a specific peer by ID.
// Returns "" if the peer is not in the peer list.
func (n *Node) PeerGRPCAddr(id string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if p, ok := n.peers[id]; ok {
		return p.GRPCAddr
	}
	return ""
}

// PeerRESTAddr returns the REST address of a specific peer by ID.
// Returns "" if the peer is not in the peer list.
func (n *Node) PeerRESTAddr(id string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if p, ok := n.peers[id]; ok {
		return p.RESTAddr
	}
	return ""
}

// LeaderGRPCAddr returns the gRPC address of the current leader.
// Returns "" if this node is the leader (no need to dial yourself).
func (n *Node) LeaderGRPCAddr() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.leaderID == n.ID {
		return ""
	}
	if p, ok := n.peers[n.leaderID]; ok {
		return p.GRPCAddr
	}
	return ""
}

// MarkPeerStatus updates the status of a known peer.
func (n *Node) MarkPeerStatus(id string, s Status) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if p, ok := n.peers[id]; ok {
		p.Status = s
	}
}
