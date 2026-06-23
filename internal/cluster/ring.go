package cluster

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const defaultVirtualNodes = 150

// Ring is a consistent hash ring.
//
// Each physical node is represented by `virtualNodes` positions on the ring.
// A key's owner is the first node clockwise from hash(key).
// When a node joins or leaves, only ~1/N keys need to move.
type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	positions    []uint32          // sorted ring positions
	owners       map[uint32]string // position → node ID
}

func NewRing(virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = defaultVirtualNodes
	}
	return &Ring{
		virtualNodes: virtualNodes,
		owners:       make(map[uint32]string),
	}
}

// Add places a node onto the ring at `virtualNodes` positions.
func (r *Ring) Add(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		pos := ringHash(fmt.Sprintf("%s#%d", nodeID, i))
		r.positions = append(r.positions, pos)
		r.owners[pos] = nodeID
	}
	sort.Slice(r.positions, func(i, j int) bool {
		return r.positions[i] < r.positions[j]
	})
}

// Remove takes a node off the ring.
func (r *Ring) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		pos := ringHash(fmt.Sprintf("%s#%d", nodeID, i))
		delete(r.owners, pos)

		// Remove pos from the sorted slice.
		idx := sort.Search(len(r.positions), func(j int) bool {
			return r.positions[j] >= pos
		})
		if idx < len(r.positions) && r.positions[idx] == pos {
			r.positions = append(r.positions[:idx], r.positions[idx+1:]...)
		}
	}
}

// GetNode returns the node ID that owns the given key.
// Returns "" if the ring is empty.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.positions) == 0 {
		return ""
	}

	h := ringHash(key)

	// Find the first ring position >= h (clockwise walk).
	idx := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= h
	})

	// Wrap around: if h is past the last position, the owner is the first node.
	if idx == len(r.positions) {
		idx = 0
	}

	return r.owners[r.positions[idx]]
}

// GetReplica returns the node ID of the Nth successor of key on the ring.
// Used to find replica nodes: GetReplica(key, 1) = primary, GetReplica(key, 2) = first replica.
// Returns "" if the ring has fewer than n distinct nodes.
func (r *Ring) GetReplica(key string, n int) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.positions) == 0 {
		return ""
	}

	h := ringHash(key)
	idx := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= h
	})

	seen := make(map[string]bool)
	for count := 0; count < len(r.positions); count++ {
		pos := r.positions[(idx+count)%len(r.positions)]
		nodeID := r.owners[pos]
		if !seen[nodeID] {
			seen[nodeID] = true
			if len(seen) == n {
				return nodeID
			}
		}
	}
	return ""
}

// Size returns the number of physical nodes on the ring.
func (r *Ring) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]bool)
	for _, id := range r.owners {
		seen[id] = true
	}
	return len(seen)
}

// ringHash hashes a string to a uint32 ring position using the first 4 bytes of MD5.
// MD5 is not used for security here — just for even distribution across the ring.
func ringHash(key string) uint32 {
	h := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}
