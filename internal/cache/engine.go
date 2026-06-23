package cache

import (
	"container/list"
	"sync"
	"time"
)

// entry is what lives inside the LRU list.
type entry struct {
	key       string
	value     string
	expiresAt time.Time // zero value = no expiry
}

func (e *entry) isExpired() bool {
	return !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
}

// sizeBytes is our memory accounting unit: key + value length.
// It's an approximation — struct overhead is not counted — but consistent.
func (e *entry) sizeBytes() int64 {
	return int64(len(e.key) + len(e.value))
}

// Engine is the in-memory cache store.
//
// Internally it is a doubly linked list (LRU order) backed by a hashmap for
// O(1) get and O(1) eviction.  The list front is most-recently-used; the back
// is evicted first when memory pressure hits.
//
// A plain Mutex is used (not RWMutex) because Get must move elements to the
// front of the LRU list, making every read a write internally.
type Engine struct {
	mu       sync.Mutex
	store    map[string]*list.Element // key → list element
	lru      *list.List               // front = MRU, back = LRU
	used     int64                    // current memory in bytes (approximate)
	maxBytes int64                    // 0 = unlimited
}

func NewEngine(maxBytes int64) *Engine {
	e := &Engine{
		store:    make(map[string]*list.Element),
		lru:      list.New(),
		maxBytes: maxBytes,
	}
	go e.runTTLCleanup() // start the background expiry worker
	return e
}

// Set stores key → value with an optional TTL. ttl=0 means no expiry.
func (e *Engine) Set(key, value string, ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Key already exists: update in place and bump to front.
	if el, ok := e.store[key]; ok {
		ent := el.Value.(*entry)
		e.used -= ent.sizeBytes()
		ent.value = value
		ent.expiresAt = makeExpiry(ttl)
		e.used += ent.sizeBytes()
		e.lru.MoveToFront(el)
		return
	}

	// New key: push to front of LRU list.
	ent := &entry{
		key:       key,
		value:     value,
		expiresAt: makeExpiry(ttl),
	}
	el := e.lru.PushFront(ent)
	e.store[key] = el
	e.used += ent.sizeBytes()

	e.evictIfNeeded()
}

// Get returns the value for key and whether it was found.
// A found-but-expired key is treated as a miss and deleted.
func (e *Engine) Get(key string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	el, ok := e.store[key]
	if !ok {
		return "", false
	}

	ent := el.Value.(*entry)

	// Lazy expiry: if the key has expired, clean it up and return a miss.
	if ent.isExpired() {
		e.removeElement(el)
		return "", false
	}

	// Move to front — this key was just accessed, it is now the MRU.
	e.lru.MoveToFront(el)
	return ent.value, true
}

// Delete removes a key from the cache. No-op if the key doesn't exist.
func (e *Engine) Delete(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if el, ok := e.store[key]; ok {
		e.removeElement(el)
	}
}

// KeyCount returns the number of keys currently in the cache.
func (e *Engine) KeyCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.store)
}

// ── Internal helpers ─────────────────────────────────────────────────────────

// removeElement removes a list element from both the LRU list and the store map.
// Caller must hold e.mu.
func (e *Engine) removeElement(el *list.Element) {
	ent := el.Value.(*entry)
	e.lru.Remove(el)
	delete(e.store, ent.key)
	e.used -= ent.sizeBytes()
}

// evictIfNeeded removes LRU entries from the back of the list until memory
// usage is within the configured limit.
// Caller must hold e.mu.
func (e *Engine) evictIfNeeded() {
	if e.maxBytes == 0 {
		return
	}
	for e.used > e.maxBytes {
		el := e.lru.Back()
		if el == nil {
			break
		}
		e.removeElement(el)
	}
}

// makeExpiry converts a TTL duration into an absolute expiry time.
// Returns zero Time if ttl <= 0 (no expiry).
func makeExpiry(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return time.Now().Add(ttl)
}
