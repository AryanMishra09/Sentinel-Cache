package cache

import "time"

// runTTLCleanup runs forever as a goroutine, deleting expired keys every second.
//
// This is the "active expiry" pass. It works alongside the "lazy expiry" in
// Get() — keys that are never read again will still be cleaned up here,
// preventing unbounded memory growth from write-only keys.
func (e *Engine) runTTLCleanup() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		e.deleteExpired()
	}
}

// deleteExpired scans the store and removes all expired entries.
// Deleting from a map during range iteration is safe in Go — the spec
// guarantees that keys deleted during iteration will not be visited again.
func (e *Engine) deleteExpired() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, el := range e.store {
		if el.Value.(*entry).isExpired() {
			e.removeElement(el)
		}
	}
}
