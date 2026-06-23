package cache

import (
	"testing"
	"time"
)

func TestSetAndGet(t *testing.T) {
	e := NewEngine(0) // 0 = no memory limit for this test

	e.Set("user:1", "aryan", 0)

	val, ok := e.Get("user:1")
	if !ok || val != "aryan" {
		t.Fatalf("expected aryan, got %q ok=%v", val, ok)
	}
}

func TestDelete(t *testing.T) {
	e := NewEngine(0)
	e.Set("user:1", "aryan", 0)
	e.Delete("user:1")

	_, ok := e.Get("user:1")
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestTTLExpiry(t *testing.T) {
	e := NewEngine(0)
	e.Set("session:1", "abc123", 50*time.Millisecond)

	// Key should exist immediately after set.
	_, ok := e.Get("session:1")
	if !ok {
		t.Fatal("expected key to exist before TTL expires")
	}

	// Wait for TTL to expire.
	time.Sleep(100 * time.Millisecond)

	// Lazy expiry: Get should now return a miss.
	_, ok = e.Get("session:1")
	if ok {
		t.Fatal("expected key to be expired")
	}
}

func TestLRUEviction(t *testing.T) {
	// Max 10 bytes. Each entry is len(key)+len(value).
	// "a"+"1" = 2 bytes, "b"+"2" = 2 bytes, ...
	// We'll fill with 5 entries of 2 bytes each (10 bytes), then add a 6th.
	e := NewEngine(10)

	e.Set("a", "1", 0) // 2 bytes
	e.Set("b", "2", 0) // 2 bytes
	e.Set("c", "3", 0) // 2 bytes
	e.Set("d", "4", 0) // 2 bytes
	e.Set("e", "5", 0) // 2 bytes — now at 10 bytes, full

	// Access "a" to make it the most recently used.
	e.Get("a")

	// Adding "f" (2 bytes) must evict the LRU entry — which should be "b"
	// because "a" was just accessed.
	e.Set("f", "6", 0)

	_, bExists := e.Get("b")
	if bExists {
		t.Fatal("expected 'b' to be evicted as LRU")
	}

	_, aExists := e.Get("a")
	if !aExists {
		t.Fatal("expected 'a' to survive (it was recently accessed)")
	}
}

func TestOverwriteUpdatesValue(t *testing.T) {
	e := NewEngine(0)
	e.Set("key", "old", 0)
	e.Set("key", "new", 0)

	val, ok := e.Get("key")
	if !ok || val != "new" {
		t.Fatalf("expected 'new', got %q", val)
	}
	if e.KeyCount() != 1 {
		t.Fatalf("expected 1 key, got %d", e.KeyCount())
	}
}
