package api

import (
	"sync"
	"time"
)

type bucket struct {
	tokens   int
	refillAt time.Time
	lastSeen time.Time
}

// ipLimiter is a discrete-refill token bucket per client IP. Each request costs
// one token; a full capacity is added once per elapsed second, capped at capacity.
// Cleanup is lazy (at most one sweep/second), so no lifecycle goroutine is needed.
type ipLimiter struct {
	mu          sync.Mutex
	buckets     map[string]bucket
	capacity    int
	maxIPs      int
	idleTimeout time.Duration
	nextCleanup time.Time
	now         func() time.Time
}

func (l *ipLimiter) allow(ip string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Read time while holding the state lock. A delayed request's earlier
	// receipt timestamp must not recreate an already-expired refill window.
	now := l.now()
	if l.nextCleanup.IsZero() || !now.Before(l.nextCleanup) {
		for ip, bucket := range l.buckets {
			if now.Sub(bucket.lastSeen) >= l.idleTimeout {
				delete(l.buckets, ip)
			}
		}
		l.nextCleanup = now.Add(time.Second)
	}
	entry, exists := l.buckets[ip]
	if !exists {
		// Never evict a live budget to admit a new IP: that would let a client
		// regain tokens by displacing its own entry from a full map.
		if len(l.buckets) >= l.maxIPs {
			return false, time.Second
		}
		entry = bucket{tokens: l.capacity, refillAt: now.Add(time.Second), lastSeen: now}
	}
	// A supplied clock may step backwards; keep this IP's state monotonic.
	if now.Before(entry.lastSeen) {
		now = entry.lastSeen
	}
	entry.lastSeen = now
	if !now.Before(entry.refillAt) {
		elapsed := now.Sub(entry.refillAt)
		entry.refillAt = now.Add(time.Second - elapsed%time.Second)
		entry.tokens = l.capacity
	}
	if entry.tokens == 0 {
		l.buckets[ip] = entry
		return false, entry.refillAt.Sub(now)
	}
	entry.tokens--
	l.buckets[ip] = entry
	return true, 0
}
