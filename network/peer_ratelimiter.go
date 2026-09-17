package network

import (
	"math"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	rateLimiterSweepInterval = time.Minute
	// rateLimiterIdleTimeout must exceed the 1s a bucket needs to refill from
	// empty, or a reaped bucket would hand out extra tokens.
	rateLimiterIdleTimeout = time.Minute
	// rateLimiterSweepMinPeers: below it the map is bounded by it already, so
	// sweeping costs more than it reclaims.
	rateLimiterSweepMinPeers    = 1024
	rateLimiterDropWarnInterval = time.Minute
)

// requestRateLimiter caps the request content each peer may send per second.
// Messages are charged for the requests they carry, so batching buys nothing.
// Over W seconds a peer gets at most perSecond*(1+W) requests — a rate, not a
// per-second quota. A non-positive rate disables the limiter.
type requestRateLimiter struct {
	perSecond float64
	// burst is the bucket capacity, and equals perSecond.
	burst float64

	mu        sync.Mutex // a peer may have several concurrent streams
	peers     map[peer.ID]*requestBucket
	nextSweep time.Time
}

type requestBucket struct {
	tokens       float64
	last         time.Time
	lastDropWarn time.Time
}

func newRequestRateLimiter(maxRequestsPerSecond int) *requestRateLimiter {
	if maxRequestsPerSecond <= 0 {
		return &requestRateLimiter{}
	}
	burst := float64(maxRequestsPerSecond)
	return &requestRateLimiter{
		perSecond: burst,
		burst:     burst,
		peers:     make(map[peer.ID]*requestBucket),
	}
}

// allow admits a message whole or not at all.
func (l *requestRateLimiter) allow(p peer.ID, n int) bool {
	if l.perSecond <= 0 {
		return true
	}
	// A call costs at least one token, so a negative count cannot credit.
	if n < 1 {
		n = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// Sampled under the lock, so timestamps written to b.last stay ordered.
	now := time.Now()

	// First, so the charge below cannot land on a bucket that has left the map.
	l.sweep(now)

	b, ok := l.peers[p]
	if !ok {
		b = &requestBucket{tokens: l.burst, last: now}
		l.peers[p] = b
	} else {
		b.tokens = math.Min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.perSecond)
		b.last = now
	}

	if b.tokens < float64(n) {
		return false
	}
	b.tokens -= float64(n)
	return true
}

// warnDrop throttles reports to one per rateLimiterDropWarnInterval per peer,
// so the refusal path cannot become a log flood. Locked separately from allow,
// keeping the log write outside that lock.
func (l *requestRateLimiter) warnDrop(p peer.ID) bool {
	if l.perSecond <= 0 {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	b, ok := l.peers[p]
	if !ok {
		return false
	}
	if !b.lastDropWarn.IsZero() && now.Sub(b.lastDropWarn) < rateLimiterDropWarnInterval {
		return false
	}
	b.lastDropWarn = now
	return true
}

// sweep discards buckets idle past rateLimiterIdleTimeout; called from allow,
// so there is no lifecycle to manage.
func (l *requestRateLimiter) sweep(now time.Time) {
	if len(l.peers) < rateLimiterSweepMinPeers {
		return
	}
	if now.Before(l.nextSweep) {
		return
	}
	l.nextSweep = now.Add(rateLimiterSweepInterval)
	cutoff := now.Add(-rateLimiterIdleTimeout)
	for p, b := range l.peers {
		if b.last.Before(cutoff) {
			delete(l.peers, p)
		}
	}
}
