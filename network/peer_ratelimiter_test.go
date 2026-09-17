package network

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// rewindBucket moves a peer's bucket back in time, so that refill can be
// exercised without the test sleeping.
func rewindBucket(t *testing.T, l *requestRateLimiter, p peer.ID, d time.Duration) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.peers[p]
	require.True(t, ok, "peer does not have a bucket")
	b.last = b.last.Add(-d)
}

// seedPeers fills the limiter with n synthetic buckets, so that a test can
// reach the map size at which the sweep stops being skipped.
func seedPeers(t *testing.T, l *requestRateLimiter, n int) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := 0; i < n; i++ {
		l.peers[peer.ID(fmt.Sprintf("filler-%d", i))] = &requestBucket{tokens: l.burst, last: time.Now()}
	}
}

// tracked reports whether the limiter still holds a bucket for p.
func tracked(t *testing.T, l *requestRateLimiter, p peer.ID) bool {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.peers[p]
	return ok
}

func TestRequestRateLimiter(t *testing.T) {
	p1 := peer.ID("peer-1")
	p2 := peer.ID("peer-2")

	t.Run("admits up to the burst and then refuses", func(t *testing.T) {
		l := newRequestRateLimiter(60)
		require.True(t, l.allow(p1, 60), "the full burst should be admitted")
		require.False(t, l.allow(p1, 1), "the bucket should now be empty")
	})

	t.Run("batching costs the same as sending separately", func(t *testing.T) {
		batched := newRequestRateLimiter(100)
		require.True(t, batched.allow(p1, 100), "one message of a hundred requests fits the allowance")
		require.False(t, batched.allow(p1, 1), "and leaves nothing behind")

		separate := newRequestRateLimiter(100)
		for i := 0; i < 100; i++ {
			require.True(t, separate.allow(p1, 1), "the same hundred requests one at a time fit")
		}
		require.False(t, separate.allow(p1, 1), "and leave nothing behind")
	})

	t.Run("a message larger than the burst is refused whole", func(t *testing.T) {
		l := newRequestRateLimiter(10)
		require.False(t, l.allow(p1, 11), "a message larger than the burst cannot be admitted")
		// a refusal must not partially consume the bucket
		require.True(t, l.allow(p1, 10), "the bucket should still be full")
	})

	t.Run("a message carrying no requests still costs one token", func(t *testing.T) {
		l := newRequestRateLimiter(3)
		for i := 0; i < 3; i++ {
			require.True(t, l.allow(p1, 0), "each near-empty message should cost one token")
		}
		require.False(t, l.allow(p1, 0), "messages that carry no requests must not be free")
	})

	t.Run("a negative count cannot credit the bucket", func(t *testing.T) {
		l := newRequestRateLimiter(2)
		require.True(t, l.allow(p1, 2), "empty the bucket")
		require.False(t, l.allow(p1, -100), "a nonsensical negative count must not be admitted")
		require.False(t, l.allow(p1, 1), "and must not have credited the bucket")
	})

	t.Run("refills over time", func(t *testing.T) {
		l := newRequestRateLimiter(100) // one hundred requests per second
		require.True(t, l.allow(p1, 100))
		require.False(t, l.allow(p1, 1))

		// half a second should return half the bucket, and no more
		rewindBucket(t, l, p1, 500*time.Millisecond)
		require.True(t, l.allow(p1, 50), "half a second should refill fifty tokens")
		require.False(t, l.allow(p1, 1), "and no more than fifty")
	})

	t.Run("refill is capped at the burst", func(t *testing.T) {
		l := newRequestRateLimiter(60)
		require.True(t, l.allow(p1, 60))

		rewindBucket(t, l, p1, time.Hour)
		require.True(t, l.allow(p1, 60), "refill should stop at the burst size")
		require.False(t, l.allow(p1, 1))
	})

	t.Run("each peer has its own bucket", func(t *testing.T) {
		l := newRequestRateLimiter(10)
		require.True(t, l.allow(p1, 10))
		require.False(t, l.allow(p1, 1), "the first peer should be exhausted")
		require.True(t, l.allow(p2, 10), "one peer's flood must not consume another's allowance")
	})

	t.Run("a non-positive rate disables the limiter", func(t *testing.T) {
		for _, limit := range []int{0, -1} {
			l := newRequestRateLimiter(limit)
			require.True(t, l.allow(p1, 1_000_000), "a disabled limiter admits everything")
			require.Empty(t, l.peers, "a disabled limiter should not track peers")
		}
	})

	t.Run("a refused message still advances the sweep clock", func(t *testing.T) {
		l := newRequestRateLimiter(1)
		require.True(t, l.allow(p1, 1))
		seedPeers(t, l, rateLimiterSweepMinPeers)

		// force the next call to sweep, then make that call a refusal
		l.mu.Lock()
		l.nextSweep = time.Time{}
		l.mu.Unlock()
		require.False(t, l.allow(p1, 1), "the second message should be refused")

		l.mu.Lock()
		swept := !l.nextSweep.IsZero()
		l.mu.Unlock()
		require.True(t, swept, "if refusals skipped the sweep, a flood of newly seen peers would grow the bucket map without bound")
	})

	t.Run("silent peers are swept", func(t *testing.T) {
		l := newRequestRateLimiter(10)
		require.True(t, l.allow(p1, 1))
		rewindBucket(t, l, p1, 2*rateLimiterIdleTimeout)
		seedPeers(t, l, rateLimiterSweepMinPeers)

		// another peer's traffic drives the sweep
		l.mu.Lock()
		l.nextSweep = time.Time{}
		l.mu.Unlock()
		require.True(t, l.allow(p2, 1))

		require.False(t, tracked(t, l, p1), "an idle peer's bucket should be discarded")
	})

	t.Run("the sweep runs at most once per interval", func(t *testing.T) {
		l := newRequestRateLimiter(10)
		require.True(t, l.allow(p1, 1))
		seedPeers(t, l, rateLimiterSweepMinPeers)

		// force the next call to sweep
		l.mu.Lock()
		l.nextSweep = time.Time{}
		l.mu.Unlock()
		require.True(t, l.allow(p2, 1))

		l.mu.Lock()
		afterSweep := l.nextSweep
		l.mu.Unlock()
		require.False(t, afterSweep.IsZero(), "the first call should have swept the table")

		// A peer that falls silent only after that sweep must survive the calls
		// that follow, because every one of them falls inside the interval. If
		// the interval were not enforced, each call would walk the whole table
		// under the lock that all inbound messages contend for.
		p3 := peer.ID("peer-3")
		require.True(t, l.allow(p3, 1))
		rewindBucket(t, l, p3, 2*rateLimiterIdleTimeout)
		for i := 0; i < 10; i++ {
			l.allow(p2, 1)
		}
		require.True(t, tracked(t, l, p3), "a sweep must not run again within the interval")

		l.mu.Lock()
		require.Equal(t, afterSweep, l.nextSweep, "and must not advance the sweep clock")
		l.mu.Unlock()
	})

	t.Run("the sweep is skipped while the map is small", func(t *testing.T) {
		l := newRequestRateLimiter(10)
		require.True(t, l.allow(p1, 1))
		rewindBucket(t, l, p1, 2*rateLimiterIdleTimeout)

		l.mu.Lock()
		l.nextSweep = time.Time{}
		l.mu.Unlock()
		require.True(t, l.allow(p2, 1))

		require.True(t, tracked(t, l, p1), "a map below the threshold must not pay for a sweep")
	})

	t.Run("a reaped bucket is recreated full, so the charge lands in the map", func(t *testing.T) {
		l := newRequestRateLimiter(10)
		require.True(t, l.allow(p1, 10), "empty p1's bucket")
		rewindBucket(t, l, p1, 2*rateLimiterIdleTimeout)
		seedPeers(t, l, rateLimiterSweepMinPeers)

		l.mu.Lock()
		l.nextSweep = time.Time{}
		l.mu.Unlock()
		require.True(t, l.allow(p2, 1), "p2's traffic drives the sweep")
		require.False(t, tracked(t, l, p1), "p1 should have been reaped")

		require.True(t, l.allow(p1, 10), "a reaped peer starts over from a full bucket")
		l.mu.Lock()
		b, ok := l.peers[p1]
		require.True(t, ok, "the bucket must be back in the map")
		require.Less(t, b.tokens, float64(10), "and must be the bucket the charge was deducted from")
		l.mu.Unlock()
	})

	t.Run("drop warnings are throttled per peer", func(t *testing.T) {
		l := newRequestRateLimiter(1)
		require.True(t, l.allow(p1, 1))
		require.False(t, l.allow(p1, 1), "p1 is now over rate")
		require.True(t, l.allow(p2, 1), "give p2 a bucket")

		require.True(t, l.warnDrop(p1), "the first drop should be reported")
		require.False(t, l.warnDrop(p1), "a flood must not become a log flood")
		require.True(t, l.warnDrop(p2), "another peer's drop is reported independently")
	})

	t.Run("an untracked peer is never reported", func(t *testing.T) {
		l := newRequestRateLimiter(1)
		require.False(t, l.warnDrop(p1), "there is nothing to report for a peer the limiter has not seen")

		disabled := newRequestRateLimiter(0)
		require.False(t, disabled.warnDrop(p1), "a disabled limiter reports nothing")
	})

	t.Run("is safe for concurrent use", func(t *testing.T) {
		// demand exactly equals the capacity, so every call should be admitted
		// and the count is deterministic regardless of interleaving
		const workers, perWorker = 10, 100
		l := newRequestRateLimiter(workers * perWorker)

		var admitted int64
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < perWorker; j++ {
					if l.allow(p1, 1) {
						atomic.AddInt64(&admitted, 1)
					}
				}
			}()
		}
		wg.Wait()

		require.Equal(t, int64(workers*perWorker), atomic.LoadInt64(&admitted),
			"concurrent callers should consume the bucket without losing or duplicating allowance")
	})
}
