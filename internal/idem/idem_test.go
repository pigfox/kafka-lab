package idem

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// manualClock moves only when a test moves it. NOTHING IN THIS FILE SLEEPS: a
// test that spent a real TTL would take as long as the window it is checking,
// and the windows worth checking are minutes.
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *manualClock {
	return &manualClock{t: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestFirstSightingReportsZero(t *testing.T) {
	s := New(8, time.Minute, newClock())
	if got := s.Observe("a"); got != 0 {
		t.Fatalf("first sighting reported %d prior sightings, want 0", got)
	}
	if s.Len() != 1 {
		t.Fatalf("len %d want 1", s.Len())
	}
}

// The returned number is the count BEFORE this call, so a caller can tell a
// second delivery from a fifth without keeping its own tally.
func TestRepeatSightingsReportThePriorCount(t *testing.T) {
	s := New(8, time.Minute, newClock())
	for want := 0; want < 4; want++ {
		if got := s.Observe("a"); got != want {
			t.Fatalf("sighting %d reported %d prior, want %d", want+1, got, want)
		}
	}
	if s.Len() != 1 {
		t.Fatalf("four sightings of one key left len %d, want 1", s.Len())
	}
}

// EXACTLY ONE WINS. This is the whole reason Observe is a single method: a
// Contains/Add pair would let every goroutine here read absent before any of
// them recorded, and every one of them would apply.
func TestConcurrentSightingsOfOneKeyElectExactlyOneFirst(t *testing.T) {
	s := New(64, time.Minute, newClock())

	const goroutines = 64
	var firsts atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if s.Observe("same") == 0 {
				firsts.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := firsts.Load(); got != 1 {
		t.Fatalf("%d goroutines saw the key as first-seen, want exactly 1", got)
	}
}

func TestConcurrentSightingsOfDistinctKeysAreAllFirst(t *testing.T) {
	s := New(256, time.Minute, newClock())

	var firsts atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if s.Observe("k"+strconv.Itoa(n)) == 0 {
				firsts.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if got := firsts.Load(); got != 128 {
		t.Fatalf("%d of 128 distinct keys were first-seen", got)
	}
}

func TestAKeyExpiresAfterItsTTL(t *testing.T) {
	c := newClock()
	s := New(8, time.Minute, c)

	s.Observe("a")
	c.advance(time.Minute)

	if got := s.Observe("a"); got != 0 {
		t.Fatalf("an expired key reported %d prior sightings; it must read as first-seen", got)
	}
	if s.Expiries() != 1 {
		t.Fatalf("expiries %d want 1", s.Expiries())
	}
}

// The boundary is inclusive: a key survives strictly less than the TTL.
func TestAKeySurvivesUpToButNotIncludingItsDeadline(t *testing.T) {
	c := newClock()
	s := New(8, time.Minute, c)

	s.Observe("a")
	c.advance(time.Minute - time.Nanosecond)
	if got := s.Observe("a"); got != 1 {
		t.Fatalf("a key one nanosecond short of its deadline reported %d prior, want 1", got)
	}
	if s.Expiries() != 0 {
		t.Fatalf("expiries %d want 0; the key was dropped early", s.Expiries())
	}
}

// The sweep stops at the first live entry rather than walking the whole set,
// which is only correct because a repeat sighting does NOT refresh the deadline.
func TestExpirySweepStopsAtTheFirstLiveEntry(t *testing.T) {
	c := newClock()
	s := New(8, time.Minute, c)

	s.Observe("old")
	c.advance(30 * time.Second)
	s.Observe("new")
	c.advance(31 * time.Second) // old is 61s (gone), new is 31s (live)

	if got := s.Observe("new"); got != 1 {
		t.Fatalf("the younger key reported %d prior, want 1 — the sweep overran", got)
	}
	if s.Expiries() != 1 {
		t.Fatalf("expiries %d want 1", s.Expiries())
	}
}

// A repeat sighting must NOT push the deadline out, or a hot key would live
// forever and insertion order would stop matching expiry order.
func TestARepeatSightingDoesNotRefreshTheDeadline(t *testing.T) {
	c := newClock()
	s := New(8, time.Minute, c)

	s.Observe("a")
	c.advance(59 * time.Second)
	s.Observe("a") // seen again, one second before the deadline
	c.advance(2 * time.Second)

	if got := s.Observe("a"); got != 0 {
		t.Fatalf("the key reported %d prior sightings; the deadline was refreshed", got)
	}
}

func TestATTLOfZeroMeansEntriesNeverExpire(t *testing.T) {
	c := newClock()
	s := New(8, 0, c)

	s.Observe("a")
	c.advance(100 * 24 * time.Hour)

	if got := s.Observe("a"); got != 1 {
		t.Fatalf("with no TTL the key reported %d prior, want 1", got)
	}
	if s.Expiries() != 0 {
		t.Fatalf("expiries %d want 0; a zero TTL must not expire anything", s.Expiries())
	}
}

func TestCapacityEvictsTheOldestFirstSighting(t *testing.T) {
	s := New(3, time.Minute, newClock())

	s.Observe("a")
	s.Observe("b")
	s.Observe("c")
	if s.Len() != 3 {
		t.Fatalf("len %d want 3", s.Len())
	}

	s.Observe("d") // pushes "a" out

	if s.Len() != 3 {
		t.Fatalf("len %d want 3; the set grew past its capacity", s.Len())
	}
	if s.Evictions() != 1 {
		t.Fatalf("evictions %d want 1", s.Evictions())
	}
	if got := s.Observe("b"); got != 1 {
		t.Fatalf("b reported %d prior, want 1 — the wrong key was evicted", got)
	}
}

// ── THE GUARANTEE BEING LOST, WRITTEN AS A TEST ────────────────────────────
//
// This is not a bug being pinned, it is the documented cost of a bounded
// in-process store being made visible. A key evicted before its redelivery
// arrives reads as first-seen, and a caller deduplicating on that answer
// applies the effect a second time. The test exists so nobody discovers this
// property by being surprised by it in production.
func TestAnEvictedKeySlipsThroughAsFirstSeen(t *testing.T) {
	s := New(2, time.Minute, newClock())

	s.Observe("victim")
	s.Observe("b")
	s.Observe("c") // capacity is 2, so "victim" is gone

	if got := s.Observe("victim"); got != 0 {
		t.Fatalf("the evicted key reported %d prior sightings, want 0", got)
	}
	if s.Evictions() == 0 {
		t.Fatal("the loss was not counted; an eviction that nothing records is a guarantee lost in silence")
	}
}

// The same slip, on the timer instead of on pressure.
func TestAnExpiredKeySlipsThroughAsFirstSeen(t *testing.T) {
	c := newClock()
	s := New(64, time.Second, c)

	s.Observe("victim")
	c.advance(2 * time.Second)

	if got := s.Observe("victim"); got != 0 {
		t.Fatalf("the expired key reported %d prior sightings, want 0", got)
	}
	if s.Expiries() != 1 {
		t.Fatalf("expiries %d want 1", s.Expiries())
	}
}

// A set that held nothing would answer first-seen to everything, which is a
// store that silently does not exist.
func TestACapacityBelowOneIsRaisedToOne(t *testing.T) {
	for _, capacity := range []int{0, -5} {
		s := New(capacity, time.Minute, newClock())
		if s.Observe("a") != 0 {
			t.Fatalf("capacity %d: first sighting was not first", capacity)
		}
		if got := s.Observe("a"); got != 1 {
			t.Fatalf("capacity %d: the set remembered nothing (%d prior)", capacity, got)
		}
	}
}

func TestANilClockReadsTheWallClock(t *testing.T) {
	s := New(8, time.Minute, nil)
	if s.Observe("a") != 0 {
		t.Fatal("first sighting was not first")
	}
	if got := s.Observe("a"); got != 1 {
		t.Fatalf("got %d prior, want 1", got)
	}
}

func TestRealClockReadsTheWallClock(t *testing.T) {
	before := time.Now()
	got := RealClock{}.Now()
	if got.Before(before) {
		t.Fatalf("RealClock returned %v, before the call at %v", got, before)
	}
}

func TestAnEmptySetSweepsNothing(t *testing.T) {
	s := New(4, time.Minute, newClock())
	if s.Len() != 0 {
		t.Fatalf("a new set holds %d keys", s.Len())
	}
	if s.Evictions() != 0 || s.Expiries() != 0 {
		t.Fatalf("a new set reports %d evictions and %d expiries", s.Evictions(), s.Expiries())
	}
	s.Observe("a")
}
