// Package idem is a bounded seen-set: it remembers which keys have gone past
// and how many times, under a capacity limit and a time-to-live.
//
// ── WHAT AN EVICTION COSTS, STATED PLAINLY BECAUSE IT IS THE POINT
//
// This store lives in one process's heap. It is bounded, so it forgets, and
// every one of the three ways it forgets destroys the guarantee for the key it
// forgot:
//
//   - CAPACITY. The oldest first-sighting is dropped to make room for a new
//     key. If that key's redelivery arrives afterwards, Observe reports it as
//     first-seen and a caller deduplicating on that answer applies the effect a
//     second time.
//   - TTL. An entry older than the window is dropped on the next Observe. Same
//     consequence, on a timer instead of on pressure.
//   - THE PROCESS ENDING. Nothing here is written down. A restart, a crash, or
//     a consumer-group rebalance that moves a partition to a different member
//     starts from an empty set, and every message still in flight is first-seen
//     again.
//
// That is the honest property of an in-process store, not a defect to hide. The
// window is a bet that a redelivery arrives before the entry ages out, and the
// bet is lost quietly — the caller sees an ordinary first-seen answer, with
// nothing to distinguish it from a genuinely new key. Evictions and Expiries
// count the two bounded cases so the loss is at least VISIBLE in aggregate; the
// third is not countable from inside, because a process that has ended is not
// running the counter.
//
// A store that survives any of the three is a store that writes to something
// outside the process, and that is a different design with a different cost.
package idem

import (
	"container/list"
	"sync"
	"time"
)

// Clock is the time source, injected so a test can move time without spending
// it. A dedupe window measured against a real clock would need a test that
// sleeps for the window, which is a test nobody runs twice.
type Clock interface {
	Now() time.Time
}

// RealClock reads the wall clock.
type RealClock struct{}

// Now returns the current time.
func (RealClock) Now() time.Time { return time.Now() }

// LossReason says which of the two bounds dropped a key.
type LossReason string

const (
	// LossCapacity is the oldest key being dropped to make room for a new one.
	LossCapacity LossReason = "capacity"
	// LossTTL is a key being dropped for age.
	LossTTL LossReason = "ttl"
)

// Loss is one forgotten key.
//
// IT IS A CALLBACK RATHER THAN A COUNTER TO READ LATER, because the two are not
// equivalent for the thing that matters. Evictions and Expiries below are
// totals a caller can poll, which is enough to answer "did we lose anything?"
// at the end of a run. A callback fires AT THE MOMENT OF THE LOSS and names the
// key, which is what lets a Prometheus counter move in real time and a log line
// say which record's guarantee just went. A poll can only ever say that some
// key, at some point, was forgotten.
type Loss struct {
	Key    string
	Reason LossReason
}

// entry is one remembered key. The deadline is measured from the FIRST
// sighting and is NOT refreshed by later ones — the window means "we remember
// this key for ttl after first seeing it", not "for ttl after last seeing it".
// That choice is what keeps insertion order and expiry order the same, which is
// what lets expire sweep from the front and stop at the first live entry.
type entry struct {
	key      string
	count    int
	deadline time.Time // zero when the set has no TTL
}

// Set is a bounded seen-set. It is safe for concurrent use.
type Set struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	clock    Clock
	byKey    map[string]*list.Element
	order    *list.List // front is the oldest first-sighting
	onLoss   func(Loss)
	evicted  uint64
	expired  uint64
}

// New returns a set holding at most capacity keys, each for ttl.
//
// A capacity below one is raised to one: a set that holds nothing would answer
// first-seen to everything, which is a store that silently does not exist. A
// ttl of zero or less means entries never expire on time and capacity alone
// bounds the set. A nil clock reads the wall clock, and a nil onLoss reports
// nothing.
//
// onLoss IS CALLED WITH THE SET'S LOCK HELD, so it must not call back into this
// set — a re-entrant callback deadlocks. Incrementing a counter or writing a log
// line is what it is for. It is called under the lock deliberately rather than
// deferred to after the unlock: a loss reported out of order with the Observe
// that caused it would let a reader see a key answer first-seen before the
// notice explaining why.
func New(capacity int, ttl time.Duration, clock Clock, onLoss func(Loss)) *Set {
	if capacity < 1 {
		capacity = 1
	}
	if onLoss == nil {
		onLoss = func(Loss) {}
	}
	if clock == nil {
		clock = RealClock{}
	}
	return &Set{
		capacity: capacity,
		ttl:      ttl,
		clock:    clock,
		byKey:    make(map[string]*list.Element, capacity),
		order:    list.New(),
		onLoss:   onLoss,
	}
}

// Observe records one sighting of key and returns how many times it had been
// seen BEFORE this call. Zero means first-seen.
//
// THIS IS THE ONLY ENTRY POINT, AND THAT IS THE DESIGN. Testing and recording
// happen under one lock, so there is no window between "has this been seen?"
// and "remember that it has" for a second goroutine to slip through. A pair of
// Contains/Add methods would read more naturally at the call site and would be
// wrong under exactly the concurrency this exists to survive: two deliveries of
// the same key on two partitions' worth of work would both read absent, both
// record, and both apply.
func (s *Set) Observe(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	s.expire(now)

	if el, ok := s.byKey[key]; ok {
		e := el.Value.(*entry)
		prior := e.count
		e.count++
		return prior
	}

	if s.order.Len() >= s.capacity {
		el := s.order.Front()
		s.order.Remove(el)
		victim := el.Value.(*entry).key
		delete(s.byKey, victim)
		s.evicted++
		s.onLoss(Loss{Key: victim, Reason: LossCapacity})
	}

	e := &entry{key: key, count: 1}
	if s.ttl > 0 {
		e.deadline = now.Add(s.ttl)
	}
	s.byKey[key] = s.order.PushBack(e)
	return 0
}

// expire drops every entry whose deadline has passed. The caller holds the
// lock. The boundary is INCLUSIVE: an entry whose deadline equals now is gone,
// so a ttl of one second means a key survives strictly less than one second.
func (s *Set) expire(now time.Time) {
	if s.ttl <= 0 {
		return
	}
	for {
		el := s.order.Front()
		if el == nil {
			return
		}
		e := el.Value.(*entry)
		if e.deadline.After(now) {
			return
		}
		s.order.Remove(el)
		delete(s.byKey, e.key)
		s.expired++
		s.onLoss(Loss{Key: e.key, Reason: LossTTL})
	}
}

// Len reports how many keys the set is currently holding.
func (s *Set) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}

// Evictions reports how many keys were dropped to stay under capacity. Every
// one of them is a key whose redelivery would now read as first-seen.
func (s *Set) Evictions() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evicted
}

// Expiries reports how many keys were dropped for age. Same consequence as an
// eviction, on a timer instead of on pressure.
func (s *Set) Expiries() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expired
}
