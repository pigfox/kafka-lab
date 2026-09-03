package apply

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pigfox/kafka-lab/internal/idem"
	"github.com/pigfox/kafka-lab/internal/runner"
)

// countingObserver tallies the four outcomes independently of the Applier's own
// counters, so a test can catch an Applier that counts one thing and publishes
// another.
type countingObserver struct {
	applied, suppressed, double, noKey atomic.Int64
}

func (o *countingObserver) Applied()       { o.applied.Add(1) }
func (o *countingObserver) Suppressed()    { o.suppressed.Add(1) }
func (o *countingObserver) DoubleApplied() { o.double.Add(1) }
func (o *countingObserver) NoKey()         { o.noKey.Add(1) }

type effectLog struct {
	mu   sync.Mutex
	recs []runner.Record
}

func (e *effectLog) record(r runner.Record) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recs = append(e.recs, r)
}

func (e *effectLog) len() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.recs)
}

func rec(key string) runner.Record {
	return runner.Record{DedupeKey: key, Partition: 0, Offset: 1}
}

// A clock frozen for the whole test, so no TTL can expire underneath an
// assertion about capacity or duplication.
type frozenClock struct{ t time.Time }

func (c frozenClock) Now() time.Time { return c.t }

func freeze() frozenClock {
	return frozenClock{t: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)}
}

func stores(capacity int) (*idem.Set, *idem.Set) {
	return idem.New(capacity, time.Minute, freeze()), idem.New(capacity, time.Minute, freeze())
}

// ── THE AT-LEAST-ONCE ARM: dedupe OFF ──────────────────────────────────────

func TestWithoutDedupeARedeliveryIsAppliedTwiceAndCounted(t *testing.T) {
	seen, applied := stores(64)
	eff := &effectLog{}
	obs := &countingObserver{}
	a := New(Options{Dedupe: false, Seen: seen, AppliedKeys: applied, Effect: eff.record, Observer: obs})

	a.Apply(rec("run:1"))
	a.Apply(rec("run:1"))

	c := a.Counts()
	if c.Applied != 2 {
		t.Fatalf("applied %d want 2 — both deliveries must run the effect", c.Applied)
	}
	if c.DoubleApplied != 1 {
		t.Fatalf("double applied %d want 1", c.DoubleApplied)
	}
	if c.Suppressed != 0 {
		t.Fatalf("suppressed %d want 0 — dedupe is off", c.Suppressed)
	}
	if eff.len() != 2 {
		t.Fatalf("the effect ran %d times, want 2", eff.len())
	}
	if obs.applied.Load() != 2 || obs.double.Load() != 1 || obs.suppressed.Load() != 0 {
		t.Fatalf("observer disagrees with the counters: applied=%d double=%d suppressed=%d",
			obs.applied.Load(), obs.double.Load(), obs.suppressed.Load())
	}
}

// With dedupe off the seen-set must not be touched at all, or the two arms
// would differ in memory pressure as well as in behaviour.
func TestWithoutDedupeTheSeenSetIsNeverConsulted(t *testing.T) {
	seen, applied := stores(64)
	a := New(Options{Dedupe: false, Seen: seen, AppliedKeys: applied})

	a.Apply(rec("run:1"))
	a.Apply(rec("run:2"))

	if seen.Len() != 0 {
		t.Fatalf("the seen-set holds %d keys with dedupe off", seen.Len())
	}
}

func TestWithoutDedupeDistinctKeysAreNotDoubleApplied(t *testing.T) {
	seen, applied := stores(64)
	a := New(Options{Dedupe: false, Seen: seen, AppliedKeys: applied})

	for i := 0; i < 20; i++ {
		a.Apply(rec("run:" + strconv.Itoa(i)))
	}

	c := a.Counts()
	if c.Applied != 20 {
		t.Fatalf("applied %d want 20", c.Applied)
	}
	if c.DoubleApplied != 0 {
		t.Fatalf("double applied %d want 0 — every key was distinct", c.DoubleApplied)
	}
}

// ── THE IDEMPOTENT ARM: dedupe ON ──────────────────────────────────────────

func TestWithDedupeARedeliveryIsSuppressedAndNeverApplied(t *testing.T) {
	seen, applied := stores(64)
	eff := &effectLog{}
	obs := &countingObserver{}
	a := New(Options{Dedupe: true, Seen: seen, AppliedKeys: applied, Effect: eff.record, Observer: obs})

	a.Apply(rec("run:1"))
	a.Apply(rec("run:1"))
	a.Apply(rec("run:1"))

	c := a.Counts()
	if c.Applied != 1 {
		t.Fatalf("applied %d want 1", c.Applied)
	}
	if c.Suppressed != 2 {
		t.Fatalf("suppressed %d want 2", c.Suppressed)
	}
	if c.DoubleApplied != 0 {
		t.Fatalf("double applied %d want 0 — dedupe is the whole point", c.DoubleApplied)
	}
	if eff.len() != 1 {
		t.Fatalf("the effect ran %d times, want 1", eff.len())
	}
	if obs.suppressed.Load() != 2 || obs.applied.Load() != 1 {
		t.Fatalf("observer disagrees: applied=%d suppressed=%d", obs.applied.Load(), obs.suppressed.Load())
	}
}

// A suppressed record must NOT reach the apply tally, or the dedupe arm would
// report the duplicates it just prevented.
func TestASuppressedRecordDoesNotReachTheApplyTally(t *testing.T) {
	seen, applied := stores(64)
	a := New(Options{Dedupe: true, Seen: seen, AppliedKeys: applied})

	a.Apply(rec("run:1"))
	a.Apply(rec("run:1"))

	if applied.Len() != 1 {
		t.Fatalf("the apply tally holds %d keys, want 1", applied.Len())
	}
	if got := a.Counts().DoubleApplied; got != 0 {
		t.Fatalf("double applied %d want 0", got)
	}
}

func TestWithDedupeDistinctKeysAllApply(t *testing.T) {
	seen, applied := stores(64)
	a := New(Options{Dedupe: true, Seen: seen, AppliedKeys: applied})

	for i := 0; i < 20; i++ {
		a.Apply(rec("run:" + strconv.Itoa(i)))
	}

	c := a.Counts()
	if c.Applied != 20 || c.Suppressed != 0 {
		t.Fatalf("applied=%d suppressed=%d, want 20 and 0", c.Applied, c.Suppressed)
	}
}

// ── THE TWO ARMS SIDE BY SIDE ──────────────────────────────────────────────
//
// This is the shape PF-S312 will measure over a real stream: the same input,
// the same effect, the flag as the only difference.
func TestTheTwoArmsDifferOnlyInSuppression(t *testing.T) {
	input := []runner.Record{
		rec("run:1"), rec("run:2"), rec("run:1"), rec("run:3"), rec("run:2"), rec("run:1"),
	}

	off := New(Options{Dedupe: false})
	on := New(Options{Dedupe: true})
	for _, r := range input {
		off.Apply(r)
		on.Apply(r)
	}

	offCounts, onCounts := off.Counts(), on.Counts()

	if offCounts.Applied != 6 {
		t.Fatalf("dedupe off applied %d of 6", offCounts.Applied)
	}
	if offCounts.DoubleApplied != 3 {
		t.Fatalf("dedupe off double-applied %d, want 3 (two repeats of run:1, one of run:2)", offCounts.DoubleApplied)
	}
	if onCounts.Applied != 3 {
		t.Fatalf("dedupe on applied %d, want 3 distinct keys", onCounts.Applied)
	}
	if onCounts.DoubleApplied != 0 {
		t.Fatalf("dedupe on double-applied %d, want 0", onCounts.DoubleApplied)
	}
	if onCounts.Suppressed != 3 {
		t.Fatalf("dedupe on suppressed %d, want 3", onCounts.Suppressed)
	}
}

// ── RECORDS WITH NO IDENTITY ───────────────────────────────────────────────

// COUNTED AND EXPOSED, NEVER SILENTLY PASSED — and still applied. Dropping it
// would lose data over a missing header; calling it deduplicated would be a lie.
func TestARecordWithNoKeyIsCountedAndStillApplied(t *testing.T) {
	eff := &effectLog{}
	obs := &countingObserver{}
	a := New(Options{Dedupe: true, Effect: eff.record, Observer: obs})

	a.Apply(runner.Record{DedupeKey: "", Partition: 3, Offset: 11})

	c := a.Counts()
	if c.NoKey != 1 {
		t.Fatalf("no-key count %d want 1", c.NoKey)
	}
	if c.Applied != 1 {
		t.Fatalf("applied %d want 1 — a keyless record is still delivered", c.Applied)
	}
	if eff.len() != 1 {
		t.Fatalf("the effect ran %d times, want 1", eff.len())
	}
	if obs.noKey.Load() != 1 {
		t.Fatalf("the observer was not told: noKey=%d", obs.noKey.Load())
	}
}

// Two keyless records are two applies and NOT a duplicate: they have no
// identity, so nothing licenses calling them the same record.
func TestKeylessRecordsAreNeverJudgedDuplicates(t *testing.T) {
	a := New(Options{Dedupe: true})

	a.Apply(runner.Record{Offset: 1})
	a.Apply(runner.Record{Offset: 2})

	c := a.Counts()
	if c.NoKey != 2 || c.Applied != 2 {
		t.Fatalf("noKey=%d applied=%d, want 2 and 2", c.NoKey, c.Applied)
	}
	if c.Suppressed != 0 || c.DoubleApplied != 0 {
		t.Fatalf("suppressed=%d double=%d, want 0 and 0", c.Suppressed, c.DoubleApplied)
	}
}

// ── WHAT A BOUNDED STORE COSTS ─────────────────────────────────────────────

// The dedupe guarantee is lost for an evicted key, and the loss is visible in
// the store's eviction count rather than only in the doc comment.
func TestAnEvictedKeyIsAppliedAgainOnTheDedupeArm(t *testing.T) {
	seen := idem.New(2, time.Minute, freeze())
	applied := idem.New(64, time.Minute, freeze())
	a := New(Options{Dedupe: true, Seen: seen, AppliedKeys: applied})

	a.Apply(rec("victim"))
	a.Apply(rec("b"))
	a.Apply(rec("c")) // capacity 2, so "victim" is gone from the seen-set
	a.Apply(rec("victim"))

	c := a.Counts()
	if c.Suppressed != 0 {
		t.Fatalf("suppressed %d want 0 — the key had been evicted", c.Suppressed)
	}
	if c.DoubleApplied != 1 {
		t.Fatalf("double applied %d want 1 — the slip must be visible", c.DoubleApplied)
	}
	if a.Seen().Evictions() == 0 {
		t.Fatal("the eviction was not counted; the guarantee was lost in silence")
	}
}

// The double-apply count is a LOWER BOUND: a key evicted from the apply tally
// stops being recognisable as a repeat.
func TestTheDoubleApplyCountIsALowerBound(t *testing.T) {
	seen := idem.New(64, time.Minute, freeze())
	applied := idem.New(2, time.Minute, freeze())
	a := New(Options{Dedupe: false, Seen: seen, AppliedKeys: applied})

	a.Apply(rec("victim"))
	a.Apply(rec("b"))
	a.Apply(rec("c")) // "victim" leaves the apply tally
	a.Apply(rec("victim"))

	if got := a.Counts().DoubleApplied; got != 0 {
		t.Fatalf("double applied %d; this test exists to pin that an evicted key reads as new (%d evictions)",
			got, a.AppliedKeys().Evictions())
	}
	if a.AppliedKeys().Evictions() == 0 {
		t.Fatal("nothing was evicted; the test no longer proves what it claims")
	}
}

// ── CONCURRENCY ────────────────────────────────────────────────────────────

// The consume loop is single-goroutine today. This asserts the Applier does not
// depend on that, because an applier that is only safe by accident breaks the
// first time someone adds a second consume loop.
func TestConcurrentDeliveriesOfOneKeyApplyItExactlyOnceWithDedupeOn(t *testing.T) {
	eff := &effectLog{}
	a := New(Options{Dedupe: true, Effect: eff.record})

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			a.Apply(rec("same"))
		}()
	}
	close(start)
	wg.Wait()

	c := a.Counts()
	if c.Applied != 1 {
		t.Fatalf("applied %d times under concurrency, want 1", c.Applied)
	}
	if c.Suppressed != 63 {
		t.Fatalf("suppressed %d, want 63", c.Suppressed)
	}
	if eff.len() != 1 {
		t.Fatalf("the effect ran %d times, want 1", eff.len())
	}
}

// ── DEFAULTS AND ACCESSORS ─────────────────────────────────────────────────

func TestZeroOptionsBuildAWorkingApplier(t *testing.T) {
	a := New(Options{})
	a.Apply(rec("a")) // nil Effect and nil Observer must not panic
	a.Apply(rec("a"))

	if a.Dedupe() {
		t.Fatal("dedupe must be OFF by default — the at-least-once arm is the default")
	}
	if a.Seen() == nil || a.AppliedKeys() == nil {
		t.Fatal("nil stores must be replaced with defaults")
	}
	if got := a.Counts().DoubleApplied; got != 1 {
		t.Fatalf("double applied %d want 1", got)
	}
}

func TestDedupeReportsTheFlag(t *testing.T) {
	if New(Options{Dedupe: true}).Dedupe() != true {
		t.Fatal("Dedupe() disagrees with the option")
	}
	if New(Options{Dedupe: false}).Dedupe() != false {
		t.Fatal("Dedupe() disagrees with the option")
	}
}

func TestTheDefaultTTLMatchesTheTopicRetention(t *testing.T) {
	// The events topic is created with retention.ms=600000 in
	// internal/kafkabus. Remembering a key for longer than the broker keeps
	// the record buys nothing, because it can no longer be redelivered.
	if DefaultTTL != 10*time.Minute {
		t.Fatalf("DefaultTTL is %v; the events topic retains 10m", DefaultTTL)
	}
	// docker-compose.yml restates both defaults as ${KL_DEDUPE_CAPACITY:-50000}
	// and ${KL_DEDUPE_TTL:-10m}, following the convention internal/config
	// documents: the compose file has to show a knob for it to be
	// discoverable, and the binary has to work when run outside compose. That
	// duplication is accepted deliberately and is not checked mechanically, so
	// a change here is a change that must be made in the compose file too.
	if DefaultCapacity != 50_000 {
		t.Fatalf("DefaultCapacity is %d; docker-compose.yml still says 50000", DefaultCapacity)
	}
}

func TestNopObserverAbsorbsEveryOutcome(t *testing.T) {
	var o Observer = nopObserver{}
	o.Applied()
	o.Suppressed()
	o.DoubleApplied()
	o.NoKey()
}

// The Applier must satisfy the interface the consume loop takes; a signature
// drift here is a compile error rather than a runtime surprise.
func TestApplierSatisfiesRecordApplier(t *testing.T) {
	var _ runner.RecordApplier = New(Options{})
}
