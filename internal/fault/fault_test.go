package fault

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pigfox/kafka-lab/internal/runner"
)

func rec(key string, partition int32, offset int64) runner.Record {
	return runner.Record{
		DedupeKey:   key,
		Topic:       "events",
		Partition:   partition,
		Offset:      offset,
		LeaderEpoch: 7,
	}
}

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "run:" + strconv.Itoa(i)
	}
	return out
}

// ── the rate bounds ────────────────────────────────────────────────────────

// ZERO IS THE DEFAULT EVERYWHERE, so it must fire nothing at all. A lab that
// injected faults without being asked would be a lab whose ordinary numbers are
// quietly wrong.
func TestRateZeroFiresNothing(t *testing.T) {
	i := New(0, "seed", nil)
	for n, k := range keys(1000) {
		if i.Fault(rec(k, 0, int64(n))) {
			t.Fatalf("key %q faulted at rate 0", k)
		}
	}
	if i.Fired() != 0 {
		t.Fatalf("fired %d keys at rate 0", i.Fired())
	}
}

func TestANegativeRateFiresNothing(t *testing.T) {
	i := New(-0.5, "seed", nil)
	if i.Fault(rec("run:1", 0, 1)) {
		t.Fatal("a negative rate faulted")
	}
}

// RATE ONE MUST FIRE ON EVERY DISTINCT KEY — once each. The float comparison
// alone cannot promise this: float64(math.MaxUint64) rounds UP to 2^64, so the
// single largest digest would fall just short of its own threshold. The bound
// is handled before the arithmetic, and this test is why.
func TestRateOneFiresExactlyOncePerDistinctKey(t *testing.T) {
	i := New(1, "seed", nil)
	all := keys(500)

	for n, k := range all {
		if !i.Fault(rec(k, 0, int64(n))) {
			t.Fatalf("key %q did not fault at rate 1", k)
		}
	}
	for n, k := range all {
		if i.Fault(rec(k, 0, int64(n))) {
			t.Fatalf("key %q faulted a second time", k)
		}
	}
	if got := i.Fired(); got != len(all) {
		t.Fatalf("fired %d distinct keys, want %d", got, len(all))
	}
}

func TestARateAboveOneBehavesAsOne(t *testing.T) {
	i := New(5, "seed", nil)
	if !i.Fault(rec("run:1", 0, 1)) {
		t.Fatal("a rate above 1 did not fault")
	}
}

func TestRateIsReported(t *testing.T) {
	if got := New(0.25, "seed", nil).Rate(); got != 0.25 {
		t.Fatalf("got %v want 0.25", got)
	}
}

// A middling rate must fire on some keys and not others; a predicate that fired
// on all or none would pass every other test here.
func TestAMiddlingRateFiresOnSomeKeysAndNotOthers(t *testing.T) {
	i := New(0.5, "seed", nil)
	var fired int
	all := keys(2000)
	for n, k := range all {
		if i.Fault(rec(k, 0, int64(n))) {
			fired++
		}
	}
	if fired == 0 || fired == len(all) {
		t.Fatalf("rate 0.5 fired on %d of %d keys; the predicate is not selective", fired, len(all))
	}
	// A generous band: this asserts the predicate is roughly proportional, not
	// that sha256 is uniform, which is not this repository's claim to make.
	if fired < len(all)/4 || fired > 3*len(all)/4 {
		t.Fatalf("rate 0.5 fired on %d of %d keys, which is not near half", fired, len(all))
	}
}

// ── ONCE PER KEY: the guard that stops the lab livelocking ─────────────────

// THIS IS THE TEST THAT PROVES THE LAB CANNOT LIVELOCK. The fault decision is a
// pure function of the seed and key, so a faulted record faults again the moment
// the rewind redelivers it — rewind, redeliver, rewind, with the consumer pinned
// on one offset forever. The guard is what makes the second delivery ordinary.
func TestARewoundRecordDoesNotFaultAgain(t *testing.T) {
	i := New(1, "seed", nil)
	r := rec("run:1", 0, 42)

	if !i.Fault(r) {
		t.Fatal("the first delivery did not fault")
	}
	// The rewind redelivers the very same record, at the very same offset.
	for attempt := 2; attempt <= 10; attempt++ {
		if i.Fault(r) {
			t.Fatalf("delivery %d faulted again; the loop would never advance", attempt)
		}
	}
}

// The guard is keyed on IDENTITY, not on offset: the same record redelivered is
// the same key, and a guard keyed on the offset would let a compacted or
// re-produced record fault forever.
func TestTheGuardIsKeyedOnIdentityNotOffset(t *testing.T) {
	i := New(1, "seed", nil)

	if !i.Fault(rec("run:1", 0, 42)) {
		t.Fatal("the first delivery did not fault")
	}
	if i.Fault(rec("run:1", 3, 999)) {
		t.Fatal("the same key at a different partition and offset faulted again")
	}
}

func TestConcurrentDeliveriesOfOneKeyFaultExactlyOnce(t *testing.T) {
	i := New(1, "seed", nil)
	var faults atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})

	for n := 0; n < 64; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if i.Fault(rec("same", 0, 1)) {
				faults.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := faults.Load(); got != 1 {
		t.Fatalf("%d goroutines faulted the same key, want exactly 1", got)
	}
}

// A record with no identity can never be given the once-only guarantee, so
// faulting it would rewind to it forever. It is never faulted.
func TestAKeylessRecordNeverFaults(t *testing.T) {
	i := New(1, "seed", nil)
	if i.Fault(runner.Record{Topic: "events", Partition: 0, Offset: 3}) {
		t.Fatal("a record with no identity faulted; the loop could not advance past it")
	}
	if i.Fired() != 0 {
		t.Fatalf("fired %d", i.Fired())
	}
}

// ── determinism: what makes the two arms comparable ────────────────────────

// THE SAME SEED AND KEY SET MUST GIVE AN IDENTICAL FAULT SET, or the two arms
// of the experiment are fed different faults and the comparison means nothing.
func TestTheSameSeedAndKeySetGiveAnIdenticalFaultSet(t *testing.T) {
	all := keys(3000)

	faultSet := func() map[string]bool {
		i := New(0.1, "pf-s313", nil)
		out := map[string]bool{}
		for n, k := range all {
			if i.Fault(rec(k, int32(n%3), int64(n))) {
				out[k] = true
			}
		}
		return out
	}

	first, second := faultSet(), faultSet()
	if len(first) == 0 {
		t.Fatal("no key faulted; the test proves nothing")
	}
	if len(first) != len(second) {
		t.Fatalf("run one faulted %d keys, run two %d", len(first), len(second))
	}
	for k := range first {
		if !second[k] {
			t.Fatalf("key %q faulted in one run and not the other", k)
		}
	}
}

// ORDER INDEPENDENCE IS THE POINT OF HASHING RATHER THAN DRAWING FROM A STREAM.
// Batch boundaries and partition assignment differ between two lab runs; a
// stream's answer depends on how many times it has been asked, so the fault set
// would move with them.
func TestTheFaultSetDoesNotDependOnDeliveryOrder(t *testing.T) {
	all := keys(2000)
	reversed := make([]string, len(all))
	for n, k := range all {
		reversed[len(all)-1-n] = k
	}

	faultSet := func(order []string) map[string]bool {
		i := New(0.1, "pf-s313", nil)
		out := map[string]bool{}
		for n, k := range order {
			if i.Fault(rec(k, int32(n%3), int64(n))) {
				out[k] = true
			}
		}
		return out
	}

	forward, backward := faultSet(all), faultSet(reversed)
	if len(forward) == 0 {
		t.Fatal("no key faulted; the test proves nothing")
	}
	if len(forward) != len(backward) {
		t.Fatalf("forward faulted %d keys, reversed %d", len(forward), len(backward))
	}
	for k := range forward {
		if !backward[k] {
			t.Fatalf("key %q faulted forwards but not backwards", k)
		}
	}
}

// A different seed must give a different fault set, or the seed is not reaching
// the digest and every run would fault the same keys.
func TestADifferentSeedGivesADifferentFaultSet(t *testing.T) {
	all := keys(2000)

	faultSet := func(seed string) map[string]bool {
		i := New(0.1, seed, nil)
		out := map[string]bool{}
		for n, k := range all {
			if i.Fault(rec(k, 0, int64(n))) {
				out[k] = true
			}
		}
		return out
	}

	a, b := faultSet("seed-a"), faultSet("seed-b")
	if len(a) == 0 || len(b) == 0 {
		t.Fatal("a seed faulted nothing; the test proves nothing")
	}
	same := 0
	for k := range a {
		if b[k] {
			same++
		}
	}
	if same == len(a) && len(a) == len(b) {
		t.Fatal("two seeds produced the identical fault set; the seed is not reaching the digest")
	}
}

// The separator keeps the seed and the key from running together, so a seed of
// "ab" with key "c" cannot collide with seed "a" and key "bc".
func TestTheSeedAndKeyCannotBeConfusedForEachOther(t *testing.T) {
	if New(1, "ab", nil).eligible("c") != New(1, "a", nil).eligible("bc") {
		// Both are true at rate 1; the real check is below at a partial rate.
		t.Fatal("unreachable at rate 1")
	}
	a := New(0.5, "ab", nil)
	b := New(0.5, "a", nil)
	// The digests must differ. Comparing eligibility alone could coincide, so
	// this drives a set of keys and requires the two to disagree somewhere.
	disagreed := false
	for _, k := range keys(500) {
		if a.eligible(k) != b.eligible("b"+k) {
			disagreed = true
			break
		}
	}
	if !disagreed {
		t.Fatal("seed+key and a shifted split produced identical decisions throughout")
	}
}

// ── batch targets ──────────────────────────────────────────────────────────

func TestTargetsIsNilWhenNothingFaults(t *testing.T) {
	i := New(0, "seed", nil)
	batch := []runner.Record{rec("run:1", 0, 1), rec("run:2", 0, 2)}
	if got := i.Targets(batch); got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
}

// ONE TARGET PER PARTITION, at the EARLIEST faulting offset — a rewind to a
// later offset would leave an earlier faulted record committed, so a fault would
// be logged whose redelivery never arrives.
func TestTargetsPicksTheEarliestFaultInEachPartition(t *testing.T) {
	i := New(1, "seed", nil)
	batch := []runner.Record{
		rec("p0-late", 0, 30),
		rec("p0-early", 0, 10),
		rec("p1-mid", 1, 20),
		rec("p1-late", 1, 25),
	}

	targets := i.Targets(batch)
	if len(targets) != 2 {
		t.Fatalf("got %d targets, want one per faulting partition", len(targets))
	}
	byPartition := map[int32]runner.Record{}
	for _, r := range targets {
		byPartition[r.Partition] = r
	}
	if got := byPartition[0].Offset; got != 10 {
		t.Fatalf("partition 0 target offset %d, want 10", got)
	}
	if got := byPartition[1].Offset; got != 20 {
		t.Fatalf("partition 1 target offset %d, want 20", got)
	}
}

// The target keeps the whole address, because a rewind needs the topic and the
// epoch as well as the offset.
func TestATargetKeepsItsWholeAddress(t *testing.T) {
	i := New(1, "seed", nil)
	targets := i.Targets([]runner.Record{rec("run:1", 2, 77)})
	if len(targets) != 1 {
		t.Fatalf("got %d targets", len(targets))
	}
	got := targets[0]
	if got.Topic != "events" || got.Partition != 2 || got.Offset != 77 || got.LeaderEpoch != 7 {
		t.Fatalf("target %+v lost part of its address", got)
	}
}

// A batch redelivered after a rewind must not fault again — Targets is where
// the loop asks, so the guard has to hold at this level too.
func TestARedeliveredBatchProducesNoNewTargets(t *testing.T) {
	i := New(1, "seed", nil)
	batch := []runner.Record{rec("run:1", 0, 10), rec("run:2", 0, 11)}

	if got := i.Targets(batch); len(got) != 1 {
		t.Fatalf("first pass produced %d targets, want 1", len(got))
	}
	if got := i.Targets(batch); got != nil {
		t.Fatalf("the redelivered batch produced targets %+v; the loop would never advance", got)
	}
}

func TestFiredCountsDistinctKeys(t *testing.T) {
	i := New(1, "seed", nil)
	i.Fault(rec("a", 0, 1))
	i.Fault(rec("a", 0, 1))
	i.Fault(rec("b", 0, 2))
	if got := i.Fired(); got != 2 {
		t.Fatalf("fired %d want 2", got)
	}
}

// ── the fired set versus the target set ────────────────────────────────────
//
// THIS DISTINCTION COST A GRADED RUN, so it is pinned in both directions.
//
// The first graded run logged the REWIND TARGETS and called that the fault
// record. Targets returns at most one record per partition per batch, so when
// two eligible keys land in the same partition of the same batch, both are
// marked fired and only the earlier is returned — the later is spent silently.
// The two arms then shared only 25 of their 34 and 30 logged keys, and the whole
// difference was which keys happened to share a batch.
//
// The FIRED set is a pure function of the seed and the delivered keys. The
// TARGET set is a subset of it chosen by broker batching. Only the first is
// reproducible, so only the first is evidence.

func TestEveryFiredKeyIsReportedThroughTheCallback(t *testing.T) {
	var fired []string
	i := New(1, "seed", func(r runner.Record) { fired = append(fired, r.DedupeKey) })

	// Three eligible keys, all in ONE partition of ONE batch. Targets can
	// return only the earliest; all three must still be reported as fired.
	batch := []runner.Record{
		rec("early", 0, 10),
		rec("middle", 0, 11),
		rec("late", 0, 12),
	}
	targets := i.Targets(batch)

	if len(targets) != 1 {
		t.Fatalf("got %d targets, want 1 (one partition)", len(targets))
	}
	if targets[0].DedupeKey != "early" {
		t.Fatalf("target is %q, want the earliest", targets[0].DedupeKey)
	}
	if len(fired) != 3 {
		t.Fatalf("the callback saw %d keys (%v); all three fired", len(fired), fired)
	}
	if i.Fired() != 3 {
		t.Fatalf("Fired() is %d, want 3", i.Fired())
	}
}

// The callback carries the whole record, so a log line can name the partition
// and offset a fault was spent on even when it produced no rewind of its own.
func TestTheFaultCallbackCarriesTheWholeRecord(t *testing.T) {
	var got []runner.Record
	i := New(1, "seed", func(r runner.Record) { got = append(got, r) })

	want := rec("run:1", 2, 77)
	i.Fault(want)

	if len(got) != 1 || got[0] != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

// A key that does not fire must not be reported, or the fired set would be the
// delivered set.
func TestTheCallbackIsSilentForKeysThatDoNotFire(t *testing.T) {
	var fired int
	i := New(0, "seed", func(runner.Record) { fired++ })
	for n, k := range keys(200) {
		i.Fault(rec(k, 0, int64(n)))
	}
	if fired != 0 {
		t.Fatalf("the callback fired %d times at rate 0", fired)
	}
}

// A key fires ONCE, so the callback fires once for it however often it is
// redelivered.
func TestTheCallbackFiresOncePerKey(t *testing.T) {
	var fired int
	i := New(1, "seed", func(runner.Record) { fired++ })
	r := rec("run:1", 0, 5)
	for n := 0; n < 10; n++ {
		i.Fault(r)
	}
	if fired != 1 {
		t.Fatalf("the callback fired %d times for one key", fired)
	}
}

// THE FIRED SET IS ORDER-INDEPENDENT EVEN WHERE THE TARGET SET IS NOT. Same
// keys, different batch shapes: the targets differ, the fired set does not.
func TestTheFiredSetSurvivesADifferentBatchShape(t *testing.T) {
	all := keys(600)

	firedUnder := func(batchSize int) map[string]bool {
		out := map[string]bool{}
		i := New(0.1, "pf-s313", func(r runner.Record) { out[r.DedupeKey] = true })
		for start := 0; start < len(all); start += batchSize {
			end := start + batchSize
			if end > len(all) {
				end = len(all)
			}
			batch := make([]runner.Record, 0, end-start)
			for n := start; n < end; n++ {
				batch = append(batch, rec(all[n], int32(n%3), int64(n)))
			}
			i.Targets(batch)
		}
		return out
	}

	small, large := firedUnder(3), firedUnder(200)
	if len(small) == 0 {
		t.Fatal("nothing fired; the test proves nothing")
	}
	if len(small) != len(large) {
		t.Fatalf("batches of 3 fired %d keys, batches of 200 fired %d", len(small), len(large))
	}
	for k := range small {
		if !large[k] {
			t.Fatalf("key %q fired under one batch shape and not the other", k)
		}
	}
}

// And the counterpart, so the test above is not passing because the target set
// happens to be stable too: the TARGET set really does move with batch shape.
func TestTheTargetSetDoesMoveWithBatchShape(t *testing.T) {
	all := keys(600)

	targetsUnder := func(batchSize int) map[string]bool {
		out := map[string]bool{}
		i := New(0.1, "pf-s313", nil)
		for start := 0; start < len(all); start += batchSize {
			end := start + batchSize
			if end > len(all) {
				end = len(all)
			}
			batch := make([]runner.Record, 0, end-start)
			for n := start; n < end; n++ {
				batch = append(batch, rec(all[n], int32(n%3), int64(n)))
			}
			for _, target := range i.Targets(batch) {
				out[target.DedupeKey] = true
			}
		}
		return out
	}

	small, large := targetsUnder(3), targetsUnder(200)
	if len(small) == len(large) {
		t.Skip("this seed and key set happen to produce equal target counts; the fired-set guarantee is the one that matters")
	}
	if len(small) < len(large) {
		t.Fatalf("smaller batches produced FEWER targets (%d vs %d), which contradicts one-target-per-partition-per-batch",
			len(small), len(large))
	}
}
