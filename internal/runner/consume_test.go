package runner

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/ratelimit"
)

// fakeFetcher hands out fixed batches and then blocks until the context ends,
// so a test can assert exactly what one batch does.
//
// A batch is described by its SIZE and the records are synthesised, because
// almost every test here is about the throttle rather than about the payload.
// The few that care what a record carries set `records` instead.
type fakeFetcher struct {
	mu       sync.Mutex
	batches  []int
	records  [][]Record
	fetchErr []error
	fetches  atomic.Int64
	commits  atomic.Int64
	commitEr error
	rewinds  [][]Record
}

func (f *fakeFetcher) Fetch(ctx context.Context) ([]Record, error) {
	i := int(f.fetches.Add(1)) - 1

	f.mu.Lock()
	var recs []Record
	var err error
	switch {
	case i < len(f.records):
		recs = f.records[i]
	case i < len(f.batches):
		recs = synth(i, f.batches[i])
	}
	if i < len(f.fetchErr) {
		err = f.fetchErr[i]
	}
	exhausted := i >= len(f.batches) && i >= len(f.records) && i >= len(f.fetchErr)
	f.mu.Unlock()

	if exhausted {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return recs, err
}

// synth builds n distinct records for batch b.
func synth(b, n int) []Record {
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{
			DedupeKey: "b" + strconv.Itoa(b) + "-r" + strconv.Itoa(i),
			Partition: int32(b % 3),
			Offset:    int64(b*1000 + i),
		}
	}
	return recs
}

func (f *fakeFetcher) Commit(context.Context) error {
	f.commits.Add(1)
	return f.commitEr
}

func (f *fakeFetcher) Rewind(to []Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rewinds = append(f.rewinds, append([]Record(nil), to...))
}

func (f *fakeFetcher) rewindsSnapshot() [][]Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]Record(nil), f.rewinds...)
}

// fixedFaulter faults on a named set of keys, so a loop test can drive the
// fault window without computing a sha256 digest to find a key that qualifies.
// What internal/fault decides is that package's business; what the loop does
// once it has decided is this one's.
type fixedFaulter struct {
	mu      sync.Mutex
	keys    map[string]bool
	fired   map[string]bool
	batches atomic.Int64
}

func faultOn(keys ...string) *fixedFaulter {
	f := &fixedFaulter{keys: map[string]bool{}, fired: map[string]bool{}}
	for _, k := range keys {
		f.keys[k] = true
	}
	return f
}

// Targets mirrors the real injector: earliest faulting record per partition,
// and once per key.
func (f *fixedFaulter) Targets(recs []Record) []Record {
	f.batches.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	targets := map[int32]Record{}
	for _, r := range recs {
		if !f.keys[r.DedupeKey] || f.fired[r.DedupeKey] {
			continue
		}
		f.fired[r.DedupeKey] = true
		if prior, ok := targets[r.Partition]; !ok || r.Offset < prior.Offset {
			targets[r.Partition] = r
		}
	}
	if len(targets) == 0 {
		return nil
	}
	out := make([]Record, 0, len(targets))
	for _, r := range targets {
		out = append(out, r)
	}
	return out
}

// recordSink keeps every record it was handed, in order.
type recordSink struct {
	mu   sync.Mutex
	recs []Record
	n    atomic.Int64
}

func (a *recordSink) Apply(r Record) {
	a.mu.Lock()
	a.recs = append(a.recs, r)
	a.mu.Unlock()
	a.n.Add(1)
}

func (a *recordSink) snapshotRecords() []Record {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Record(nil), a.recs...)
}

// countingSleeper records the delays asked for WITHOUT SPENDING THEM. A test
// that actually slept 500ms per message would take longer than the demo.
type countingSleeper struct {
	mu     sync.Mutex
	delays []time.Duration
	err    error
}

func (c *countingSleeper) Sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delays = append(c.delays, d)
	return c.err
}

func (c *countingSleeper) snapshot() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.delays...)
}

func workAt(ms int64) *atomic.Int64 {
	v := &atomic.Int64{}
	v.Store(ms)
	return v
}

func TestConsumeLoopProcessesEveryRecordInABatch(t *testing.T) {
	f := &fakeFetcher{batches: []int{5}}
	a := &recordSink{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, a, nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if a.n.Load() != 5 {
		t.Fatalf("applied %d of 5", a.n.Load())
	}
	if f.commits.Load() != 1 {
		t.Fatalf("committed %d times, want 1 per batch", f.commits.Load())
	}
}

// THE LOOP HANDS THE WHOLE RECORD TO THE APPLIER, not a count. A count cannot
// say whether the SAME record went past twice, which is the only question a
// delivery-semantics demo is about.
func TestConsumeLoopPassesEachRecordThroughIntact(t *testing.T) {
	want := []Record{
		{DedupeKey: "run:1", Partition: 0, Offset: 100},
		{DedupeKey: "run:2", Partition: 2, Offset: 101},
		{DedupeKey: "", Partition: 1, Offset: 7}, // no identity, still delivered
	}
	f := &fakeFetcher{records: [][]Record{want}}
	a := &recordSink{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, a, nil, nil)

	got := a.snapshotRecords()
	if len(got) != len(want) {
		t.Fatalf("applied %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

// A record with no dedupe header must reach the applier with an EMPTY key. The
// loop inventing one would make every redelivery look new, which is worse than
// admitting the record cannot be deduplicated.
func TestConsumeLoopDoesNotSynthesiseAMissingDedupeKey(t *testing.T) {
	f := &fakeFetcher{records: [][]Record{{{Partition: 4, Offset: 9}}}}
	a := &recordSink{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, a, nil, nil)

	got := a.snapshotRecords()
	if len(got) != 1 {
		t.Fatalf("applied %d records, want 1", len(got))
	}
	if got[0].DedupeKey != "" {
		t.Fatalf("the loop invented the key %q", got[0].DedupeKey)
	}
}

// THE PER-RECORD THROTTLE IS WHAT MAKES LAG BUILD. Throttling per BATCH would
// give a consumer whose achieved rate depends on batch size — drag the slider
// to 1/s and it would still drain hundreds a second, so the lag panel would sit
// flat while the slider said starved.
func TestConsumeLoopThrottlesPerRecordNotPerBatch(t *testing.T) {
	f := &fakeFetcher{batches: []int{4}}
	lim := ratelimit.New(20) // 50ms apart
	a := &recordSink{}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = ConsumeLoop(ctx, quiet(), lim, workAt(0), &countingSleeper{}, f, a, nil, nil)
	elapsed := time.Since(start)

	// Four records at 20/s is three inter-record gaps: ~150ms. A per-batch
	// throttle would have finished in one gap.
	if a.n.Load() < 4 {
		t.Fatalf("applied %d of 4 in %v", a.n.Load(), elapsed)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("a 4-record batch at 20/s finished in %v; the throttle is per batch", elapsed)
	}
}

// The work delay is read PER RECORD from the atomic, so dragging the work
// slider takes effect on the very next message rather than at the end of a
// batch that may be thousands long.
func TestConsumeLoopReadsTheWorkDelayPerRecord(t *testing.T) {
	f := &fakeFetcher{batches: []int{3}}
	sleeper := &countingSleeper{}
	work := workAt(10)

	// Flip the setting after the first record is under way.
	go func() {
		waitForCount(sleeper, 1)
		work.Store(40)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), work, sleeper, f, nil, nil, nil)

	delays := sleeper.snapshot()
	if len(delays) < 3 {
		t.Fatalf("got %d delays, want 3", len(delays))
	}
	if delays[0] != 10*time.Millisecond {
		t.Fatalf("first delay %v want 10ms", delays[0])
	}
	if delays[len(delays)-1] != 40*time.Millisecond {
		t.Fatalf("last delay %v want 40ms — the new setting was not picked up mid-batch", delays[len(delays)-1])
	}
}

func TestConsumeLoopSkipsSleepingWhenWorkIsZero(t *testing.T) {
	f := &fakeFetcher{batches: []int{3}}
	sleeper := &countingSleeper{}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0), sleeper, f, nil, nil, nil)

	if got := len(sleeper.snapshot()); got != 0 {
		t.Fatalf("work of 0 must not call the sleeper; it was called %d times", got)
	}
}

func TestConsumeLoopSurvivesFetchFailures(t *testing.T) {
	f := &fakeFetcher{batches: []int{0, 2}, fetchErr: []error{errFake, nil}}
	a := &recordSink{}
	var reported atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, a, nil, func(error) { reported.Add(1) })

	if reported.Load() != 1 {
		t.Fatalf("reported %d fetch errors, want 1", reported.Load())
	}
	if a.n.Load() != 2 {
		t.Fatalf("the loop did not recover: applied %d", a.n.Load())
	}
}

func TestConsumeLoopSurvivesCommitFailures(t *testing.T) {
	f := &fakeFetcher{batches: []int{1, 1}, commitEr: errFake}
	var reported atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, nil, nil, func(error) { reported.Add(1) })

	if reported.Load() < 2 {
		t.Fatalf("reported %d commit errors; the loop stopped early", reported.Load())
	}
}

func TestConsumeLoopStopsOnAContextErrorFromFetch(t *testing.T) {
	f := &fakeFetcher{batches: []int{0}, fetchErr: []error{context.Canceled}}
	err := ConsumeLoop(context.Background(), quiet(), ratelimit.New(control.MaxRatePerSec),
		workAt(0), &countingSleeper{}, f, nil, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestConsumeLoopStopsOnAContextErrorFromCommit(t *testing.T) {
	f := &fakeFetcher{batches: []int{1}, commitEr: context.Canceled}
	err := ConsumeLoop(context.Background(), quiet(), ratelimit.New(control.MaxRatePerSec),
		workAt(0), &countingSleeper{}, f, nil, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestConsumeLoopStopsWhenTheSleeperIsInterrupted(t *testing.T) {
	f := &fakeFetcher{batches: []int{5}}
	err := ConsumeLoop(context.Background(), quiet(), ratelimit.New(control.MaxRatePerSec),
		workAt(10), &countingSleeper{err: context.Canceled}, f, nil, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

// An idle consumer — a poll that returned nothing — must still notice shutdown
// rather than spinning on empty fetches forever.
func TestConsumeLoopNoticesShutdownWhileIdle(t *testing.T) {
	f := &fakeFetcher{batches: []int{0, 0, 0}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec),
		workAt(0), &countingSleeper{}, f, nil, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	// An empty batch must not commit — there is no progress to record.
	if f.commits.Load() != 0 {
		t.Fatalf("an empty fetch committed %d times", f.commits.Load())
	}
}

func TestConsumeLoopStopsPromptlyOnCancellation(t *testing.T) {
	f := &fakeFetcher{batches: []int{1_000_000}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec),
			workAt(0), &countingSleeper{}, f, nil, nil, nil)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop outran its own cancellation")
	}
}

func TestConsumeLoopToleratesNilCallbacks(t *testing.T) {
	f := &fakeFetcher{batches: []int{2}, fetchErr: []error{nil, errFake}}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec),
		workAt(0), &countingSleeper{}, f, nil, nil, nil)
}

func TestNopApplierApplies(t *testing.T) {
	nopApplier{}.Apply(Record{DedupeKey: "a"}) // must not panic
}

func TestIsContextError(t *testing.T) {
	if !isContextError(context.Canceled) || !isContextError(context.DeadlineExceeded) {
		t.Fatal("both context errors must be recognised")
	}
	if isContextError(errFake) {
		t.Fatal("a broker error is not a context error")
	}
}

func waitForCount(s *countingSleeper, n int) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(s.snapshot()) >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// An empty fetch is a poll that timed out with a LIVE context: the loop must
// go round again rather than committing nothing or treating it as shutdown.
func TestConsumeLoopContinuesAfterAnEmptyFetch(t *testing.T) {
	f := &fakeFetcher{batches: []int{0, 3}}
	a := &recordSink{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, a, nil, nil)

	if a.n.Load() != 3 {
		t.Fatalf("applied %d; an empty fetch stopped the loop", a.n.Load())
	}
	if f.commits.Load() != 1 {
		t.Fatalf("commits: got %d want 1 — the empty batch must not commit", f.commits.Load())
	}
}

// ── the fault window ───────────────────────────────────────────────────────

// A REWIND REPLACES THE COMMIT. Committing and then rewinding would move the
// group's offset past records the loop is about to reprocess, so a restart
// mid-experiment would skip them — the opposite failure to the one being
// modelled.
func TestAFaultRewindsInsteadOfCommitting(t *testing.T) {
	batch := []Record{
		{DedupeKey: "k1", Topic: "events", Partition: 0, Offset: 10, LeaderEpoch: 4},
		{DedupeKey: "k2", Topic: "events", Partition: 0, Offset: 11, LeaderEpoch: 4},
	}
	f := &fakeFetcher{records: [][]Record{batch}}
	a := &recordSink{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, a, faultOn("k1"), nil)

	if f.commits.Load() != 0 {
		t.Fatalf("the faulted batch committed %d times; a rewind must replace the commit", f.commits.Load())
	}
	rewinds := f.rewindsSnapshot()
	if len(rewinds) != 1 {
		t.Fatalf("got %d rewinds, want 1", len(rewinds))
	}
	if len(rewinds[0]) != 1 || rewinds[0][0].DedupeKey != "k1" {
		t.Fatalf("rewound to %+v, want the record keyed k1", rewinds[0])
	}
}

// EVERY RECORD OF THE BATCH IS APPLIED BEFORE THE REWIND. That ordering is the
// whole point: the duplicate that matters is the one whose effect already ran.
// A rewind that fired before the batch finished applying would model a lost
// message rather than a repeated one.
func TestEveryRecordIsAppliedBeforeAFaultRewinds(t *testing.T) {
	batch := []Record{
		{DedupeKey: "k1", Partition: 0, Offset: 10},
		{DedupeKey: "k2", Partition: 0, Offset: 11},
		{DedupeKey: "k3", Partition: 0, Offset: 12},
	}
	f := &fakeFetcher{records: [][]Record{batch}}
	a := &recordSink{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, a, faultOn("k1"), nil)

	if got := a.n.Load(); got != 3 {
		t.Fatalf("applied %d of 3 before the rewind", got)
	}
}

// THE REWIND TARGET CARRIES THE EPOCH. Defaulting it to -1 waives the broker's
// truncation check, which turns data loss into a silent resume at the wrong
// place.
func TestARewindTargetCarriesTopicOffsetAndEpoch(t *testing.T) {
	batch := []Record{{DedupeKey: "k1", Topic: "events", Partition: 2, Offset: 77, LeaderEpoch: 9}}
	f := &fakeFetcher{records: [][]Record{batch}}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, nil, faultOn("k1"), nil)

	rewinds := f.rewindsSnapshot()
	if len(rewinds) != 1 || len(rewinds[0]) != 1 {
		t.Fatalf("got rewinds %+v", rewinds)
	}
	got := rewinds[0][0]
	if got.Topic != "events" || got.Partition != 2 || got.Offset != 77 || got.LeaderEpoch != 9 {
		t.Fatalf("rewind target %+v lost part of its address", got)
	}
}

// A batch that faults nothing commits exactly as before. The fault path must be
// invisible when the rate is zero, which is the lab's default.
func TestABatchThatFaultsNothingCommitsNormally(t *testing.T) {
	f := &fakeFetcher{batches: []int{3}}
	a := &recordSink{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, a, faultOn("nothing-matches-this"), nil)

	if f.commits.Load() != 1 {
		t.Fatalf("committed %d times, want 1", f.commits.Load())
	}
	if len(f.rewindsSnapshot()) != 0 {
		t.Fatal("a batch with no fault rewound")
	}
}

// A nil Faulter is the ordinary lab: no rewinds, commits as before.
func TestANilFaulterNeverRewinds(t *testing.T) {
	f := &fakeFetcher{batches: []int{2, 2}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, nil, nil, nil)

	if len(f.rewindsSnapshot()) != 0 {
		t.Fatal("a nil faulter rewound")
	}
	if f.commits.Load() != 2 {
		t.Fatalf("committed %d times, want 2", f.commits.Load())
	}
}

func TestNopFaulterTargetsNothing(t *testing.T) {
	if got := (nopFaulter{}).Targets([]Record{{DedupeKey: "a"}}); got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
}

// AN EMPTY BATCH NEVER REACHES THE FAULT WINDOW. It has no records to apply, so
// there is nothing whose effect could have run before the rewind — and a rewind
// on an idle poll would move the cursor for no reason.
func TestAnEmptyBatchIsNotOfferedToTheFaulter(t *testing.T) {
	f := &fakeFetcher{batches: []int{0, 0, 1}}
	faulter := faultOn("b2-r0")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, nil, faulter, nil)

	if got := faulter.batches.Load(); got != 1 {
		t.Fatalf("the faulter saw %d batches; the two empty polls must not reach it", got)
	}
}

// ONE REWIND PER PARTITION, at the EARLIEST faulting offset. A rewind to a later
// offset would leave an earlier faulted record committed, so a fault would be
// logged whose redelivery never arrives.
func TestARewindCoversEachPartitionAtItsEarliestFault(t *testing.T) {
	batch := []Record{
		{DedupeKey: "p0-late", Topic: "events", Partition: 0, Offset: 30},
		{DedupeKey: "p0-early", Topic: "events", Partition: 0, Offset: 10},
		{DedupeKey: "p1-only", Topic: "events", Partition: 1, Offset: 20},
	}
	f := &fakeFetcher{records: [][]Record{batch}}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0),
		&countingSleeper{}, f, nil, faultOn("p0-late", "p0-early", "p1-only"), nil)

	rewinds := f.rewindsSnapshot()
	if len(rewinds) != 1 {
		t.Fatalf("got %d rewinds, want 1", len(rewinds))
	}
	byPartition := map[int32]Record{}
	for _, r := range rewinds[0] {
		byPartition[r.Partition] = r
	}
	if len(byPartition) != 2 {
		t.Fatalf("rewound %d partitions, want 2", len(byPartition))
	}
	if got := byPartition[0].Offset; got != 10 {
		t.Fatalf("partition 0 rewound to offset %d, want the earliest fault at 10", got)
	}
	if got := byPartition[1].Offset; got != 20 {
		t.Fatalf("partition 1 rewound to offset %d, want 20", got)
	}
}
