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
		&countingSleeper{}, f, a, nil)
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
		&countingSleeper{}, f, a, nil)

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
		&countingSleeper{}, f, a, nil)

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
	_ = ConsumeLoop(ctx, quiet(), lim, workAt(0), &countingSleeper{}, f, a, nil)
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
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), work, sleeper, f, nil, nil)

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
	_ = ConsumeLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), workAt(0), sleeper, f, nil, nil)

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
		&countingSleeper{}, f, a, func(error) { reported.Add(1) })

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
		&countingSleeper{}, f, nil, func(error) { reported.Add(1) })

	if reported.Load() < 2 {
		t.Fatalf("reported %d commit errors; the loop stopped early", reported.Load())
	}
}

func TestConsumeLoopStopsOnAContextErrorFromFetch(t *testing.T) {
	f := &fakeFetcher{batches: []int{0}, fetchErr: []error{context.Canceled}}
	err := ConsumeLoop(context.Background(), quiet(), ratelimit.New(control.MaxRatePerSec),
		workAt(0), &countingSleeper{}, f, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestConsumeLoopStopsOnAContextErrorFromCommit(t *testing.T) {
	f := &fakeFetcher{batches: []int{1}, commitEr: context.Canceled}
	err := ConsumeLoop(context.Background(), quiet(), ratelimit.New(control.MaxRatePerSec),
		workAt(0), &countingSleeper{}, f, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestConsumeLoopStopsWhenTheSleeperIsInterrupted(t *testing.T) {
	f := &fakeFetcher{batches: []int{5}}
	err := ConsumeLoop(context.Background(), quiet(), ratelimit.New(control.MaxRatePerSec),
		workAt(10), &countingSleeper{err: context.Canceled}, f, nil, nil)
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
		workAt(0), &countingSleeper{}, f, nil, nil)
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
			workAt(0), &countingSleeper{}, f, nil, nil)
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
		workAt(0), &countingSleeper{}, f, nil, nil)
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
		&countingSleeper{}, f, a, nil)

	if a.n.Load() != 3 {
		t.Fatalf("applied %d; an empty fetch stopped the loop", a.n.Load())
	}
	if f.commits.Load() != 1 {
		t.Fatalf("commits: got %d want 1 — the empty batch must not commit", f.commits.Load())
	}
}
