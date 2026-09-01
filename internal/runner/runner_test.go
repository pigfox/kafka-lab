package runner

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/ratelimit"
)

var errFake = errors.New("transient broker trouble")

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// ── SignalContext ───────────────────────────────────────────────────────────

func TestSignalContextIsLiveUntilCancelled(t *testing.T) {
	ctx, cancel := SignalContext()
	if err := ctx.Err(); err != nil {
		t.Fatalf("a fresh signal context must be live: %v", err)
	}
	cancel()
	<-ctx.Done()
}

// ── ApplySettings ───────────────────────────────────────────────────────────

type fakeFeed struct {
	mu      sync.Mutex
	s       control.Settings
	changed chan struct{}
}

func newFakeFeed(s control.Settings) *fakeFeed {
	return &fakeFeed{s: s, changed: make(chan struct{})}
}

func (f *fakeFeed) Settings() (control.Settings, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.s, true
}

func (f *fakeFeed) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

func (f *fakeFeed) publish(s control.Settings) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.s = s
	close(f.changed)
	f.changed = make(chan struct{})
}

type recordingApplier struct {
	mu  sync.Mutex
	got []control.Settings
}

func (r *recordingApplier) Apply(s control.Settings) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, s)
}

func (r *recordingApplier) snapshot() []control.Settings {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]control.Settings(nil), r.got...)
}

// A SERVICE THAT STARTED AFTER THE SETTINGS RECORD WAS WRITTEN has already
// missed the change event. A loop that only reacted to future events would run
// on defaults forever, which is the bug this ordering prevents.
func TestApplySettingsAppliesTheCurrentValueBeforeWaiting(t *testing.T) {
	feed := newFakeFeed(control.Settings{ProducerRatePerSec: 42})
	applier := &recordingApplier{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ApplySettings(ctx, feed, applier) }()

	waitFor(t, func() bool { return len(applier.snapshot()) >= 1 })
	if got := applier.snapshot()[0].ProducerRatePerSec; got != 42 {
		t.Fatalf("first apply: got %v want 42", got)
	}

	cancel()
	<-done
}

func TestApplySettingsAppliesEveryChange(t *testing.T) {
	feed := newFakeFeed(control.Settings{ProducerRatePerSec: 1})
	applier := &recordingApplier{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ApplySettings(ctx, feed, applier) }()

	waitFor(t, func() bool { return len(applier.snapshot()) >= 1 })
	feed.publish(control.Settings{ProducerRatePerSec: 2})
	waitFor(t, func() bool { return len(applier.snapshot()) >= 2 })
	feed.publish(control.Settings{ProducerRatePerSec: 3})
	waitFor(t, func() bool { return len(applier.snapshot()) >= 3 })

	got := applier.snapshot()
	for i, want := range []float64{1, 2, 3} {
		if got[i].ProducerRatePerSec != want {
			t.Fatalf("apply %d: got %v want %v", i, got[i].ProducerRatePerSec, want)
		}
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

// ── ProduceLoop ─────────────────────────────────────────────────────────────

type fakeEmitter struct {
	n   atomic.Int64
	err error
}

func (f *fakeEmitter) Emit(context.Context) error {
	f.n.Add(1)
	return f.err
}

func TestProduceLoopEmitsUnderTheLimit(t *testing.T) {
	e := &fakeEmitter{}
	lim := ratelimit.New(control.MaxRatePerSec)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := ProduceLoop(ctx, quiet(), lim, e, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if e.n.Load() == 0 {
		t.Fatal("the loop emitted nothing")
	}
}

// A BROKER THAT IS BRIEFLY UNAVAILABLE must not kill the producer, or the
// demo's recovery story becomes "and then you restart the container yourself".
func TestProduceLoopSurvivesEmitFailures(t *testing.T) {
	e := &fakeEmitter{err: errFake}
	lim := ratelimit.New(control.MaxRatePerSec)

	var reported atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := ProduceLoop(ctx, quiet(), lim, e, func(error) { reported.Add(1) })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if e.n.Load() < 2 {
		t.Fatalf("the loop gave up after %d emits", e.n.Load())
	}
	if reported.Load() != e.n.Load() {
		t.Fatalf("reported %d errors for %d failed emits", reported.Load(), e.n.Load())
	}
}

// A context error from the emitter is shutdown, not a transient fault.
func TestProduceLoopStopsOnAContextErrorFromTheEmitter(t *testing.T) {
	e := &fakeEmitter{err: context.Canceled}
	lim := ratelimit.New(control.MaxRatePerSec)

	done := make(chan error, 1)
	go func() { done <- ProduceLoop(context.Background(), quiet(), lim, e, nil) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the loop treated a context error as transient")
	}
}

func TestProduceLoopToleratesANilErrorCallback(t *testing.T) {
	e := &fakeEmitter{err: errFake}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = ProduceLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), e, nil)
}

// A cancelled context must win against a limiter fast enough to always have a
// token ready, or shutdown never happens.
func TestProduceLoopStopsPromptlyOnCancellation(t *testing.T) {
	e := &fakeEmitter{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ProduceLoop(ctx, quiet(), ratelimit.New(control.MaxRatePerSec), e, nil) }()

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

// ── RealSleeper ─────────────────────────────────────────────────────────────

func TestRealSleeperSleeps(t *testing.T) {
	start := time.Now()
	if err := (RealSleeper{}).Sleep(context.Background(), 30*time.Millisecond); err != nil {
		t.Fatalf("sleep: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("returned after %v", elapsed)
	}
}

func TestRealSleeperReturnsEarlyOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := (RealSleeper{}).Sleep(ctx, 5*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("slept %v past cancellation", elapsed)
	}
}

func TestRealSleeperOfZeroIsImmediate(t *testing.T) {
	if err := (RealSleeper{}).Sleep(context.Background(), 0); err != nil {
		t.Fatalf("got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A zero sleep still reports an already-cancelled context, so a consumer
	// with work set to zero does not spin through shutdown.
	if err := (RealSleeper{}).Sleep(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition never became true")
}

// WAIT CAN RETURN NIL AT THE SAME INSTANT THE CONTEXT IS CANCELLED — its select
// has both the rate-change channel and ctx.Done() ready, and Go picks among
// ready cases at random. Racing into that interleaving would be a flaky test,
// so the Waiter seam drives it directly: this limiter reports admission for a
// context that is already dead, and the loop must stop rather than emit.
type admittingWaiter struct{ cancel context.CancelFunc }

func (a *admittingWaiter) Wait(context.Context) error {
	a.cancel()
	return nil
}

func TestProduceLoopRechecksTheContextAfterWaitAdmits(t *testing.T) {
	e := &fakeEmitter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := ProduceLoop(ctx, quiet(), &admittingWaiter{cancel: cancel}, e, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if e.n.Load() != 0 {
		t.Fatalf("the loop emitted %d records after cancellation", e.n.Load())
	}
}

// The real limiter must satisfy the seam, or the interface is decorative.
var _ Waiter = (*ratelimit.Limiter)(nil)
