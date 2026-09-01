package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewAndRate(t *testing.T) {
	l := New(12.5)
	if got := l.Rate(); got != 12.5 {
		t.Fatalf("got %v want 12.5", got)
	}
}

func TestSetRate(t *testing.T) {
	l := New(10)
	l.SetRate(99)
	if got := l.Rate(); got != 99 {
		t.Fatalf("got %v want 99", got)
	}
}

// Setting the rate it already has must not close the wake channel, or a
// control record restating current state would interrupt the pipeline.
func TestSetRateToSameValueDoesNotWake(t *testing.T) {
	l := New(10)
	_, before := l.snapshot()
	l.SetRate(10)
	_, after := l.snapshot()
	if before != after {
		t.Fatal("no-op SetRate replaced the wake channel")
	}
	select {
	case <-before:
		t.Fatal("no-op SetRate closed the wake channel")
	default:
	}
}

func TestSetRateWakesTheChannel(t *testing.T) {
	l := New(10)
	_, before := l.snapshot()
	l.SetRate(20)
	select {
	case <-before:
	default:
		t.Fatal("SetRate must close the channel a parked Wait is selecting on")
	}
	_, after := l.snapshot()
	if before == after {
		t.Fatal("SetRate must install a fresh wake channel")
	}
}

// The first token of a burst-1 limiter is available immediately, so Wait
// returns without consulting a timer at all.
func TestWaitReturnsImmediatelyForTheFirstToken(t *testing.T) {
	l := New(1)
	if err := l.Wait(context.Background()); err != nil {
		t.Fatalf("first token: %v", err)
	}
}

func TestWaitHonoursTheTimer(t *testing.T) {
	l := New(1000)
	if err := l.Wait(context.Background()); err != nil {
		t.Fatalf("first: %v", err)
	}
	start := time.Now()
	if err := l.Wait(context.Background()); err != nil {
		t.Fatalf("second: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Microsecond {
		t.Fatalf("second token returned in %v; the limiter did not throttle", elapsed)
	}
}

// A cancelled context must lose the race DETERMINISTICALLY, ahead of a token
// that is already available — otherwise a shutdown can be outrun by a fast
// limiter and the produce loop never exits.
func TestWaitReturnsContextErrorEvenWhenATokenIsFree(t *testing.T) {
	l := New(MaxRateForTest)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := l.Wait(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v want context.Canceled", err)
	}
}

func TestWaitReturnsWhenContextIsCancelledMidWait(t *testing.T) {
	l := New(1) // one per second: the second token is a full second away
	if err := l.Wait(context.Background()); err != nil {
		t.Fatalf("first: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := l.Wait(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Wait ignored cancellation for %v", elapsed)
	}
}

// THE REASON THIS PACKAGE EXISTS. rate.Limiter.SetLimit does not wake a parked
// Wait; this one must, or dragging the consumer slider open does nothing until
// the old reservation expires.
func TestWaitReturnsEarlyWhenTheRateChanges(t *testing.T) {
	l := New(1) // second token is a second away
	if err := l.Wait(context.Background()); err != nil {
		t.Fatalf("first: %v", err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		l.SetRate(500)
	}()
	start := time.Now()
	if err := l.Wait(context.Background()); err != nil {
		t.Fatalf("after rate change: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("a rate change did not wake the parked Wait: blocked %v", elapsed)
	}
}

// A reservation the limiter refuses outright must be reported as a failure to
// be admitted, never as admission.
func TestWaitReportsARefusedReservation(t *testing.T) {
	l := New(1)
	// A zero-burst limiter can never satisfy a one-event reservation, which
	// is the only way Reserve reports !OK. New never builds one; this reaches
	// past it on purpose to pin the branch's behaviour.
	l.lim = newZeroBurstLimiter()
	if err := l.Wait(context.Background()); err == nil {
		t.Fatal("a refused reservation must not be reported as admission")
	}
}

func TestNewTimerIsARealTimer(t *testing.T) {
	tm := newTimer(time.Millisecond)
	defer tm.Stop()
	select {
	case <-tm.C():
	case <-time.After(time.Second):
		t.Fatal("the production timer never fired")
	}
}

func TestRealTimerStopIsSafe(t *testing.T) {
	tm := newTimer(time.Hour)
	tm.Stop()
	select {
	case <-tm.C():
		t.Fatal("a stopped timer fired")
	case <-time.After(10 * time.Millisecond):
	}
}
