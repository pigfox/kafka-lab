// Package ratelimit wraps golang.org/x/time/rate with a limit that can be
// changed while a caller is blocked in Wait.
//
// WHY A WRAPPER AT ALL. rate.Limiter already has SetLimit, and it is already
// safe for concurrent use — so this type looks like a wrapper that earns
// nothing. It earns one specific thing: SetLimit does NOT wake a goroutine
// already parked inside Wait. Drag the consumer slider from 1/s to 200/s and
// the consumer sits out the remainder of its one-second reservation before the
// new rate has any effect, which on this demo is exactly the moment the user is
// staring at the lag panel waiting for the drain to start.
//
// The fix is a generation channel: SetRate swaps the underlying limiter and
// closes the channel the parked Wait is selecting on, so the parked call
// returns immediately and re-enters against the new limit.
package ratelimit

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// Limiter is a token bucket whose rate can be replaced live.
type Limiter struct {
	mu      sync.Mutex
	lim     *rate.Limiter
	rps     float64
	changed chan struct{}
}

// New returns a limiter admitting rps events per second. The burst is one, so
// a paused-then-resumed limiter cannot discharge a backlog of saved-up tokens
// in a single spike — on a rate demo, a burst is a lie in the graph.
func New(rps float64) *Limiter {
	return &Limiter{
		lim:     rate.NewLimiter(rate.Limit(rps), 1),
		rps:     rps,
		changed: make(chan struct{}),
	}
}

// Rate reports the current limit in events per second.
func (l *Limiter) Rate() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rps
}

// SetRate replaces the limit and wakes any caller currently parked in Wait.
// Setting the rate it already has is a no-op, so a control record that merely
// restates the current state does not interrupt the pipeline.
func (l *Limiter) SetRate(rps float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rps == l.rps {
		return
	}
	l.rps = rps
	l.lim = rate.NewLimiter(rate.Limit(rps), 1)
	close(l.changed)
	l.changed = make(chan struct{})
}

// snapshot returns the limiter and the wake channel under one lock, so a
// SetRate landing between the two reads cannot hand back a limiter paired with
// an already-closed channel.
func (l *Limiter) snapshot() (*rate.Limiter, chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lim, l.changed
}

// Wait blocks until the limiter admits one event, ctx is done, or the rate
// changes underneath it. A rate change returns nil: the caller has not been
// admitted, but it has not failed either, so it loops and waits again against
// the new limit.
func (l *Limiter) Wait(ctx context.Context) error {
	lim, changed := l.snapshot()

	// A cancelled context must lose the race deterministically, ahead of an
	// available token; otherwise a shutdown can be outrun by a fast limiter
	// and the loop never exits.
	if err := ctx.Err(); err != nil {
		return err
	}

	res := lim.Reserve()
	if !res.OK() {
		// Unreachable with burst 1 and a finite rate, but a reservation the
		// limiter refuses outright must not be reported as admission.
		return context.Canceled
	}
	delay := res.Delay()
	if delay <= 0 {
		return nil
	}

	timer := newTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		res.Cancel()
		return ctx.Err()
	case <-changed:
		res.Cancel()
		return nil
	case <-timer.C():
		return nil
	}
}
