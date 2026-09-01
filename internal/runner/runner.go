// Package runner holds the pieces of each binary's main loop that are worth
// testing: the produce loop, the consume loop, and process shutdown.
//
// WHY THIS PACKAGE EXISTS AT ALL. A `func main()` cannot be unit tested — it
// takes no arguments, returns nothing, and its failure mode is os.Exit. So the
// binaries in cmd/ are deliberately about twenty lines each: read config, dial,
// wire, call into here. Everything with a decision in it lives on this side of
// the line, where a fake can drive it in microseconds and no broker is needed.
package runner

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pigfox/kafka-lab/internal/control"
)

// SignalContext returns a context cancelled on SIGINT or SIGTERM.
//
// `docker compose down` sends SIGTERM and then SIGKILL fifteen seconds later.
// A service that ignores the first one is a service that takes fifteen seconds
// to stop, every time, and `./stop.sh` then feels broken when it is merely
// being patient.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// Waiter is the rate limit the produce loop obeys. It is an interface, not the
// concrete *ratelimit.Limiter, for ONE reason worth the indirection: Wait can
// legitimately return nil at the same instant the context is cancelled — its
// select has both the rate-change channel and ctx.Done() ready, and Go picks
// among ready cases at random. The loop must therefore re-check the context
// after every Wait, and this seam is what lets a test drive that exact
// interleaving instead of hoping to race into it.
type Waiter interface {
	Wait(ctx context.Context) error
}

// Emitter is what the produce loop writes to.
type Emitter interface {
	Emit(ctx context.Context) error
}

// Applier receives settings whenever the control topic changes.
type Applier interface {
	Apply(s control.Settings)
}

// SettingsFeed is the control watcher, as the loops see it.
type SettingsFeed interface {
	Settings() (control.Settings, bool)
	Changed() <-chan struct{}
}

// ApplySettings watches feed and pushes every change into applier until ctx is
// done. It applies the CURRENT value first, before waiting for anything: a
// service that started after the settings record was written has already missed
// the change event, and a loop that only reacts to future events would run on
// defaults forever.
func ApplySettings(ctx context.Context, feed SettingsFeed, applier Applier) error {
	for {
		changed := feed.Changed()
		s, _ := feed.Settings()
		applier.Apply(s)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// ProduceLoop emits under a rate limit until ctx is done.
//
// AN EMIT FAILURE IS LOGGED AND THE LOOP CONTINUES. A broker that is briefly
// unavailable — a rebalance, a restart from the compose file — must not kill
// the producer, or the demo's recovery story becomes "and then you restart the
// container yourself". The one thing that DOES stop it is the context.
func ProduceLoop(ctx context.Context, log *slog.Logger, lim Waiter, e Emitter, onError func(error)) error {
	if onError == nil {
		onError = func(error) {}
	}
	for {
		if err := lim.Wait(ctx); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.Emit(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			onError(err)
			log.Warn("emit failed", "error", err)
		}
	}
}

// Sleeper is the consumer's simulated work. It is an interface purely so a test
// can assert the delay WITHOUT SPENDING IT — a consumer test that actually
// slept 500ms per message would take longer than the demo.
type Sleeper interface {
	Sleep(ctx context.Context, d time.Duration) error
}

// RealSleeper sleeps for real.
type RealSleeper struct{}

// Sleep waits for d or until ctx is done, whichever comes first.
func (RealSleeper) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
