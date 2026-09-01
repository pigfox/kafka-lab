package runner

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/pigfox/kafka-lab/internal/ratelimit"
)

// Fetcher hands the consume loop one batch of records at a time.
type Fetcher interface {
	// Fetch blocks until records are available or ctx is done. A returned
	// error other than a context error is transient and the loop retries.
	Fetch(ctx context.Context) (int, error)
	// Commit records progress. It is what makes lag fall.
	Commit(ctx context.Context) error
}

// ConsumeLoop is the throttle-and-drain half of the demo.
//
// THE ORDER HERE IS THE WHOLE STORY, and it is worth spelling out because a
// plausible-looking rearrangement breaks the thing the lab exists to show:
//
//  1. Fetch a batch. Kafka hands over as many records as it has.
//  2. For EACH record, wait on the rate limiter, then sleep the simulated work.
//  3. Commit once the batch is done.
//
// The per-RECORD throttle in step 2 is what makes lag build. Throttling per
// BATCH instead would produce a consumer whose achieved rate depends on batch
// size — drag the slider to 1/s and it would still drain hundreds of records a
// second, because one "unit" is a whole batch. The lag panel would then be flat
// while the slider said starved, which is precisely the lie this design is
// arranged to avoid.
//
// Committing after the batch rather than per record is the ordinary trade:
// per-record commits are a round trip each and would themselves become the
// bottleneck, making the measured consume rate a measurement of the commit path
// instead of the throttle.
func ConsumeLoop(
	ctx context.Context,
	log *slog.Logger,
	lim *ratelimit.Limiter,
	work *atomic.Int64,
	sleeper Sleeper,
	f Fetcher,
	onConsumed func(),
	onError func(error),
) error {
	if onConsumed == nil {
		onConsumed = func() {}
	}
	if onError == nil {
		onError = func(error) {}
	}

	for {
		n, err := f.Fetch(ctx)
		if err != nil {
			if isContextError(err) {
				return err
			}
			onError(err)
			log.Warn("fetch failed", "error", err)
			continue
		}

		for i := 0; i < n; i++ {
			if err := lim.Wait(ctx); err != nil {
				return err
			}
			// The work delay is read PER RECORD from the atomic, so dragging
			// the work slider takes effect on the very next message rather
			// than at the end of a batch that may be thousands long.
			d := time.Duration(work.Load()) * time.Millisecond
			if d > 0 {
				if err := sleeper.Sleep(ctx, d); err != nil {
					return err
				}
			}
			onConsumed()
		}

		if n == 0 {
			// Nothing was fetched — a poll that timed out. Yield to the
			// context so an idle consumer still notices shutdown.
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}

		if err := f.Commit(ctx); err != nil {
			if isContextError(err) {
				return err
			}
			onError(err)
			log.Warn("commit failed", "error", err)
		}
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
