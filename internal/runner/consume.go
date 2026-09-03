package runner

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/pigfox/kafka-lab/internal/ratelimit"
)

// Record is one message as the consume loop sees it.
//
// NO kgo TYPE APPEARS IN THIS PACKAGE, and that is the boundary the whole
// design rests on: internal/kafkabus is the only place that knows what a
// franz-go client is, and the loops here are driven by fakes in microseconds
// with no broker anywhere. A *kgo.Record reaching this file would drag the
// library into every test that touches the consume loop.
//
// DedupeKey is the message's identity, read from the header named by
// event.DedupeHeader. It is EMPTY when the record carried no such header, and
// the loop does not invent one — a synthesised identity would be unique per
// delivery, so a redelivery would carry a different key and the idempotency
// store would report it as first-seen. An empty key is a record that cannot be
// deduplicated, and saying so is the only honest answer.
// Topic, Partition, Offset and LeaderEpoch are what a rewind needs: a cursor
// seek is addressed by topic and partition, and the LEADER EPOCH is carried
// rather than defaulted to -1 because it is what lets the broker detect that
// the offset being sought belongs to a log that has since been truncated. A -1
// disables that check, which would turn a data-loss condition into a silent
// resume at the wrong place.
type Record struct {
	DedupeKey   string
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
}

// RecordApplier performs one record's effect.
//
// It replaced a bare `func()` counter callback. A counter can only say how many
// records went past; it cannot say whether the SAME record went past twice,
// which is the only question a delivery-semantics demo is about.
type RecordApplier interface {
	Apply(r Record)
}

// Fetcher hands the consume loop one batch of records at a time.
type Fetcher interface {
	// Fetch blocks until records are available or ctx is done. A returned
	// error other than a context error is transient and the loop retries.
	Fetch(ctx context.Context) ([]Record, error)
	// Commit records progress. It is what makes lag fall.
	Commit(ctx context.Context) error
	// Rewind moves the consume cursor back to each given record, so those
	// records and everything after them in their partitions are delivered
	// again. It takes no context and returns no error because the underlying
	// seek is a local re-assignment rather than a broker call.
	Rewind(to []Record)
}

// Faulter decides whether a batch's commit is replaced by a rewind.
//
// It is an interface here, rather than internal/fault imported directly, for
// the reason every other seam in this package exists: the loop's behaviour
// under a fault is worth testing, and a test that had to compute sha256 digests
// to find a key that faults would be testing the injector rather than the loop.
type Faulter interface {
	// Targets returns the records to rewind to, or nil to commit normally.
	Targets(recs []Record) []Record
}

// ConsumeLoop is the throttle-and-drain half of the demo.
//
// THE ORDER HERE IS THE WHOLE STORY, and it is worth spelling out because a
// plausible-looking rearrangement breaks the thing the lab exists to show:
//
//  1. Fetch a batch. Kafka hands over as many records as it has.
//  2. For EACH record, wait on the rate limiter, sleep the simulated work, then
//     apply the record.
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
//
// APPLY HAPPENS BEFORE COMMIT, AND THAT ORDER IS THE AT-LEAST-ONCE CONTRACT.
// Anything that stops the process between step 2 and step 3 — a crash, a
// SIGKILL, a rebalance — leaves the effect applied and the offset unrecorded,
// so the next member to own the partition reads those records again. Committing
// FIRST would swap the failure for a worse one: the offset would move past work
// that never happened, and the messages would be gone rather than repeated.
// At-least-once is the choice, and the duplicate is its cost.
func ConsumeLoop(
	ctx context.Context,
	log *slog.Logger,
	lim *ratelimit.Limiter,
	work *atomic.Int64,
	sleeper Sleeper,
	f Fetcher,
	applier RecordApplier,
	faulter Faulter,
	onError func(error),
) error {
	if applier == nil {
		applier = nopApplier{}
	}
	if faulter == nil {
		faulter = nopFaulter{}
	}
	if onError == nil {
		onError = func(error) {}
	}

	for {
		recs, err := f.Fetch(ctx)
		if err != nil {
			if isContextError(err) {
				return err
			}
			onError(err)
			log.Warn("fetch failed", "error", err)
			continue
		}

		for _, rec := range recs {
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
			applier.Apply(rec)
		}

		if len(recs) == 0 {
			// Nothing was fetched — a poll that timed out. Yield to the
			// context so an idle consumer still notices shutdown.
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}

		// ── THE FAULT WINDOW ────────────────────────────────────────────
		//
		// This is the ONLY point in the loop where a rewind may happen, and
		// its position is the contract. Every record of the batch has been
		// applied by now, and no commit has been issued yet, so a rewind here
		// leaves exactly the state a crash between apply and commit leaves:
		// the effect ran, the offset did not move.
		//
		// It is outside the poll and it is not concurrent with a commit —
		// franz-go's SetOffsets documentation asks for both, warning that it
		// is "strongly recommended to use this function outside of the context
		// of a PollFetches loop ... and to not use this concurrent with
		// committing". Fetch, apply, this, and Commit are four sequential
		// steps of one goroutine, so neither can overlap.
		//
		// A rewind REPLACES the commit rather than preceding it. Committing
		// and then rewinding would move the group's offset past records the
		// loop is about to process again, so a restart mid-experiment would
		// skip them entirely — the opposite failure to the one being modelled.
		if targets := faulter.Targets(recs); len(targets) > 0 {
			for _, t := range targets {
				log.Info("fault injected: rewinding instead of committing",
					"key", t.DedupeKey,
					"partition", t.Partition,
					"offset", t.Offset,
					"epoch", t.LeaderEpoch)
			}
			f.Rewind(targets)
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

// nopFaulter never faults, which is the lab's ordinary behaviour.
type nopFaulter struct{}

// Targets always returns nil.
func (nopFaulter) Targets([]Record) []Record { return nil }

// nopApplier lets a caller run the loop for its throttling behaviour alone,
// without a nil check on the hot path.
type nopApplier struct{}

// Apply does nothing.
func (nopApplier) Apply(Record) {}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
