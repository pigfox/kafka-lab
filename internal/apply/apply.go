// Package apply is the consumer's side effect, with enough bookkeeping that a
// SECOND application of the same record is observable rather than invisible.
//
// WHY A COUNTER WAS NOT ENOUGH. The consume loop used to take a bare `func()`
// that incremented consumed_total. That number answers "how many records went
// past", which is the right question for a throughput panel and the wrong one
// entirely for delivery semantics: a topic delivering every record twice and a
// topic delivering twice as many records produce the same line on that graph.
// So this package keeps a bounded per-key tally of what it has applied, and a
// record whose key it has applied before is counted as a DOUBLE APPLY.
//
// FOUR OUTCOMES, AND EVERY RECORD LANDS IN EXACTLY ONE:
//
//   - APPLIED — the effect ran. This includes both first applications and
//     double applies; a double apply is an apply that also happened before.
//   - SUPPRESSED — dedupe was on and the key had been seen, so the effect did
//     not run. This is the count that should be non-zero on the dedupe arm and
//     zero on the other.
//   - DOUBLE APPLIED — the effect ran for a key it had already run for. This is
//     the defect the dedupe store exists to remove, and it is counted in BOTH
//     arms so the two are comparable.
//   - NO KEY — the record carried no identity header. It is COUNTED AND
//     EXPOSED, and it is still applied. Dropping it would lose data over a
//     missing header, and pretending it was deduplicated would be a lie; the
//     honest answer is that at-least-once still holds for it and nothing here
//     can make it idempotent.
//
// The tallies are bounded, so they inherit every limit in internal/idem: a key
// evicted for capacity or age is a key whose second application is no longer
// recognisable, and the double-apply count is therefore a LOWER BOUND rather
// than a total. Seen exposes the store so a caller can publish the eviction and
// expiry counts alongside it.
package apply

import (
	"sync/atomic"
	"time"

	"github.com/pigfox/kafka-lab/internal/idem"
	"github.com/pigfox/kafka-lab/internal/runner"
)

// DefaultCapacity is how many keys each store holds by default.
const DefaultCapacity = 50_000

// DefaultTTL is how long a key is remembered by default.
//
// It matches the events topic's ten-minute retention on purpose: a record the
// broker has already aged out cannot be redelivered, so remembering its key for
// longer costs memory and buys nothing.
const DefaultTTL = 10 * time.Minute

// Observer is told what happened to each record, so a caller can publish the
// four outcomes without this package importing a metrics library.
//
// EACH METHOD TAKES THE RECORD, not just the fact that it happened. Suppressed
// and DoubleApplied are the two ways a REDELIVERY shows up — suppressed on the
// dedupe arm, applied a second time on the other — so a caller holding the
// record can log the key, partition and offset of every duplicate as it lands.
// That log is the evidence a published duplicate count is checked against, and
// a bare counter cannot produce it.
type Observer interface {
	Applied(r runner.Record)
	Suppressed(r runner.Record)
	DoubleApplied(r runner.Record)
	NoKey(r runner.Record)
}

// Counts is a snapshot of the four outcomes.
type Counts struct {
	Applied       uint64
	Suppressed    uint64
	DoubleApplied uint64
	NoKey         uint64
}

// Options configures an Applier. Every field has a working zero value except
// Effect, whose zero value is a no-op.
type Options struct {
	// Dedupe turns suppression on. It is OFF by default, which is the
	// at-least-once arm.
	Dedupe bool
	// Seen is the dedupe store. Nil builds one at the package defaults.
	Seen *idem.Set
	// AppliedKeys is the per-key apply tally. Nil builds one at the package
	// defaults. It is SEPARATE from Seen because it must keep counting on the
	// arm where Seen is never consulted.
	AppliedKeys *idem.Set
	// Effect is the side effect itself. Nil means none.
	Effect func(runner.Record)
	// Observer receives each outcome. Nil means none.
	Observer Observer
}

// Applier is the consume loop's runner.RecordApplier.
type Applier struct {
	dedupe      bool
	seen        *idem.Set
	appliedKeys *idem.Set
	effect      func(runner.Record)
	obs         Observer

	nApplied    atomic.Uint64
	nSuppressed atomic.Uint64
	nDouble     atomic.Uint64
	nNoKey      atomic.Uint64
}

// New builds an Applier from opts.
func New(opts Options) *Applier {
	a := &Applier{
		dedupe:      opts.Dedupe,
		seen:        opts.Seen,
		appliedKeys: opts.AppliedKeys,
		effect:      opts.Effect,
		obs:         opts.Observer,
	}
	if a.seen == nil {
		a.seen = idem.New(DefaultCapacity, DefaultTTL, nil, nil)
	}
	if a.appliedKeys == nil {
		a.appliedKeys = idem.New(DefaultCapacity, DefaultTTL, nil, nil)
	}
	if a.effect == nil {
		a.effect = func(runner.Record) {}
	}
	if a.obs == nil {
		a.obs = nopObserver{}
	}
	return a
}

// Apply performs one record's effect, or suppresses it as a duplicate.
//
// THE ORDER OF THE THREE BRANCHES IS THE CONTRACT. Identity is checked first,
// because a record with no key cannot be judged a duplicate and must not be
// counted as one. Suppression is checked second, so a suppressed record never
// reaches the apply tally — it was not applied, and recording it there would
// make the dedupe arm report the duplicates it just prevented.
func (a *Applier) Apply(r runner.Record) {
	if r.DedupeKey == "" {
		a.nNoKey.Add(1)
		a.obs.NoKey(r)
		a.run(r)
		return
	}

	if a.dedupe && a.seen.Observe(r.DedupeKey) > 0 {
		a.nSuppressed.Add(1)
		a.obs.Suppressed(r)
		return
	}

	if a.appliedKeys.Observe(r.DedupeKey) > 0 {
		a.nDouble.Add(1)
		a.obs.DoubleApplied(r)
	}
	a.run(r)
}

// run performs the effect and counts it. A double apply is counted here TOO —
// it is an apply that also happened before, not a separate kind of event — so
// Applied stays a true count of how many times the effect ran.
func (a *Applier) run(r runner.Record) {
	a.effect(r)
	a.nApplied.Add(1)
	a.obs.Applied(r)
}

// Counts returns a snapshot of the four outcomes.
func (a *Applier) Counts() Counts {
	return Counts{
		Applied:       a.nApplied.Load(),
		Suppressed:    a.nSuppressed.Load(),
		DoubleApplied: a.nDouble.Load(),
		NoKey:         a.nNoKey.Load(),
	}
}

// Dedupe reports whether suppression is on.
func (a *Applier) Dedupe() bool { return a.dedupe }

// Seen returns the dedupe store, so a caller can publish its eviction and
// expiry counts — the two ways the guarantee is lost while the process runs.
func (a *Applier) Seen() *idem.Set { return a.seen }

// AppliedKeys returns the per-key apply tally. Its evictions bound how far back
// a double apply is still recognisable.
func (a *Applier) AppliedKeys() *idem.Set { return a.appliedKeys }

// nopObserver ignores every outcome.
type nopObserver struct{}

func (nopObserver) Applied(runner.Record)       {}
func (nopObserver) Suppressed(runner.Record)    {}
func (nopObserver) DoubleApplied(runner.Record) {}
func (nopObserver) NoKey(runner.Record)         {}
