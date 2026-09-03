// Package fault makes at-least-once VISIBLE by producing the duplicate that
// matters: a record whose effect was applied and whose offset was then not
// committed.
//
// ── WHAT IT DOES, AND WHY THE OBVIOUS VERSION DOES NOTHING
//
// The obvious injector skips the commit for a batch and carries on. It produces
// NO duplicate at all, and produces none silently. franz-go keeps the consume
// cursor in memory and advances it on every poll; the committed offset is read
// back exactly once per partition assignment, in groupConsumer.fetchOffsets,
// "issued once we join a group to see what the prior commits were". So a
// skipped commit changes only what a FUTURE assignment would resume from. The
// loop polls forward and never sees those records again — and a harness built
// that way reports zero duplicates while looking like it works.
//
// So the fault SEEKS THE CURSOR BACKWARDS, with Client.SetOffsets, to the
// offset of the record that faulted. That is an in-process redelivery to the
// same consumer instance: no rebalance, no restart, no wall-clock wait.
//
// ── A REWIND REDELIVERS A TAIL, NOT A RECORD
//
// SetOffsets moves a PARTITION cursor, so rewinding to a faulted record's
// offset redelivers that record and every later record of that partition the
// loop had already polled. Duplicates therefore EXCEED injected faults, and the
// multiplier is whatever the broker put in that batch. That is not noise and it
// is not a defect in this package — it is what a crash between apply and commit
// actually does, and a mechanism tuned to redeliver exactly one record per fault
// would buy a rounder number by modelling something that does not happen.
//
// ── THE ONCE-PER-KEY GUARD IS LOAD-BEARING
//
// The fault decision is a pure function of the seed and the key, which is what
// makes the two arms comparable — same records, same faults, whatever the
// delivery order. It also means a faulted record faults AGAIN when the rewind
// redelivers it: rewind, redeliver, rewind, forever, with the consumer pinned on
// one offset and lag climbing without bound. A key that has faulted therefore
// never faults again for the life of the process. The guard is not hardening,
// it is what stops the lab livelocking on its own first fault.
package fault

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sync"

	"github.com/pigfox/kafka-lab/internal/runner"
)

// Injector decides which records fault, and remembers which already have.
type Injector struct {
	rate float64
	seed string

	mu    sync.Mutex
	fired map[string]struct{}
}

// New returns an injector firing on approximately rate of all distinct keys.
//
// A rate of zero — the default everywhere — fires nothing, so the lab's
// ordinary behaviour is unchanged until someone asks for a fault. A rate of one
// or more fires on every distinct key exactly once.
func New(rate float64, seed string) *Injector {
	return &Injector{rate: rate, seed: seed, fired: make(map[string]struct{})}
}

// Rate reports the configured fault rate.
func (i *Injector) Rate() float64 { return i.rate }

// Fired reports how many distinct keys have faulted.
func (i *Injector) Fired() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.fired)
}

// eligible reports whether key is in the fault set for this seed and rate.
//
// IT IS A HASH, NOT A DRAW FROM AN RNG STREAM, and the difference is the whole
// reason the two arms can be compared. A stream's answer depends on how many
// times it has been asked, so batch boundaries, partition assignment and
// delivery order would all move the fault set — and the dedupe arm and the
// at-least-once arm would be fed different faults, which is a comparison of two
// different experiments. sha256(seed ‖ key) depends on nothing but its inputs.
func (i *Injector) eligible(key string) bool {
	// Both bounds are handled before the arithmetic. Zero is the default and
	// must fire nothing; one must fire on everything, and the float comparison
	// below cannot promise that, because float64(math.MaxUint64) rounds UP to
	// 2^64 and would leave the single largest digest just short of its own
	// threshold.
	if i.rate <= 0 {
		return false
	}
	if i.rate >= 1 {
		return true
	}
	sum := sha256.Sum256([]byte(i.seed + "\x00" + key))
	u := binary.BigEndian.Uint64(sum[:8])
	return float64(u) < i.rate*math.Ldexp(1, 64)
}

// Fault reports whether this record should fault now, recording the key so it
// never faults twice.
//
// Testing and recording happen under one lock for the same reason idem.Observe
// does it: a check-then-record pair would let a redelivered record slip between
// the two and fault a second time, which is the livelock this guard exists to
// prevent.
func (i *Injector) Fault(r runner.Record) bool {
	if r.DedupeKey == "" {
		// A record with no identity cannot be tracked, so it cannot be given
		// the once-only guarantee, so it is never faulted. Faulting it would
		// rewind to it forever.
		return false
	}
	if !i.eligible(r.DedupeKey) {
		return false
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if _, already := i.fired[r.DedupeKey]; already {
		return false
	}
	i.fired[r.DedupeKey] = struct{}{}
	return true
}

// Targets picks the records to rewind to for one batch: the EARLIEST faulting
// record in each partition.
//
// Earliest rather than latest, because a rewind to a later offset would leave
// an earlier faulted record committed — the loop would move past a record whose
// fault never produced its redelivery, and the injection log would then name
// faults the metrics cannot account for.
//
// It returns nil when nothing faulted, which is the signal to commit normally.
func (i *Injector) Targets(recs []runner.Record) []runner.Record {
	var targets map[int32]runner.Record
	for _, r := range recs {
		if !i.Fault(r) {
			continue
		}
		if targets == nil {
			targets = make(map[int32]runner.Record, 1)
		}
		if prior, ok := targets[r.Partition]; !ok || r.Offset < prior.Offset {
			targets[r.Partition] = r
		}
	}
	if targets == nil {
		return nil
	}
	out := make([]runner.Record, 0, len(targets))
	for _, r := range targets {
		out = append(out, r)
	}
	return out
}
