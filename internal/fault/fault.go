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
	rate    float64
	seed    string
	onFault func(runner.Record)

	mu    sync.Mutex
	fired map[string]struct{}
}

// New returns an injector firing on approximately rate of all distinct keys.
//
// A rate of zero — the default everywhere — fires nothing, so the lab's
// ordinary behaviour is unchanged until someone asks for a fault. A rate of one
// or more fires on every distinct key exactly once.
//
// onFault is called once for every key that fires, at the moment it fires.
//
// ── WHY THE CALLBACK EXISTS, WHICH A MEASURED RUN DISCOVERED THE HARD WAY
//
// It is tempting to log the REWIND TARGETS instead and call that the fault
// record. That is what the first graded run did, and the log it produced was
// incomplete in a way that looked complete. Targets picks at most ONE record per
// partition per batch, so when two eligible keys land in the same partition of
// the same batch, both are marked fired and only the earlier is returned. The
// later one is spent silently.
//
// The consequence is not a small inaccuracy. The FIRED set is a pure function of
// the seed and the delivered keys, and is therefore identical across two runs of
// the same records. The TARGET set is a subset of it chosen by batch
// composition, which the broker decides and which differs between runs. A
// comparison of two arms' target sets is a comparison of their batching; the
// first graded run's arms shared only 25 of 34 and 30 keys, and the difference
// was entirely this.
func New(rate float64, seed string, onFault func(runner.Record)) *Injector {
	if onFault == nil {
		onFault = func(runner.Record) {}
	}
	return &Injector{rate: rate, seed: seed, onFault: onFault, fired: make(map[string]struct{})}
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
	if _, already := i.fired[r.DedupeKey]; already {
		i.mu.Unlock()
		return false
	}
	i.fired[r.DedupeKey] = struct{}{}
	i.mu.Unlock()

	// Reported OUTSIDE the lock: this is the only call here that reaches
	// arbitrary caller code, and holding the injector's lock across it would
	// make a logging call able to stall every other partition's decision.
	i.onFault(r)
	return true
}

// Targets picks the records to rewind to for one batch: the EARLIEST faulting
// record in each partition.
//
// Earliest rather than latest, because a rewind to a later offset would leave
// an earlier faulted record committed — the loop would move past a record whose
// fault never produced its redelivery.
//
// AT MOST ONE TARGET PER PARTITION, SO THE TARGET SET IS A SUBSET OF THE FIRED
// SET. Two eligible keys in the same partition of the same batch both fire and
// both are spent, but only the earlier is rewound to — and rewinding to the
// earlier one redelivers the later one anyway, so nothing is lost. What this
// does mean is that the target set depends on BATCH COMPOSITION and the fired
// set does not. Every fired key is reported through onFault for exactly that
// reason: the reproducible record of what an injector did is what it FIRED.
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
