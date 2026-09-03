// Package event generates the synthetic stream the lab measures.
//
// The events are deliberately BORING AND CHEAP. This lab is about throughput,
// lag and backpressure, so anything expensive to generate would put the
// producer's own CPU into a measurement that is supposed to be about the
// broker and the consumer's declared work delay. Every field here is either a
// counter, a table lookup, or one call to a seeded PRNG.
//
// Generation is SEEDED AND DETERMINISTIC for a given seed, which is what makes
// it testable: the stream a test asserts on is the stream the producer emits.
package event

import (
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// Kinds and Regions are the small closed sets the generator draws from. They
// exist so a reader watching the live tail in the admin UI sees a stream with
// visible structure rather than noise.
var (
	Kinds   = []string{"order.placed", "order.paid", "order.shipped", "cart.updated", "user.signup"}
	Regions = []string{"eu-west", "us-east", "ap-south", "sa-east"}
)

// Event is one synthetic record.
type Event struct {
	Seq            uint64 `json:"seq"`
	Kind           string `json:"kind"`
	Region         string `json:"region"`
	AmountCents    int64  `json:"amount_cents"`
	EmittedAtMilli int64  `json:"emitted_at_unix_ms"`
	Filler         string `json:"filler,omitempty"`
}

// Generator produces a deterministic stream for a given seed. It is safe for
// concurrent use; the producer is single-goroutine today, but a generator that
// is only safe by accident is a generator that breaks the first time someone
// adds a second produce loop.
type Generator struct {
	mu     sync.Mutex
	rnd    *rand.Rand
	seq    uint64
	filler string
}

// New returns a generator seeded with seed. fillerBytes pads each event so the
// lab can be run against a message size other than the ~120 bytes the fields
// alone occupy; zero means no padding.
func New(seed int64, fillerBytes int) *Generator {
	if fillerBytes < 0 {
		fillerBytes = 0
	}
	filler := ""
	if fillerBytes > 0 {
		b := make([]byte, fillerBytes)
		for i := range b {
			b[i] = 'x'
		}
		filler = string(b)
	}
	return &Generator{rnd: rand.New(rand.NewSource(seed)), filler: filler}
}

// Next returns the next event in the stream, stamped with now.
func (g *Generator) Next(now time.Time) Event {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seq++
	return Event{
		Seq:            g.seq,
		Kind:           Kinds[g.rnd.Intn(len(Kinds))],
		Region:         Regions[g.rnd.Intn(len(Regions))],
		AmountCents:    int64(g.rnd.Intn(500_00) + 1_00),
		EmittedAtMilli: now.UnixMilli(),
		Filler:         g.filler,
	}
}

// Count reports how many events this generator has produced.
func (g *Generator) Count() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.seq
}

// Key is the record key. Partitioning by REGION rather than by sequence is the
// choice that makes the topic worth browsing in kafka-ui: keys repeat, so
// partitions carry related records instead of a round-robin smear.
func (e Event) Key() []byte { return []byte(e.Region) }

// JSON renders the record value.
func (e Event) JSON() ([]byte, error) {
	b, err := marshal(e)
	if err != nil {
		return nil, fmt.Errorf("event: marshal seq %d: %w", e.Seq, err)
	}
	return b, nil
}

// Parse reads a record value back. The admin tail uses it to render a readable
// line rather than raw JSON.
func Parse(b []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return Event{}, fmt.Errorf("event: unmarshal: %w", err)
	}
	return e, nil
}

// Summary is the one-line form shown in the admin UI's live tail.
func (e Event) Summary() string {
	return "#" + strconv.FormatUint(e.Seq, 10) + " " + e.Kind + " " + e.Region +
		" " + strconv.FormatFloat(float64(e.AmountCents)/100, 'f', 2, 64)
}

// Age is how long the event sat between production and the moment given. On a
// throttled consumer this is the number that grows, and it is the same story
// the lag panel tells from the broker's side.
func (e Event) Age(now time.Time) time.Duration {
	if e.EmittedAtMilli == 0 {
		return 0
	}
	d := now.Sub(time.UnixMilli(e.EmittedAtMilli))
	if d < 0 {
		return 0
	}
	return d
}

// marshal is a variable so the error-wrapping branch of JSON has a reachable
// input. Every field of Event is a scalar or a string, so encoding/json cannot
// fail on it today — but the branch is not dead code, it is code whose only
// input is a future field, and a wrap that has never run is a wrap that has
// never been shown to name the sequence number it claims to name.
var marshal = json.Marshal

// DedupeHeader is the record header carrying each event's identity, read by
// the consumer's idempotency store and written by the producer.
//
// The record key is the REGION — one of the four values in Regions — so it
// names a partition and cannot identify a record. The sequence number is in
// the payload, but taking the identity from there would require the consumer to
// unmarshal every value before it could tell one delivery from another, which
// makes a message's identity depend on the value schema.
const DedupeHeader = "kafka-lab-event-id"

// dedupeSeparator joins the run nonce to the sequence number. The nonce is
// hex, so it cannot contain this byte and the two halves stay separable.
const dedupeSeparator = ":"

// DedupeID builds the header value for one event: the producer's run nonce,
// a separator, and the sequence number.
//
// THE NONCE IS WHAT MAKES THE ID UNIQUE ACROSS RUNS. Seq restarts at 1 every
// time the producer process starts, so a bare sequence number would collide
// with the previous run's on a topic that retains ten minutes — and a consumer
// deduplicating on it would discard the new run's first messages as duplicates
// of the old run's.
func DedupeID(nonce string, seq uint64) string {
	return nonce + dedupeSeparator + strconv.FormatUint(seq, 10)
}

// NewRunNonce returns a fresh 16-character hex nonce for one producer run.
func NewRunNonce() (string, error) {
	b := make([]byte, 8)
	if _, err := randRead(b); err != nil {
		return "", fmt.Errorf("event: run nonce: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// randRead is a variable for the same reason marshal above is one: crypto/rand
// on every platform this lab runs on does not fail, so the error branch has no
// reachable input in production and would be an untested wrap claiming to name
// something it has never named.
var randRead = crand.Read
