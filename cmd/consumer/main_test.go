package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/apply"
	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/event"
	"github.com/pigfox/kafka-lab/internal/idem"
	"github.com/pigfox/kafka-lab/internal/metrics"
	"github.com/pigfox/kafka-lab/internal/ratelimit"
	"github.com/pigfox/kafka-lab/internal/runner"
)

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func TestIsShutdown(t *testing.T) {
	if !isShutdown(context.Canceled) || !isShutdown(context.DeadlineExceeded) {
		t.Fatal("both context errors are ordinary shutdown")
	}
	if isShutdown(errors.New("broker exploded")) {
		t.Fatal("a real failure is not shutdown")
	}
}

// BOTH CONSUMER KNOBS AND BOTH METRICS MOVE TOGETHER. A rate that reached the
// limiter but not the gauge would put the old requested figure on the dashboard
// beside the new achieved one, which reads as a broken pipeline.
func TestApplierUpdatesEveryConsumerKnob(t *testing.T) {
	lim := ratelimit.New(10)
	work := &atomic.Int64{}
	m := metrics.New(metrics.RoleConsumer)

	applier{lim: lim, work: work, m: m}.Apply(control.Settings{
		ConsumerRatePerSec: 33, ConsumerWorkMillis: 44,
	})

	if got := lim.Rate(); got != 33 {
		t.Fatalf("limiter: got %v want 33", got)
	}
	if got := work.Load(); got != 44 {
		t.Fatalf("work: got %v want 44", got)
	}
	if got := gaugeValue(t, m, "kafka_lab_rate_limit_per_second"); got != 33 {
		t.Fatalf("rate gauge: got %v want 33", got)
	}
	if got := gaugeValue(t, m, "kafka_lab_work_milliseconds"); got != 44 {
		t.Fatalf("work gauge: got %v want 44", got)
	}
}

// The applier reads only the CONSUMER's knobs; taking the producer's would make
// both services run at whichever slider moved last.
func TestApplierIgnoresTheProducerKnob(t *testing.T) {
	lim := ratelimit.New(10)
	applier{lim: lim, work: &atomic.Int64{}, m: metrics.New(metrics.RoleConsumer)}.
		Apply(control.Settings{ProducerRatePerSec: 500, ConsumerRatePerSec: 6})
	if got := lim.Rate(); got != 6 {
		t.Fatalf("got %v want 6", got)
	}
}

func TestServeMetricsServesAndShutsDown(t *testing.T) {
	m := metrics.New(metrics.RoleConsumer)
	m.Consumed.Add(9)

	ctx, cancel := context.WithCancel(context.Background())
	addr := "127.0.0.1:19912"
	serveMetrics(ctx, quiet(), addr, m)

	body := waitForMetrics(t, "http://"+addr+"/metrics")
	if !strings.Contains(body, `kafka_lab_consumed_total{service="consumer"} 9`) {
		t.Fatalf("metrics body:\n%s", body)
	}

	cancel()
	waitForClosed(t, "http://"+addr+"/metrics")
}

func waitForMetrics(t *testing.T, url string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			defer resp.Body.Close()
			buf := make([]byte, 64<<10)
			n, _ := resp.Body.Read(buf)
			return string(buf[:n])
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the metrics listener never came up at %s", url)
	return ""
}

func waitForClosed(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err != nil {
			return
		}
		resp.Body.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the metrics listener outlived its context")
}

func gaugeValue(t *testing.T, m *metrics.Set, name string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			if metric.GetGauge() != nil {
				return metric.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("gauge %s not found", name)
	return 0
}

// ── reading the identity off the wire ──────────────────────────────────────

func TestDedupeKeyOfReadsTheProducersHeader(t *testing.T) {
	r := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: event.DedupeHeader, Value: []byte("deadbeefdeadbeef:17")},
	}}
	if got := dedupeKeyOf(r); got != "deadbeefdeadbeef:17" {
		t.Fatalf("got %q", got)
	}
}

func TestDedupeKeyOfFindsTheHeaderAmongOthers(t *testing.T) {
	r := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "traceparent", Value: []byte("00-abc-def-01")},
		{Key: event.DedupeHeader, Value: []byte("n:5")},
		{Key: "content-type", Value: []byte("application/json")},
	}}
	if got := dedupeKeyOf(r); got != "n:5" {
		t.Fatalf("got %q", got)
	}
}

// A MISSING HEADER YIELDS AN EMPTY STRING AND NOTHING IS INVENTED. Any
// per-delivery substitute — a timestamp, a counter, even partition/offset — is
// unique by construction for a redelivery, so the store would report every
// repeat as first-seen and suppress nothing while appearing to work.
func TestDedupeKeyOfSynthesisesNothing(t *testing.T) {
	cases := []struct {
		name string
		rec  *kgo.Record
	}{
		{"no headers at all", &kgo.Record{Partition: 3, Offset: 99}},
		{"other headers only", &kgo.Record{Partition: 3, Offset: 99, Headers: []kgo.RecordHeader{
			{Key: "traceparent", Value: []byte("00-abc-def-01")},
		}}},
		{"the header with an empty value", &kgo.Record{Partition: 3, Offset: 99, Headers: []kgo.RecordHeader{
			{Key: event.DedupeHeader, Value: []byte("")},
		}}},
		{"the header with a nil value", &kgo.Record{Partition: 3, Offset: 99, Headers: []kgo.RecordHeader{
			{Key: event.DedupeHeader, Value: nil},
		}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := dedupeKeyOf(c.rec); got != "" {
				t.Fatalf("got %q, want the empty string", got)
			}
		})
	}
}

// The header name is case-sensitive on the wire, and the producer writes it in
// exactly one form. A lookup that quietly matched another casing would hide a
// producer that had drifted.
func TestDedupeKeyOfDoesNotMatchADifferentCasing(t *testing.T) {
	r := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: strings.ToUpper(event.DedupeHeader), Value: []byte("n:5")},
	}}
	if got := dedupeKeyOf(r); got != "" {
		t.Fatalf("got %q; the lookup is not exact", got)
	}
}

func TestToRunnerRecordsCarriesIdentityPartitionAndOffset(t *testing.T) {
	in := []*kgo.Record{
		{Partition: 0, Offset: 10, Headers: []kgo.RecordHeader{{Key: event.DedupeHeader, Value: []byte("n:1")}}},
		{Partition: 2, Offset: 11, Headers: []kgo.RecordHeader{{Key: event.DedupeHeader, Value: []byte("n:2")}}},
		{Partition: 1, Offset: 12},
	}
	want := []runner.Record{
		{DedupeKey: "n:1", Partition: 0, Offset: 10},
		{DedupeKey: "n:2", Partition: 2, Offset: 11},
		{DedupeKey: "", Partition: 1, Offset: 12},
	}

	got := toRunnerRecords(in)
	if len(got) != len(want) {
		t.Fatalf("mapped %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestToRunnerRecordsOnAnEmptyFetch(t *testing.T) {
	if got := toRunnerRecords(nil); len(got) != 0 {
		t.Fatalf("an empty fetch mapped to %d records", len(got))
	}
}

// ── the drain counter must not move ────────────────────────────────────────

// consumed_total is the achieved rate the lag story is told with. A record the
// consumer spent its throttle and work delay on has been worked on whether or
// not its effect ran, so switching dedupe ON must not make the drain panel
// report a slower consumer.
func TestConsumedTotalCountsEveryRecordOnBothArms(t *testing.T) {
	input := []runner.Record{
		{DedupeKey: "n:1"}, {DedupeKey: "n:2"}, {DedupeKey: "n:1"},
		{DedupeKey: "n:3"}, {DedupeKey: "n:1"},
	}

	for _, dedupe := range []bool{false, true} {
		m := metrics.New(metrics.RoleConsumer)
		d := drainCounter{m: m, inner: apply.New(apply.Options{Dedupe: dedupe, Observer: observer{m: m, log: quiet()}})}
		for _, r := range input {
			d.Apply(r)
		}
		if got := counterValue(t, m, "kafka_lab_consumed_total"); got != 5 {
			t.Fatalf("dedupe=%v: consumed_total %v want 5 — the drain panel moved", dedupe, got)
		}
	}
}

// The same input, the flag as the only difference. This is the shape PF-S312
// will measure over a live stream.
func TestTheObserverPublishesBothArmsToDistinctSeries(t *testing.T) {
	input := []runner.Record{
		{DedupeKey: "n:1"}, {DedupeKey: "n:2"}, {DedupeKey: "n:1"},
		{DedupeKey: "n:3"}, {DedupeKey: "n:1"}, {DedupeKey: ""},
	}

	type arm struct {
		applied, suppressed, double, noKey float64
	}
	got := map[bool]arm{}
	for _, dedupe := range []bool{false, true} {
		m := metrics.New(metrics.RoleConsumer)
		d := drainCounter{m: m, inner: apply.New(apply.Options{Dedupe: dedupe, Observer: observer{m: m, log: quiet()}})}
		for _, r := range input {
			d.Apply(r)
		}
		got[dedupe] = arm{
			applied:    counterValue(t, m, "kafka_lab_applied_total"),
			suppressed: counterValue(t, m, "kafka_lab_duplicates_suppressed_total"),
			double:     counterValue(t, m, "kafka_lab_double_applied_total"),
			noKey:      counterValue(t, m, "kafka_lab_records_without_dedupe_key_total"),
		}
	}

	off, on := got[false], got[true]

	// Six records in: n:1 three times, n:2 and n:3 once each, one keyless.
	if off.applied != 6 {
		t.Fatalf("dedupe off: applied %v want 6", off.applied)
	}
	if off.double != 2 {
		t.Fatalf("dedupe off: double applied %v want 2 (the two repeats of n:1)", off.double)
	}
	if off.suppressed != 0 {
		t.Fatalf("dedupe off: suppressed %v want 0", off.suppressed)
	}
	if on.applied != 4 {
		t.Fatalf("dedupe on: applied %v want 4 (three distinct keys plus the keyless one)", on.applied)
	}
	if on.suppressed != 2 {
		t.Fatalf("dedupe on: suppressed %v want 2", on.suppressed)
	}
	if on.double != 0 {
		t.Fatalf("dedupe on: double applied %v want 0", on.double)
	}
	if off.noKey != 1 || on.noKey != 1 {
		t.Fatalf("no-key count off=%v on=%v, want 1 on both — it is never silently passed", off.noKey, on.noKey)
	}
}

// ── the shutdown commit ────────────────────────────────────────────────────

type fakeCommitter struct {
	calls  atomic.Int64
	err    error
	hadCtx atomic.Bool
}

func (f *fakeCommitter) CommitUncommittedOffsets(ctx context.Context) error {
	f.calls.Add(1)
	f.hadCtx.Store(ctx.Err() == nil)
	return f.err
}

// Autocommit is disabled, so the only commits are the per-batch one and this.
// A clean stop part-way through a batch would otherwise re-read every applied
// record on the next start.
func TestCommitFinalCommitsOnTheWayOut(t *testing.T) {
	c := &fakeCommitter{}
	commitFinal(quiet(), c)
	if c.calls.Load() != 1 {
		t.Fatalf("committed %d times, want 1", c.calls.Load())
	}
}

// IT MUST NOT INHERIT THE LOOP'S CONTEXT. By the time this runs the loop's
// context is already cancelled, so a commit built on it would fail before it
// was sent — and would fail silently, because the failure is only logged.
func TestCommitFinalUsesALiveContext(t *testing.T) {
	c := &fakeCommitter{}
	commitFinal(quiet(), c)
	if !c.hadCtx.Load() {
		t.Fatal("the final commit was handed a context that was already done")
	}
}

// A failed final commit is logged and swallowed: the records are redelivered,
// which is legitimate at-least-once behaviour, and crashing on the way out
// would turn a lost commit into a non-zero exit.
func TestCommitFinalSurvivesAFailedCommit(t *testing.T) {
	c := &fakeCommitter{err: errors.New("coordinator gone")}
	commitFinal(quiet(), c)
	if c.calls.Load() != 1 {
		t.Fatalf("committed %d times, want 1", c.calls.Load())
	}
}

func counterValue(t *testing.T, m *metrics.Set, name string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			if metric.GetCounter() != nil {
				return metric.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("counter %s not found", name)
	return 0
}

// ── the rewind ─────────────────────────────────────────────────────────────

// THE EPOCH TRAVELS WITH THE RECORD. franz-go documents LeaderEpoch as what
// clients use for data-loss detection: seeking with the real epoch lets the
// broker say the log has been truncated beneath us, while -1 waives the check
// and resumes at whatever now sits at that offset.
func TestToRunnerRecordsCarriesTheTopicAndLeaderEpoch(t *testing.T) {
	in := []*kgo.Record{{
		Topic: "events", Partition: 2, Offset: 77, LeaderEpoch: 9,
		Headers: []kgo.RecordHeader{{Key: event.DedupeHeader, Value: []byte("n:1")}},
	}}

	got := toRunnerRecords(in)
	want := runner.Record{DedupeKey: "n:1", Topic: "events", Partition: 2, Offset: 77, LeaderEpoch: 9}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

// setOffsetsRecorder captures what a rewind would hand franz-go, so the
// translation is assertable without a broker.
type setOffsetsRecorder struct {
	calls []map[string]map[int32]kgo.EpochOffset
}

func (s *setOffsetsRecorder) SetOffsets(o map[string]map[int32]kgo.EpochOffset) {
	s.calls = append(s.calls, o)
}

func TestRewindSeeksEachPartitionToItsRecordsOffsetAndEpoch(t *testing.T) {
	rec := &setOffsetsRecorder{}
	f := &fetcher{seeker: rec}

	f.Rewind([]runner.Record{
		{Topic: "events", Partition: 0, Offset: 10, LeaderEpoch: 4},
		{Topic: "events", Partition: 2, Offset: 20, LeaderEpoch: 5},
	})

	if len(rec.calls) != 1 {
		t.Fatalf("SetOffsets called %d times, want 1", len(rec.calls))
	}
	byPartition, ok := rec.calls[0]["events"]
	if !ok {
		t.Fatalf("no offsets set for the events topic: %+v", rec.calls[0])
	}
	if len(byPartition) != 2 {
		t.Fatalf("set %d partitions, want 2", len(byPartition))
	}
	if got := byPartition[0]; got.Offset != 10 || got.Epoch != 4 {
		t.Fatalf("partition 0 sought to %+v, want offset 10 epoch 4", got)
	}
	if got := byPartition[2]; got.Offset != 20 || got.Epoch != 5 {
		t.Fatalf("partition 2 sought to %+v, want offset 20 epoch 5", got)
	}
}

// A REWIND MUST NEVER PASS EPOCH -1. That value waives truncation detection, so
// a seek into a log that has been truncated would silently resume at whatever
// now occupies the offset instead of reporting data loss.
func TestRewindNeverWaivesTruncationDetection(t *testing.T) {
	rec := &setOffsetsRecorder{}
	f := &fetcher{seeker: rec}

	f.Rewind([]runner.Record{{Topic: "events", Partition: 1, Offset: 5, LeaderEpoch: 3}})

	for topic, byPartition := range rec.calls[0] {
		for partition, eo := range byPartition {
			if eo.Epoch == -1 {
				t.Fatalf("%s/%d was sought with epoch -1", topic, partition)
			}
		}
	}
}

func TestRewindGroupsTargetsByTopic(t *testing.T) {
	rec := &setOffsetsRecorder{}
	f := &fetcher{seeker: rec}

	f.Rewind([]runner.Record{
		{Topic: "events", Partition: 0, Offset: 1, LeaderEpoch: 1},
		{Topic: "other", Partition: 0, Offset: 2, LeaderEpoch: 1},
	})

	if got := len(rec.calls[0]); got != 2 {
		t.Fatalf("set offsets for %d topics, want 2", got)
	}
}

func TestRewindOnAnEmptyTargetListSetsNothing(t *testing.T) {
	rec := &setOffsetsRecorder{}
	f := &fetcher{seeker: rec}
	f.Rewind(nil)
	if len(rec.calls[0]) != 0 {
		t.Fatalf("an empty rewind set %+v", rec.calls[0])
	}
}

// ── the loss callbacks ─────────────────────────────────────────────────────

// EVERY LOSS IS COUNTED UNDER ITS OWN STORE AND REASON. A double-apply figure
// taken from a run whose store was overflowing measures the store, not the
// delivery semantics, so these counters are what decide whether a measured run
// is publishable at all.
func TestALossIncrementsTheRightCounterForItsStoreAndReason(t *testing.T) {
	cases := []struct {
		store  metrics.Store
		reason idem.LossReason
		metric string
		label  string
	}{
		{metrics.StoreSeen, idem.LossCapacity, "kafka_lab_dedupe_evictions_total", "seen"},
		{metrics.StoreSeen, idem.LossTTL, "kafka_lab_dedupe_expiries_total", "seen"},
		{metrics.StoreApplied, idem.LossCapacity, "kafka_lab_dedupe_evictions_total", "applied"},
		{metrics.StoreApplied, idem.LossTTL, "kafka_lab_dedupe_expiries_total", "applied"},
	}
	for _, c := range cases {
		t.Run(string(c.store)+"/"+string(c.reason), func(t *testing.T) {
			m := metrics.New(metrics.RoleConsumer)
			lossOf(quiet(), m, c.store)(idem.Loss{Key: "run:1", Reason: c.reason})

			if got := labelledCounter(t, m, c.metric, c.label); got != 1 {
				t.Fatalf("%s{store=%q} is %v, want 1", c.metric, c.label, got)
			}
			// And nothing else moved.
			other := "kafka_lab_dedupe_expiries_total"
			if c.metric == other {
				other = "kafka_lab_dedupe_evictions_total"
			}
			if got := labelledCounter(t, m, other, c.label); got != 0 {
				t.Fatalf("%s{store=%q} also moved, to %v", other, c.label, got)
			}
		})
	}
}

// The two stores must not be conflated: `seen` decides suppression, `applied`
// makes a repeat recognisable, and a loss in each has a different consequence.
func TestALossInOneStoreDoesNotMoveTheOther(t *testing.T) {
	m := metrics.New(metrics.RoleConsumer)
	lossOf(quiet(), m, metrics.StoreSeen)(idem.Loss{Key: "run:1", Reason: idem.LossCapacity})

	if got := labelledCounter(t, m, "kafka_lab_dedupe_evictions_total", "seen"); got != 1 {
		t.Fatalf("seen evictions %v want 1", got)
	}
	if got := labelledCounter(t, m, "kafka_lab_dedupe_evictions_total", "applied"); got != 0 {
		t.Fatalf("applied evictions %v want 0", got)
	}
}

// A store wired through lossOf reports its real losses, so the counter and the
// set agree rather than being two independent tallies.
func TestAStoreWiredThroughLossOfReportsItsRealEvictions(t *testing.T) {
	m := metrics.New(metrics.RoleConsumer)
	s := idem.New(2, time.Minute, nil, lossOf(quiet(), m, metrics.StoreSeen))

	s.Observe("a")
	s.Observe("b")
	s.Observe("c") // evicts "a"
	s.Observe("d") // evicts "b"

	if got := labelledCounter(t, m, "kafka_lab_dedupe_evictions_total", "seen"); got != 2 {
		t.Fatalf("counter says %v evictions", got)
	}
	if s.Evictions() != 2 {
		t.Fatalf("the set says %d evictions", s.Evictions())
	}
}

func labelledCounter(t *testing.T, m *metrics.Set, name, store string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, l := range metric.GetLabel() {
				if l.GetName() == "store" && l.GetValue() == store {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("counter %s{store=%q} not found", name, store)
	return 0
}

// ── configuration ──────────────────────────────────────────────────────────
//
// run() dials twice and then blocks, so nothing downstream of it can be driven
// in a test. The reads are the part with a decision in them, and a wrong
// default here never reaches a broker to be noticed — the lab would simply run
// the wrong experiment and report it confidently.

func TestConsumerConfigDefaultsRunOnAnEmptyEnvironment(t *testing.T) {
	for _, key := range []string{
		"KAFKA_BROKERS", "KL_METRICS_ADDR", "KL_DIAL_RETRY",
		"KL_DEDUPE", "KL_DEDUPE_CAPACITY", "KL_DEDUPE_TTL",
		"KL_FAULT_RATE", "KL_FAULT_SEED",
	} {
		t.Setenv(key, "")
	}

	cfg := readConsumerConfig()

	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "kafka:9092" {
		t.Errorf("brokers %v want [kafka:9092]", cfg.Brokers)
	}
	if cfg.MetricsAddr != ":2112" {
		t.Errorf("metrics addr %q want :2112", cfg.MetricsAddr)
	}
	if cfg.DialEvery != 2*time.Second {
		t.Errorf("dial retry %v want 2s", cfg.DialEvery)
	}

	// THE TWO DEFAULTS THAT DECIDE WHAT THE LAB IS. Dedupe off is the honest
	// arm — at-least-once is what the broker gives — and a fault rate of zero
	// means the lab injects nothing until asked. Either one defaulting the
	// other way would change every number the lab reports, silently.
	if cfg.Dedupe {
		t.Error("dedupe defaults ON; the default arm must be at-least-once")
	}
	if cfg.FaultRate != 0 {
		t.Errorf("fault rate defaults to %v; the lab must inject nothing unless asked", cfg.FaultRate)
	}

	if cfg.DedupeCapacity != apply.DefaultCapacity {
		t.Errorf("capacity %d want %d", cfg.DedupeCapacity, apply.DefaultCapacity)
	}
	if cfg.DedupeTTL != apply.DefaultTTL {
		t.Errorf("ttl %v want %v", cfg.DedupeTTL, apply.DefaultTTL)
	}
	if cfg.FaultSeed != "kafka-lab" {
		t.Errorf("fault seed %q want kafka-lab", cfg.FaultSeed)
	}
}

func TestConsumerConfigReadsEveryKnob(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "a:9092,b:9092")
	t.Setenv("KL_METRICS_ADDR", ":9998")
	t.Setenv("KL_DIAL_RETRY", "1500ms")
	t.Setenv("KL_DEDUPE", "true")
	t.Setenv("KL_DEDUPE_CAPACITY", "1234")
	t.Setenv("KL_DEDUPE_TTL", "90s")
	t.Setenv("KL_FAULT_RATE", "0.01")
	t.Setenv("KL_FAULT_SEED", "pf-s314")

	cfg := readConsumerConfig()

	if len(cfg.Brokers) != 2 {
		t.Errorf("brokers %v", cfg.Brokers)
	}
	if cfg.MetricsAddr != ":9998" {
		t.Errorf("metrics addr %q", cfg.MetricsAddr)
	}
	if cfg.DialEvery != 1500*time.Millisecond {
		t.Errorf("dial retry %v", cfg.DialEvery)
	}
	if !cfg.Dedupe {
		t.Error("dedupe did not turn on")
	}
	if cfg.DedupeCapacity != 1234 {
		t.Errorf("capacity %d", cfg.DedupeCapacity)
	}
	if cfg.DedupeTTL != 90*time.Second {
		t.Errorf("ttl %v", cfg.DedupeTTL)
	}
	if cfg.FaultRate != 0.01 {
		t.Errorf("fault rate %v", cfg.FaultRate)
	}
	if cfg.FaultSeed != "pf-s314" {
		t.Errorf("fault seed %q", cfg.FaultSeed)
	}
}

// An unparseable knob falls back rather than failing the process — but the two
// that decide the experiment must fall back to OFF, not to on.
func TestConsumerConfigFallsBackSafelyOnUnparseableKnobs(t *testing.T) {
	t.Setenv("KL_DEDUPE", "yes-please")
	t.Setenv("KL_FAULT_RATE", "one percent")
	t.Setenv("KL_DEDUPE_CAPACITY", "lots")
	t.Setenv("KL_DEDUPE_TTL", "a while")

	cfg := readConsumerConfig()

	if cfg.Dedupe {
		t.Error("an unparseable KL_DEDUPE turned dedupe ON")
	}
	if cfg.FaultRate != 0 {
		t.Errorf("an unparseable KL_FAULT_RATE gave rate %v; it must inject nothing", cfg.FaultRate)
	}
	if cfg.DedupeCapacity != apply.DefaultCapacity {
		t.Errorf("capacity %d want the default", cfg.DedupeCapacity)
	}
	if cfg.DedupeTTL != apply.DefaultTTL {
		t.Errorf("ttl %v want the default", cfg.DedupeTTL)
	}
}

// ── construction ───────────────────────────────────────────────────────────

func TestNewDeliveriesCarriesTheDedupeFlagThrough(t *testing.T) {
	for _, dedupe := range []bool{false, true} {
		m := metrics.New(metrics.RoleConsumer)
		a := newDeliveries(quiet(), m, consumerConfig{
			Dedupe:         dedupe,
			DedupeCapacity: 64,
			DedupeTTL:      time.Minute,
		})
		if a.Dedupe() != dedupe {
			t.Fatalf("built with Dedupe=%v, applier reports %v", dedupe, a.Dedupe())
		}
	}
}

// THE TWO STORES ARE SEPARATE OBJECTS. `seen` decides suppression and is only
// consulted when dedupe is on; `applied` is the per-key tally that makes a
// repeat recognisable and runs on both arms. One store doing both jobs would
// make the at-least-once arm report the duplicates it is supposed to expose.
func TestNewDeliveriesBuildsTwoDistinctStores(t *testing.T) {
	a := newDeliveries(quiet(), metrics.New(metrics.RoleConsumer), consumerConfig{
		DedupeCapacity: 64,
		DedupeTTL:      time.Minute,
	})
	if a.Seen() == a.AppliedKeys() {
		t.Fatal("the seen-set and the apply tally are the same store")
	}
	if a.Seen() == nil || a.AppliedKeys() == nil {
		t.Fatal("a store was nil")
	}
}

// Each store's losses must reach its OWN label, or a reader cannot tell a store
// that was too small from one that is working.
func TestNewDeliveriesWiresEachStoreToItsOwnLossLabel(t *testing.T) {
	m := metrics.New(metrics.RoleConsumer)
	a := newDeliveries(quiet(), m, consumerConfig{
		Dedupe:         true,
		DedupeCapacity: 1, // so the second distinct key evicts the first
		DedupeTTL:      time.Minute,
	})

	a.Apply(runner.Record{DedupeKey: "a"})
	a.Apply(runner.Record{DedupeKey: "b"})

	if got := labelledCounter(t, m, "kafka_lab_dedupe_evictions_total", "seen"); got == 0 {
		t.Error("the seen-set's eviction did not reach the seen label")
	}
	if got := labelledCounter(t, m, "kafka_lab_dedupe_evictions_total", "applied"); got == 0 {
		t.Error("the apply tally's eviction did not reach the applied label")
	}
}

// The capacity and TTL must reach the stores, not sit unused in the config.
func TestNewDeliveriesAppliesTheConfiguredCapacity(t *testing.T) {
	a := newDeliveries(quiet(), metrics.New(metrics.RoleConsumer), consumerConfig{
		DedupeCapacity: 2,
		DedupeTTL:      time.Minute,
	})

	for _, k := range []string{"a", "b", "c", "d"} {
		a.Apply(runner.Record{DedupeKey: k})
	}
	if got := a.AppliedKeys().Len(); got != 2 {
		t.Fatalf("the apply tally holds %d keys; the configured capacity of 2 did not reach it", got)
	}
}

func TestNewInjectorCarriesTheRateAndSeed(t *testing.T) {
	i := newInjector(quiet(), consumerConfig{FaultRate: 0.25, FaultSeed: "pf-s314"})
	if got := i.Rate(); got != 0.25 {
		t.Fatalf("rate %v want 0.25", got)
	}
}

// A rate of zero is the default and must fire nothing at all.
func TestNewInjectorAtTheDefaultRateFiresNothing(t *testing.T) {
	i := newInjector(quiet(), consumerConfig{FaultSeed: "kafka-lab"})
	for n := 0; n < 500; n++ {
		if i.Fault(runner.Record{DedupeKey: "run:" + strconv.Itoa(n)}) {
			t.Fatal("the default injector faulted a record")
		}
	}
	if i.Fired() != 0 {
		t.Fatalf("fired %d keys at the default rate", i.Fired())
	}
}

// THE INJECTOR LOGS EVERY KEY THAT FIRES, not the rewind targets. Targets picks
// at most one record per partition per batch, so a log of targets is a subset
// chosen by broker batching and cannot be compared across two runs.
func TestNewInjectorLogsEveryFiredKey(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	i := newInjector(log, consumerConfig{FaultRate: 1, FaultSeed: "pf-s314"})

	// Three eligible keys in ONE partition of ONE batch: Targets can return
	// only the earliest, but all three fire.
	targets := i.Targets([]runner.Record{
		{DedupeKey: "early", Topic: "events", Partition: 0, Offset: 10, LeaderEpoch: 4},
		{DedupeKey: "middle", Topic: "events", Partition: 0, Offset: 11, LeaderEpoch: 4},
		{DedupeKey: "late", Topic: "events", Partition: 0, Offset: 12, LeaderEpoch: 4},
	})
	if len(targets) != 1 {
		t.Fatalf("got %d targets, want 1", len(targets))
	}

	out := buf.String()
	if got := strings.Count(out, `msg="fault fired"`); got != 3 {
		t.Fatalf("the log carries %d fault-fired lines, want 3:\n%s", got, out)
	}
	for _, key := range []string{"early", "middle", "late"} {
		if !strings.Contains(out, "key="+key) {
			t.Errorf("the log does not name the fired key %q:\n%s", key, out)
		}
	}
	// The line must carry the whole address, so a fault can be traced to the
	// record it belongs to.
	if !strings.Contains(out, "partition=0") || !strings.Contains(out, "epoch=4") {
		t.Errorf("a fault line is missing its partition or epoch:\n%s", out)
	}
}
