package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/apply"
	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/event"
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
		d := drainCounter{m: m, inner: apply.New(apply.Options{Dedupe: dedupe, Observer: observer{m: m}})}
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
		d := drainCounter{m: m, inner: apply.New(apply.Options{Dedupe: dedupe, Observer: observer{m: m}})}
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
