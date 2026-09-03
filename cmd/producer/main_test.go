package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/event"
	"github.com/pigfox/kafka-lab/internal/metrics"
	"github.com/pigfox/kafka-lab/internal/ratelimit"
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

// A SETTINGS RECORD MUST REACH THE LIVE LIMITER AND THE METRIC TOGETHER. If it
// reached only the limiter, the dashboard would show the old requested rate
// beside the new achieved one and read as a bug in the pipeline.
func TestApplierUpdatesTheLimiterAndTheSettingMetric(t *testing.T) {
	lim := ratelimit.New(10)
	m := metrics.New(metrics.RoleProducer)
	a := applier{lim: lim, m: m, log: quiet()}

	a.Apply(control.Settings{ProducerRatePerSec: 123})

	if got := lim.Rate(); got != 123 {
		t.Fatalf("limiter: got %v want 123", got)
	}
	if got := gaugeValue(t, m, "kafka_lab_rate_limit_per_second"); got != 123 {
		t.Fatalf("metric: got %v want 123", got)
	}
}

// The applier reads only the PRODUCER's knob. Taking the consumer's would make
// both services run at whichever slider moved last.
func TestApplierIgnoresTheConsumerKnobs(t *testing.T) {
	lim := ratelimit.New(10)
	a := applier{lim: lim, m: metrics.New(metrics.RoleProducer), log: quiet()}
	a.Apply(control.Settings{ProducerRatePerSec: 7, ConsumerRatePerSec: 400, ConsumerWorkMillis: 99})
	if got := lim.Rate(); got != 7 {
		t.Fatalf("got %v want 7", got)
	}
}

func TestServeMetricsServesAndShutsDown(t *testing.T) {
	m := metrics.New(metrics.RoleProducer)
	m.Produced.Add(5)

	ctx, cancel := context.WithCancel(context.Background())
	addr := "127.0.0.1:19911"
	serveMetrics(ctx, quiet(), addr, m)

	body := waitForMetrics(t, "http://"+addr+"/metrics")
	if !strings.Contains(body, `kafka_lab_produced_total{service="producer"} 5`) {
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

// ── the wire contract: partition key and identity header ───────────────────

// fakeProduceClient captures the records the emitter builds, so what goes on
// the wire is assertable without a broker.
type fakeProduceClient struct {
	mu   sync.Mutex
	recs []*kgo.Record
	err  error
}

func (f *fakeProduceClient) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	f.mu.Lock()
	f.recs = append(f.recs, r)
	err := f.err
	f.mu.Unlock()
	promise(r, err)
}

func (f *fakeProduceClient) snapshot() []*kgo.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*kgo.Record(nil), f.recs...)
}

func emitN(t *testing.T, n int, nonce string) (*fakeProduceClient, *metrics.Set) {
	t.Helper()
	cl := &fakeProduceClient{}
	m := metrics.New(metrics.RoleProducer)
	e := &emitter{cl: cl, gen: event.New(1, 0), m: m, nonce: nonce}
	for i := 0; i < n; i++ {
		if err := e.Emit(context.Background()); err != nil {
			t.Fatalf("emit %d: %v", i, err)
		}
	}
	return cl, m
}

// THE PARTITION KEY IS THE REGION AND MUST STAY THE REGION. Partitioning by
// region is what makes the topic worth browsing — keys repeat, so partitions
// carry related records instead of a round-robin smear. A change that
// repartitioned the stream would also silently change which consumer member
// sees which records, so it fails here rather than in a dashboard.
func TestTheRecordKeyIsTheEventRegion(t *testing.T) {
	cl, _ := emitN(t, 50, "deadbeefdeadbeef")

	regions := map[string]bool{}
	for _, r := range event.Regions {
		regions[r] = true
	}

	recs := cl.snapshot()
	if len(recs) != 50 {
		t.Fatalf("produced %d records, want 50", len(recs))
	}
	for i, r := range recs {
		ev, err := event.Parse(r.Value)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if string(r.Key) != ev.Region {
			t.Fatalf("record %d: key %q is not the event's region %q", i, r.Key, ev.Region)
		}
		if !regions[string(r.Key)] {
			t.Fatalf("record %d: key %q is not one of the declared regions", i, r.Key)
		}
	}
}

// The key must NOT be the identity: there are four regions, so a consumer
// deduplicating on the key would collapse the whole stream to four messages.
func TestTheRecordKeyIsNotUniquePerRecord(t *testing.T) {
	cl, _ := emitN(t, 200, "deadbeefdeadbeef")

	keys := map[string]bool{}
	for _, r := range cl.snapshot() {
		keys[string(r.Key)] = true
	}
	if len(keys) > len(event.Regions) {
		t.Fatalf("%d distinct keys over 200 records; the partition key is no longer the region", len(keys))
	}
	if len(keys) < 2 {
		t.Fatalf("only %d distinct key(s); the stream is not spread over partitions", len(keys))
	}
}

// The identity travels in the header, and it is unique per record.
func TestEveryRecordCarriesAUniqueIdentityHeader(t *testing.T) {
	const nonce = "00112233445566ff"
	cl, _ := emitN(t, 200, nonce)

	ids := map[string]bool{}
	for i, r := range cl.snapshot() {
		id := headerValue(r, event.DedupeHeader)
		if id == "" {
			t.Fatalf("record %d carries no %s header", i, event.DedupeHeader)
		}
		if ids[id] {
			t.Fatalf("record %d repeats the identity %q", i, id)
		}
		ids[id] = true

		ev, err := event.Parse(r.Value)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if want := event.DedupeID(nonce, ev.Seq); id != want {
			t.Fatalf("record %d: header %q want %q", i, id, want)
		}
	}
	if len(ids) != 200 {
		t.Fatalf("%d distinct identities over 200 records", len(ids))
	}
}

// Two producer runs must not issue the same identity for their first record,
// or a consumer would discard the newer run's opening messages as duplicates.
func TestTwoRunsIssueDifferentIdentitiesForTheSameSequence(t *testing.T) {
	first, _ := emitN(t, 1, "aaaaaaaaaaaaaaaa")
	second, _ := emitN(t, 1, "bbbbbbbbbbbbbbbb")

	a := headerValue(first.snapshot()[0], event.DedupeHeader)
	b := headerValue(second.snapshot()[0], event.DedupeHeader)
	if a == b {
		t.Fatalf("both runs issued %q for their first record", a)
	}
}

// Exactly one header, so a consumer scanning for the first match cannot pick
// up a stale duplicate of it.
func TestTheIdentityHeaderIsTheOnlyHeader(t *testing.T) {
	cl, _ := emitN(t, 5, "deadbeefdeadbeef")
	for i, r := range cl.snapshot() {
		if len(r.Headers) != 1 {
			t.Fatalf("record %d carries %d headers, want 1", i, len(r.Headers))
		}
		if r.Headers[0].Key != event.DedupeHeader {
			t.Fatalf("record %d: header %q want %q", i, r.Headers[0].Key, event.DedupeHeader)
		}
	}
}

// produced_total counts ACKNOWLEDGED records; a failed produce is an error.
func TestEmitCountsAcknowledgementsAndFailuresSeparately(t *testing.T) {
	cl, m := emitN(t, 3, "deadbeefdeadbeef")
	if got := counterValue(t, m, "kafka_lab_produced_total"); got != 3 {
		t.Fatalf("produced_total %v want 3", got)
	}

	cl.mu.Lock()
	cl.err = errors.New("broker refused")
	cl.mu.Unlock()

	e := &emitter{cl: cl, gen: event.New(1, 0), m: m, nonce: "deadbeefdeadbeef"}
	if err := e.Emit(context.Background()); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if got := counterValue(t, m, "kafka_lab_produced_total"); got != 3 {
		t.Fatalf("produced_total %v want 3; a failed produce was counted as throughput", got)
	}
	if got := counterValue(t, m, "kafka_lab_errors_total"); got != 1 {
		t.Fatalf("errors_total %v want 1", got)
	}
}

func headerValue(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
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
