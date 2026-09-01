package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pigfox/kafka-lab/internal/metrics"
	"github.com/pigfox/kafka-lab/internal/promquery"
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

// The queries must name the metrics the services actually publish and the
// window the operator configured. A typo here is a panel that reads zero
// forever with no error anywhere.
func TestRateQueriesMatchThePublishedMetricNames(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Query().Get("query"))
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"3.5"]}]}}`)
	}))
	defer srv.Close()

	r := &rates{c: promquery.New(srv.URL, time.Second), window: "45s"}

	if got, err := r.ProducedPerSec(context.Background()); err != nil || got != 3.5 {
		t.Fatalf("produced: got %v, %v", got, err)
	}
	if got, err := r.ConsumedPerSec(context.Background()); err != nil || got != 3.5 {
		t.Fatalf("consumed: got %v, %v", got, err)
	}

	if len(seen) != 2 {
		t.Fatalf("queries: %v", seen)
	}
	if seen[0] != `sum(rate(kafka_lab_produced_total[45s]))` {
		t.Fatalf("produced query: %q", seen[0])
	}
	if seen[1] != `sum(rate(kafka_lab_consumed_total[45s]))` {
		t.Fatalf("consumed query: %q", seen[1])
	}
	// The names must be the ones the metric set really registers, not a
	// hand-copied string that drifts.
	for _, q := range seen {
		if !strings.Contains(q, metrics.Namespace) {
			t.Fatalf("query %q does not use the published namespace", q)
		}
	}
}

func TestRateQueryErrorsAreReported(t *testing.T) {
	r := &rates{c: promquery.New("http://127.0.0.1:1", 200*time.Millisecond), window: "30s"}
	if _, err := r.ProducedPerSec(context.Background()); err == nil {
		t.Fatal("want an error")
	}
	if _, err := r.ConsumedPerSec(context.Background()); err == nil {
		t.Fatal("want an error")
	}
}

// A FAILED POLL LEAVES THE GAUGE ALONE rather than zeroing it. A zero is
// indistinguishable on the graph from a fully drained queue, which is the exact
// opposite of what a coordinator that will not answer usually means.
func TestPollLagLeavesTheGaugeAloneOnFailure(t *testing.T) {
	m := metrics.New(metrics.RoleAdmin)
	m.Lag.Set(500)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	// A reader against a closed port fails every poll.
	err := pollLag(ctx, quiet(), &failingLag{}, m, 10*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if got := gaugeValue(t, m, "kafka_lab_consumer_group_lag"); got != 500 {
		t.Fatalf("a failed poll moved the gauge to %v; it must stay at 500", got)
	}
	if got := counterValue(t, m, "kafka_lab_errors_total"); got == 0 {
		t.Fatal("a failed poll must be counted")
	}
}

func TestPollLagStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pollLag(ctx, quiet(), &failingLag{}, metrics.New(metrics.RoleAdmin), time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func gaugeValue(t *testing.T, m *metrics.Set, name string) float64 {
	t.Helper()
	return sample(t, m, name, func(g float64, c float64, isGauge bool) float64 {
		if isGauge {
			return g
		}
		return c
	})
}

func counterValue(t *testing.T, m *metrics.Set, name string) float64 {
	t.Helper()
	return sample(t, m, name, func(g float64, c float64, isGauge bool) float64 {
		if isGauge {
			return g
		}
		return c
	})
}

func sample(t *testing.T, m *metrics.Set, name string, pick func(float64, float64, bool) float64) float64 {
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
				return pick(metric.GetGauge().GetValue(), 0, true)
			}
			if metric.GetCounter() != nil {
				return pick(0, metric.GetCounter().GetValue(), false)
			}
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

// failingLag is a coordinator that will not answer.
type failingLag struct{}

func (failingLag) Total(context.Context) (int64, error) {
	return 0, errors.New("group coordinator unavailable")
}

// A poll that SUCCEEDS must move the gauge, or the test above would pass
// against a pollLag that never wrote to it at all.
func TestPollLagPublishesASuccessfulRead(t *testing.T) {
	m := metrics.New(metrics.RoleAdmin)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = pollLag(ctx, quiet(), fixedLag(4242), m, 10*time.Millisecond)

	if got := gaugeValue(t, m, "kafka_lab_consumer_group_lag"); got != 4242 {
		t.Fatalf("gauge: got %v want 4242", got)
	}
}

type fixedLag int64

func (f fixedLag) Total(context.Context) (int64, error) { return int64(f), nil }
