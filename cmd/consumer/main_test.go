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

	"github.com/pigfox/kafka-lab/internal/control"
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
