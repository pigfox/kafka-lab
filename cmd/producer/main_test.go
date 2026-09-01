package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
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
