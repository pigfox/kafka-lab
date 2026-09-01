// Command admin serves the control UI and publishes settings onto the bus.
//
// WHAT ADMIN IS NOT ALLOWED TO DO is the interesting part: it never calls the
// producer or the consumer over HTTP. Its only outbound effect on the pipeline
// is a record published to the compacted `control` topic. Its inbound picture
// comes from two read-only planes — Prometheus for achieved rates, the group
// coordinator for lag — plus its own tail consumer, which joins a SEPARATE
// group so that watching does not perturb what is being measured.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/pigfox/kafka-lab/internal/adminui"
	"github.com/pigfox/kafka-lab/internal/config"
	"github.com/pigfox/kafka-lab/internal/kafkabus"
	"github.com/pigfox/kafka-lab/internal/metrics"
	"github.com/pigfox/kafka-lab/internal/promquery"
	"github.com/pigfox/kafka-lab/internal/runner"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil && !isShutdown(err) {
		log.Error("admin stopped", "error", err)
		os.Exit(1)
	}
	log.Info("admin stopped cleanly")
}

func isShutdown(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

func run(log *slog.Logger) error {
	ctx, stop := runner.SignalContext()
	defer stop()

	brokers := config.Brokers("KAFKA_BROKERS", "kafka:9092")
	httpAddr := config.String("KL_HTTP_ADDR", ":8080")
	dialEvery := config.Duration("KL_DIAL_RETRY", 2*time.Second)
	promBase := config.String("KL_PROMETHEUS_URL", "http://prometheus:9090")
	rateWindow := config.String("KL_RATE_WINDOW", "30s")
	tailBuffer := config.Int("KL_TAIL_BUFFER", 64)
	lagEvery := config.Duration("KL_LAG_INTERVAL", 2*time.Second)

	// The links the PAGE renders are host-side, because the browser is outside
	// the compose network. http://prometheus:9090 would be a dead link there.
	links := adminui.Links{
		Grafana:    config.String("KL_LINK_GRAFANA", "http://localhost:18081"),
		Prometheus: config.String("KL_LINK_PROMETHEUS", "http://localhost:18082"),
		KafkaUI:    config.String("KL_LINK_KAFKA_UI", "http://localhost:18083"),
	}

	m := metrics.New(metrics.RoleAdmin)

	log.Info("waiting for the broker", "brokers", brokers)
	publishCl, err := kafkabus.DialRetry(ctx, log, brokers, dialEvery)
	if err != nil {
		return err
	}
	defer publishCl.Close()

	controlCl, err := kafkabus.DialRetry(ctx, log, brokers, dialEvery, kafkabus.ControlConsumerOpts()...)
	if err != nil {
		return err
	}
	defer controlCl.Close()

	tailCl, err := kafkabus.DialRetry(ctx, log, brokers, dialEvery, kafkabus.AdminTailOpts()...)
	if err != nil {
		return err
	}
	defer tailCl.Close()

	watcher := kafkabus.NewWatcher(controlCl, log, func() { m.ControlApplied.Inc() })
	tailer := kafkabus.NewTailer(tailCl, log, tailBuffer, nil)
	lag := kafkabus.NewLagReader(publishCl, kafkabus.ConsumerGroup)

	srv, err := adminui.New(adminui.Options{
		Log:       log,
		Publisher: kafkabus.NewPublisher(publishCl, log),
		Settings:  watcher,
		Rates:     &rates{c: promquery.New(promBase, 5*time.Second), window: rateWindow},
		Lag:       lag,
		Tail:      tailer,
		Links:     links,
	})
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.Handle("/", srv.Handler())

	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// NO WRITE TIMEOUT, deliberately. The live tail is an SSE stream that
		// stays open indefinitely; a write timeout would cut it at a fixed
		// interval and the browser would reconnect forever, which looks like a
		// flapping backend rather than a configured one.
	}

	errs := make(chan error, 4)
	go func() { errs <- watcher.Run(ctx) }()
	go func() { errs <- tailer.Run(ctx) }()
	go func() { errs <- pollLag(ctx, log, lag, m, lagEvery) }()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()
	go func() {
		log.Info("admin listening", "addr", httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errs <- err
			return
		}
		errs <- context.Canceled
	}()

	return <-errs
}

// rates asks Prometheus what was ACHIEVED. The window is configurable because
// a short one is twitchy and a long one hides the moment a slider moved, and
// which of those matters depends on whether you are demonstrating or measuring.
type rates struct {
	c      *promquery.Client
	window string
}

func (r *rates) ProducedPerSec(ctx context.Context) (float64, error) {
	return r.c.Scalar(ctx, fmt.Sprintf(`sum(rate(%s_produced_total[%s]))`, metrics.Namespace, r.window))
}

func (r *rates) ConsumedPerSec(ctx context.Context) (float64, error) {
	return r.c.Scalar(ctx, fmt.Sprintf(`sum(rate(%s_consumed_total[%s]))`, metrics.Namespace, r.window))
}

// pollLag publishes the consumer group's lag as admin's own metric, so
// Prometheus can scrape it and Grafana can plot it on the same panel as the two
// rates. Admin is the only service positioned to ask.
//
// A FAILED POLL LEAVES THE GAUGE ALONE rather than zeroing it. A zero would be
// indistinguishable on the graph from a fully drained queue, which is the exact
// opposite of what a coordinator that will not answer usually means.
func pollLag(ctx context.Context, log *slog.Logger, lag adminui.LagSource, m *metrics.Set, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			v, err := lag.Total(ctx)
			if err != nil {
				m.Errors.Inc()
				log.Warn("lag poll failed", "error", err)
				continue
			}
			m.Lag.Set(float64(v))
		}
	}
}
