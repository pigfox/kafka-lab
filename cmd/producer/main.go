// Command producer emits a synthetic event stream at a rate set over the bus.
//
// This file is deliberately about wiring and nothing else. Every decision worth
// testing lives in internal/ — the rate limiter that wakes on a live change
// (internal/ratelimit), the produce loop's survive-a-broker-blip policy
// (internal/runner), the event shape (internal/event). A main() cannot be unit
// tested, so main() is given nothing to be wrong about.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/config"
	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/event"
	"github.com/pigfox/kafka-lab/internal/kafkabus"
	"github.com/pigfox/kafka-lab/internal/metrics"
	"github.com/pigfox/kafka-lab/internal/ratelimit"
	"github.com/pigfox/kafka-lab/internal/runner"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil && !isShutdown(err) {
		log.Error("producer stopped", "error", err)
		os.Exit(1)
	}
	log.Info("producer stopped cleanly")
}

func isShutdown(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

func run(log *slog.Logger) error {
	ctx, stop := runner.SignalContext()
	defer stop()

	brokers := config.Brokers("KAFKA_BROKERS", "kafka:9092")
	metricsAddr := config.String("KL_METRICS_ADDR", ":2112")
	dialEvery := config.Duration("KL_DIAL_RETRY", 2*time.Second)
	seed := config.Int("KL_EVENT_SEED", 1)
	filler := config.Int("KL_EVENT_FILLER_BYTES", 0)

	m := metrics.New(metrics.RoleProducer)
	serveMetrics(ctx, log, metricsAddr, m)

	// TWO CLIENTS, ONE PROCESS. The produce client and the control watcher are
	// separate because franz-go binds a client's consumer configuration at
	// construction: one client cannot both produce to `events` and consume
	// `control` from offset 0 as a non-group member. Two small clients is
	// cheaper than the contortion.
	log.Info("waiting for the broker", "brokers", brokers)
	producerCl, err := kafkabus.DialRetry(ctx, log, brokers, dialEvery,
		kgo.DefaultProduceTopic(control.EventsTopic),
		kgo.ProducerBatchMaxBytes(1<<20),
	)
	if err != nil {
		return err
	}
	defer producerCl.Close()

	controlCl, err := kafkabus.DialRetry(ctx, log, brokers, dialEvery, kafkabus.ControlConsumerOpts()...)
	if err != nil {
		return err
	}
	defer controlCl.Close()

	lim := ratelimit.New(control.Defaults().ProducerRatePerSec)
	m.RateLimit.Set(control.Defaults().ProducerRatePerSec)

	watcher := kafkabus.NewWatcher(controlCl, log, func() { m.ControlApplied.Inc() })
	gen := event.New(int64(seed), filler)

	errs := make(chan error, 3)
	go func() { errs <- watcher.Run(ctx) }()
	go func() {
		errs <- runner.ApplySettings(ctx, watcher, applier{lim: lim, m: m, log: log})
	}()
	go func() {
		errs <- runner.ProduceLoop(ctx, log, lim,
			&emitter{cl: producerCl, gen: gen, m: m},
			func(error) { m.Errors.Inc() })
	}()

	return <-errs
}

// applier pushes a settings change into the live rate limiter and the metric
// that reports what was ASKED for.
type applier struct {
	lim *ratelimit.Limiter
	m   *metrics.Set
	log *slog.Logger
}

func (a applier) Apply(s control.Settings) {
	a.lim.SetRate(s.ProducerRatePerSec)
	a.m.RateLimit.Set(s.ProducerRatePerSec)
}

// emitter produces one event.
//
// IT PRODUCES ASYNCHRONOUSLY and counts on the CALLBACK, not at the call site.
// A synchronous produce would make the achieved rate a measurement of the
// broker's round-trip time rather than of the rate limit, which is the one
// thing this service exists to demonstrate. Counting in the callback means
// produced_total counts ACKNOWLEDGED records — a record that failed is an
// error, not throughput.
type emitter struct {
	cl  *kgo.Client
	gen *event.Generator
	m   *metrics.Set
}

func (e *emitter) Emit(ctx context.Context) error {
	ev := e.gen.Next(time.Now())
	value, err := ev.JSON()
	if err != nil {
		return err
	}
	e.cl.Produce(ctx, &kgo.Record{Key: ev.Key(), Value: value}, func(_ *kgo.Record, err error) {
		if err != nil {
			e.m.Errors.Inc()
			return
		}
		e.m.Produced.Inc()
	})
	return nil
}

// serveMetrics starts the /metrics listener and shuts it down with ctx.
func serveMetrics(ctx context.Context, log *slog.Logger, addr string, m *metrics.Set) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		log.Info("metrics listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics listener stopped", "error", err)
		}
	}()
}
