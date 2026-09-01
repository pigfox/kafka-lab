// Command consumer reads the events topic under a rate limit set over the bus.
//
// It is the throttled half of the demo: drag its rate below the producer's and
// lag climbs; open it back up and lag drains. The ordering that makes that true
// — a per-RECORD throttle rather than a per-batch one — lives in
// internal/runner.ConsumeLoop, with the reasoning written down beside it.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/config"
	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/kafkabus"
	"github.com/pigfox/kafka-lab/internal/metrics"
	"github.com/pigfox/kafka-lab/internal/ratelimit"
	"github.com/pigfox/kafka-lab/internal/runner"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil && !isShutdown(err) {
		log.Error("consumer stopped", "error", err)
		os.Exit(1)
	}
	log.Info("consumer stopped cleanly")
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

	m := metrics.New(metrics.RoleConsumer)
	serveMetrics(ctx, log, metricsAddr, m)

	log.Info("waiting for the broker", "brokers", brokers)
	consumerCl, err := kafkabus.DialRetry(ctx, log, brokers, dialEvery, kafkabus.ConsumerOpts()...)
	if err != nil {
		return err
	}
	defer consumerCl.Close()

	controlCl, err := kafkabus.DialRetry(ctx, log, brokers, dialEvery, kafkabus.ControlConsumerOpts()...)
	if err != nil {
		return err
	}
	defer controlCl.Close()

	defaults := control.Defaults()
	lim := ratelimit.New(defaults.ConsumerRatePerSec)
	work := &atomic.Int64{}
	work.Store(int64(defaults.ConsumerWorkMillis))
	m.RateLimit.Set(defaults.ConsumerRatePerSec)
	m.WorkMillis.Set(float64(defaults.ConsumerWorkMillis))

	watcher := kafkabus.NewWatcher(controlCl, log, func() { m.ControlApplied.Inc() })

	errs := make(chan error, 3)
	go func() { errs <- watcher.Run(ctx) }()
	go func() {
		errs <- runner.ApplySettings(ctx, watcher, applier{lim: lim, work: work, m: m})
	}()
	go func() {
		errs <- runner.ConsumeLoop(ctx, log, lim, work, runner.RealSleeper{},
			&fetcher{cl: consumerCl},
			func() { m.Consumed.Inc() },
			func(error) { m.Errors.Inc() })
	}()

	return <-errs
}

type applier struct {
	lim  *ratelimit.Limiter
	work *atomic.Int64
	m    *metrics.Set
}

func (a applier) Apply(s control.Settings) {
	a.lim.SetRate(s.ConsumerRatePerSec)
	a.work.Store(int64(s.ConsumerWorkMillis))
	a.m.RateLimit.Set(s.ConsumerRatePerSec)
	a.m.WorkMillis.Set(float64(s.ConsumerWorkMillis))
}

// fetcher adapts the kgo client to runner.Fetcher.
//
// COMMITTING IS WHAT MAKES LAG FALL. Lag is the distance between the log end
// and this group's COMMITTED offset, so a consumer that read everything and
// committed nothing would leave the panel pinned at maximum while the messages
// were long gone. The commit is the visible half of the drain.
type fetcher struct{ cl *kgo.Client }

func (f *fetcher) Fetch(ctx context.Context) (int, error) {
	// A bounded poll rather than an indefinite one: the loop must come back
	// round often enough to notice a cancelled context even on a silent topic.
	pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	fetches := f.cl.PollRecords(pollCtx, 500)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		// A poll that merely timed out is not a fault; it is an idle topic.
		for _, e := range errs {
			if e.Err != context.DeadlineExceeded {
				return 0, e.Err
			}
		}
		return 0, nil
	}
	return fetches.NumRecords(), nil
}

func (f *fetcher) Commit(ctx context.Context) error {
	return f.cl.CommitUncommittedOffsets(ctx)
}

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
