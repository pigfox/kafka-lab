// Command consumer reads the events topic under a rate limit set over the bus.
//
// It is the throttled half of the demo: drag its rate below the producer's and
// lag climbs; open it back up and lag drains. The ordering that makes that true
// — a per-RECORD throttle rather than a per-batch one — lives in
// internal/runner.ConsumeLoop, with the reasoning written down beside it.
//
// It is also where DELIVERY SEMANTICS are visible. Kafka gives at-least-once,
// so a record can arrive twice; internal/apply counts what that costs, and
// internal/idem suppresses the repeat when the dedupe flag is on. The flag is
// OFF by default so the lab's default arm is the honest one.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/apply"
	"github.com/pigfox/kafka-lab/internal/config"
	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/event"
	"github.com/pigfox/kafka-lab/internal/idem"
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

	// DEDUPE IS OFF BY DEFAULT, and that is the honest default rather than a
	// cautious one: at-least-once is what the broker gives, so a lab whose
	// default arm hid the duplicates would be teaching the wrong lesson. Both
	// arms are reachable by exporting this one variable, with no code change.
	dedupe := config.Bool("KL_DEDUPE", false)
	dedupeCapacity := config.Int("KL_DEDUPE_CAPACITY", apply.DefaultCapacity)
	dedupeTTL := config.Duration("KL_DEDUPE_TTL", apply.DefaultTTL)

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

	deliveries := apply.New(apply.Options{
		Dedupe:      dedupe,
		Seen:        idem.New(dedupeCapacity, dedupeTTL, nil),
		AppliedKeys: idem.New(dedupeCapacity, dedupeTTL, nil),
		Observer:    observer{m: m},
		// NO EFFECT IS ATTACHED. This lab has no business side effect to run,
		// and inventing one would put a fabricated workload inside a
		// measurement about delivery. applied_total IS the record of the
		// effect running; a real consumer would pass its own function here.
	})
	log.Info("delivery semantics configured",
		"dedupe", dedupe, "capacity", dedupeCapacity, "ttl", dedupeTTL,
		"header", event.DedupeHeader)

	watcher := kafkabus.NewWatcher(controlCl, log, func() { m.ControlApplied.Inc() })

	errs := make(chan error, 3)
	go func() { errs <- watcher.Run(ctx) }()
	go func() {
		errs <- runner.ApplySettings(ctx, watcher, applier{lim: lim, work: work, m: m})
	}()
	go func() {
		errs <- runner.ConsumeLoop(ctx, log, lim, work, runner.RealSleeper{},
			&fetcher{cl: consumerCl},
			drainCounter{m: m, inner: deliveries},
			func(error) { m.Errors.Inc() })
	}()

	loopErr := <-errs
	commitFinal(log, consumerCl)
	return loopErr
}

// committer is the shutdown commit as this file needs it, named small so a test
// can drive it without a broker.
type committer interface {
	CommitUncommittedOffsets(ctx context.Context) error
}

// commitFinal records progress on the way out.
//
// WITHOUT THIS, A CLEAN STOP LOSES UP TO A BATCH. Autocommit is disabled (see
// kafkabus.ConsumerOpts), so the ONLY commits are the explicit one at the end
// of each batch and this one; a consumer interrupted part-way through a batch
// would otherwise leave every applied record uncommitted and re-read all of
// them on the next start. Those redeliveries are legitimate at-least-once
// behaviour rather than a bug, but they are the AVOIDABLE kind, and a demo
// producing avoidable duplicates on every ./stop.sh teaches nothing about the
// unavoidable ones.
//
// It uses a FRESH context on purpose: the loop's context is already cancelled
// by the time this runs, so a commit inheriting it would fail before it was
// sent.
func commitFinal(log *slog.Logger, c committer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.CommitUncommittedOffsets(ctx); err != nil {
		log.Warn("final commit failed; those records will be redelivered", "error", err)
		return
	}
	log.Info("final offsets committed")
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

// drainCounter increments consumed_total for EVERY record the loop finished
// working on, then hands the record to the delivery-semantics applier.
//
// THE WRAPPER EXISTS SO THE DRAIN PANEL DOES NOT MOVE. consumed_total is the
// achieved rate the lag story is told with, and a record the consumer spent its
// throttle and its work delay on has been worked on whether or not its effect
// ran. Counting it inside the applier instead would make consumed_total fall
// when dedupe was switched on, and the drain panel would report a slower
// consumer where nothing about the consumer had changed.
type drainCounter struct {
	m     *metrics.Set
	inner runner.RecordApplier
}

func (d drainCounter) Apply(r runner.Record) {
	d.m.Consumed.Inc()
	d.inner.Apply(r)
}

// observer publishes the four delivery outcomes.
type observer struct{ m *metrics.Set }

func (o observer) Applied()       { o.m.Applied.Inc() }
func (o observer) Suppressed()    { o.m.Suppressed.Inc() }
func (o observer) DoubleApplied() { o.m.DoubleApplied.Inc() }
func (o observer) NoKey()         { o.m.NoDedupeKey.Inc() }

// fetcher adapts the kgo client to runner.Fetcher.
//
// COMMITTING IS WHAT MAKES LAG FALL. Lag is the distance between the log end
// and this group's COMMITTED offset, so a consumer that read everything and
// committed nothing would leave the panel pinned at maximum while the messages
// were long gone. The commit is the visible half of the drain.
type fetcher struct{ cl *kgo.Client }

func (f *fetcher) Fetch(ctx context.Context) ([]runner.Record, error) {
	// A bounded poll rather than an indefinite one: the loop must come back
	// round often enough to notice a cancelled context even on a silent topic.
	pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	fetches := f.cl.PollRecords(pollCtx, 500)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		// A poll that merely timed out is not a fault; it is an idle topic.
		for _, e := range errs {
			if e.Err != context.DeadlineExceeded {
				return nil, e.Err
			}
		}
		return nil, nil
	}
	return toRunnerRecords(fetches.Records()), nil
}

func (f *fetcher) Commit(ctx context.Context) error {
	return f.cl.CommitUncommittedOffsets(ctx)
}

// toRunnerRecords lifts franz-go records into the library-agnostic type the
// consume loop takes, so no kgo type crosses into internal/runner.
func toRunnerRecords(rs []*kgo.Record) []runner.Record {
	out := make([]runner.Record, len(rs))
	for i, r := range rs {
		out[i] = runner.Record{
			DedupeKey: dedupeKeyOf(r),
			Partition: r.Partition,
			Offset:    r.Offset,
		}
	}
	return out
}

// dedupeKeyOf reads the identity header the producer wrote.
//
// A MISSING OR EMPTY HEADER YIELDS AN EMPTY STRING, AND NOTHING IS INVENTED.
// The obvious substitute — partition and offset — is wrong in the one case that
// matters: a rebalance can redeliver a record at the SAME offset, so it would
// sometimes work, and a retained record read by a new group member after a
// partition reassignment is the same record with the same offset but was never
// applied by this process. Worse, any per-delivery identity (a timestamp, a
// counter) is unique BY CONSTRUCTION, so every redelivery would read as
// first-seen and the dedupe store would suppress nothing while appearing to
// work. An empty key is a record that cannot be deduplicated, and
// internal/apply counts it as exactly that.
func dedupeKeyOf(r *kgo.Record) string {
	for _, h := range r.Headers {
		if h.Key == event.DedupeHeader {
			return string(h.Value)
		}
	}
	return ""
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
