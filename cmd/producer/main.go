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

// producerConfig is everything this binary reads from the environment.
//
// IT IS A STRUCT AND A FUNCTION SO IT CAN BE TESTED, which is the same move
// produceClient, committer and offsetSeeker already make elsewhere in these two
// commands: name the part that has a decision in it, and the part that needs a
// broker stops standing in front of it. run() below is still untestable — it
// dials twice and then blocks forever — but the reads are not, and reads are
// where a wrong default hides.
type producerConfig struct {
	Brokers     []string
	MetricsAddr string
	DialEvery   time.Duration
	Seed        int
	Filler      int

	// Nonce is KL_RUN_NONCE verbatim. EMPTY MEANS GENERATE, and the
	// resolution is deliberately not done here: reading configuration and
	// reaching for entropy are different jobs with different failure modes.
	Nonce string
}

// readProducerConfig reads the environment. Every default is a literal, so a
// bare clone runs with an empty environment.
func readProducerConfig() producerConfig {
	return producerConfig{
		Brokers:     config.Brokers("KAFKA_BROKERS", "kafka:9092"),
		MetricsAddr: config.String("KL_METRICS_ADDR", ":2112"),
		DialEvery:   config.Duration("KL_DIAL_RETRY", 2*time.Second),
		Seed:        config.Int("KL_EVENT_SEED", 1),
		Filler:      config.Int("KL_EVENT_FILLER_BYTES", 0),
		Nonce:       config.String("KL_RUN_NONCE", ""),
	}
}

// resolveNonce returns the run nonce for one producer process.
//
// ONE NONCE PER RUN. Every record this process emits carries it, so the
// consumer can tell this run's message #1 from the previous run's — the
// sequence number restarts at 1 on every start, and the events topic retains
// ten minutes, so the two overlap.
//
// A FAILURE TO READ ENTROPY STOPS THE PRODUCER rather than falling back to a
// fixed value: a predictable nonce is a nonce that collides, and a consumer
// would then discard this run's opening messages as duplicates of the last.
//
// KL_RUN_NONCE OVERRIDES IT, AND ONLY AN EXPERIMENT SHOULD SET IT. Pinning the
// nonce makes the whole record stream reproducible: the generator is already
// seeded, so a fixed nonce plus a fixed KL_EVENT_SEED means two separate runs
// emit byte-identical keys. That is what lets a measured comparison feed BOTH
// arms the same records — and since the fault set is a pure function of the
// seed and the key, the same faults too. Without it the two arms have disjoint
// key spaces and are not comparable.
//
// It is unsafe in ordinary use for exactly the reason the random default
// exists: two runs sharing a nonce also share identities, so a consumer with
// dedupe on would discard the second run's messages as duplicates of the first.
// The experiment resets the broker between arms, which is what makes it safe
// there and nowhere else — hence the warning, which is the only signal a reader
// of the logs gets that the run is not self-contained.
//
// generate is injected so the entropy failure has a reachable input. It is the
// same reason internal/event keeps randRead as a variable.
func resolveNonce(log *slog.Logger, configured string, generate func() (string, error)) (string, error) {
	if configured != "" {
		log.Warn("KL_RUN_NONCE is set: record identities are REPRODUCIBLE and will collide with any other run sharing this nonce",
			"nonce", configured)
		return configured, nil
	}
	return generate()
}

func run(log *slog.Logger) error {
	ctx, stop := runner.SignalContext()
	defer stop()

	cfg := readProducerConfig()

	m := metrics.New(metrics.RoleProducer)
	serveMetrics(ctx, log, cfg.MetricsAddr, m)

	// TWO CLIENTS, ONE PROCESS. The produce client and the control watcher are
	// separate because franz-go binds a client's consumer configuration at
	// construction: one client cannot both produce to `events` and consume
	// `control` from offset 0 as a non-group member. Two small clients is
	// cheaper than the contortion.
	log.Info("waiting for the broker", "brokers", cfg.Brokers)
	producerCl, err := kafkabus.DialRetry(ctx, log, cfg.Brokers, cfg.DialEvery,
		kgo.DefaultProduceTopic(control.EventsTopic),
		kgo.ProducerBatchMaxBytes(1<<20),
	)
	if err != nil {
		return err
	}
	defer producerCl.Close()

	controlCl, err := kafkabus.DialRetry(ctx, log, cfg.Brokers, cfg.DialEvery, kafkabus.ControlConsumerOpts()...)
	if err != nil {
		return err
	}
	defer controlCl.Close()

	lim := ratelimit.New(control.Defaults().ProducerRatePerSec)
	m.RateLimit.Set(control.Defaults().ProducerRatePerSec)

	watcher := kafkabus.NewWatcher(controlCl, log, func() { m.ControlApplied.Inc() })
	gen := event.New(int64(cfg.Seed), cfg.Filler)

	nonce, err := resolveNonce(log, cfg.Nonce, event.NewRunNonce)
	if err != nil {
		return err
	}
	log.Info("producer run identity", "nonce", nonce, "header", event.DedupeHeader)

	errs := make(chan error, 3)
	go func() { errs <- watcher.Run(ctx) }()
	go func() {
		errs <- runner.ApplySettings(ctx, watcher, applier{lim: lim, m: m, log: log})
	}()
	go func() {
		errs <- runner.ProduceLoop(ctx, log, lim,
			&emitter{cl: producerCl, gen: gen, m: m, nonce: nonce},
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

// produceClient is the produce call as the emitter needs it.
//
// IT IS AN INTERFACE SO THE RECORD IS ASSERTABLE. What the producer puts on the
// wire — the partition key and the identity header — is now a contract with the
// consumer, and a contract that can only be checked by running eight containers
// is a contract that gets checked once.
type produceClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
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
	cl    produceClient
	gen   *event.Generator
	m     *metrics.Set
	nonce string
}

func (e *emitter) Emit(ctx context.Context) error {
	ev := e.gen.Next(time.Now())
	value, err := ev.JSON()
	if err != nil {
		return err
	}
	// THE KEY AND THE HEADER ANSWER DIFFERENT QUESTIONS, and swapping them is
	// the mistake this arrangement exists to prevent. The KEY is the region,
	// which decides the PARTITION — four values, chosen so partitions carry
	// related records and the topic is worth browsing. The HEADER is the
	// record's IDENTITY, which the consumer deduplicates on. A key used as an
	// identity would collapse the whole stream to four messages.
	rec := &kgo.Record{
		Key:   ev.Key(),
		Value: value,
		Headers: []kgo.RecordHeader{{
			Key:   event.DedupeHeader,
			Value: []byte(event.DedupeID(e.nonce, ev.Seq)),
		}},
	}
	e.cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
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
