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
	"github.com/pigfox/kafka-lab/internal/fault"
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

// consumerConfig is everything this binary reads from the environment.
//
// IT IS A STRUCT AND A FUNCTION SO IT CAN BE TESTED, the same move the
// committer and offsetSeeker interfaces below already make: name the part that
// has a decision in it, so the part that needs a broker stops standing in front
// of it. run() is still untestable — it dials twice and blocks — but a wrong
// default is a defect that never reaches a broker to be noticed.
type consumerConfig struct {
	Brokers     []string
	MetricsAddr string
	DialEvery   time.Duration

	// DEDUPE IS OFF BY DEFAULT, and that is the honest default rather than a
	// cautious one: at-least-once is what the broker gives, so a lab whose
	// default arm hid the duplicates would be teaching the wrong lesson. Both
	// arms are reachable by exporting one variable, with no code change.
	Dedupe         bool
	DedupeCapacity int
	DedupeTTL      time.Duration

	// FAULT INJECTION IS OFF BY DEFAULT (rate 0). The rate is the fraction of
	// distinct keys whose commit is replaced by a cursor rewind — see
	// internal/fault, which explains why skipping the commit alone produces no
	// duplicate at all. The seed makes the fault set a pure function of the
	// record keys, so both arms of an experiment are fed the same faults.
	FaultRate float64
	FaultSeed string
}

// readConsumerConfig reads the environment. Every default is a literal, so a
// bare clone runs with an empty environment.
func readConsumerConfig() consumerConfig {
	return consumerConfig{
		Brokers:        config.Brokers("KAFKA_BROKERS", "kafka:9092"),
		MetricsAddr:    config.String("KL_METRICS_ADDR", ":2112"),
		DialEvery:      config.Duration("KL_DIAL_RETRY", 2*time.Second),
		Dedupe:         config.Bool("KL_DEDUPE", false),
		DedupeCapacity: config.Int("KL_DEDUPE_CAPACITY", apply.DefaultCapacity),
		DedupeTTL:      config.Duration("KL_DEDUPE_TTL", apply.DefaultTTL),
		FaultRate:      config.Float("KL_FAULT_RATE", 0),
		FaultSeed:      config.String("KL_FAULT_SEED", "kafka-lab"),
	}
}

// newDeliveries builds the delivery-semantics applier and its two bounded
// stores, each wired to report what it forgets.
//
// NO EFFECT IS ATTACHED. This lab has no business side effect to run, and
// inventing one would put a fabricated workload inside a measurement about
// delivery. applied_total IS the record of the effect running; a real consumer
// would pass its own function here.
func newDeliveries(log *slog.Logger, m *metrics.Set, cfg consumerConfig) *apply.Applier {
	return apply.New(apply.Options{
		Dedupe:      cfg.Dedupe,
		Seen:        idem.New(cfg.DedupeCapacity, cfg.DedupeTTL, nil, lossOf(log, m, metrics.StoreSeen)),
		AppliedKeys: idem.New(cfg.DedupeCapacity, cfg.DedupeTTL, nil, lossOf(log, m, metrics.StoreApplied)),
		Observer:    observer{m: m, log: log},
	})
}

// newInjector builds the fault injector, logging every key that fires.
//
// IT LOGS FIRED KEYS, NOT REWIND TARGETS, and the distinction cost a graded run
// to learn: Targets returns at most one record per partition per batch, so the
// target set is a subset of the fired set chosen by broker batching. The fired
// set is a pure function of the seed and the delivered keys, so it is the one
// that reproduces — and the one a comparison between two arms can be made over.
func newInjector(log *slog.Logger, cfg consumerConfig) *fault.Injector {
	return fault.New(cfg.FaultRate, cfg.FaultSeed, func(r runner.Record) {
		log.Info("fault fired", "key", r.DedupeKey, "partition", r.Partition,
			"offset", r.Offset, "epoch", r.LeaderEpoch)
	})
}

func run(log *slog.Logger) error {
	ctx, stop := runner.SignalContext()
	defer stop()

	cfg := readConsumerConfig()

	m := metrics.New(metrics.RoleConsumer)
	serveMetrics(ctx, log, cfg.MetricsAddr, m)

	log.Info("waiting for the broker", "brokers", cfg.Brokers)
	consumerCl, err := kafkabus.DialRetry(ctx, log, cfg.Brokers, cfg.DialEvery, kafkabus.ConsumerOpts()...)
	if err != nil {
		return err
	}
	defer consumerCl.Close()

	controlCl, err := kafkabus.DialRetry(ctx, log, cfg.Brokers, cfg.DialEvery, kafkabus.ControlConsumerOpts()...)
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

	deliveries := newDeliveries(log, m, cfg)
	injector := newInjector(log, cfg)

	log.Info("delivery semantics configured",
		"dedupe", cfg.Dedupe, "capacity", cfg.DedupeCapacity, "ttl", cfg.DedupeTTL,
		"header", event.DedupeHeader,
		"fault_rate", cfg.FaultRate, "fault_seed", cfg.FaultSeed)

	watcher := kafkabus.NewWatcher(controlCl, log, func() { m.ControlApplied.Inc() })

	errs := make(chan error, 3)
	go func() { errs <- watcher.Run(ctx) }()
	go func() {
		errs <- runner.ApplySettings(ctx, watcher, applier{lim: lim, work: work, m: m})
	}()
	go func() {
		errs <- runner.ConsumeLoop(ctx, log, lim, work, runner.RealSleeper{},
			&fetcher{cl: consumerCl, seeker: consumerCl},
			drainCounter{m: m, inner: deliveries},
			injector,
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

// observer publishes the four delivery outcomes, and logs the two of them that
// are redeliveries.
//
// SUPPRESSED AND DOUBLE-APPLIED ARE THE SAME EVENT SEEN FROM THE TWO ARMS: a
// record arriving whose key has been here before. On the dedupe arm its effect
// is skipped; on the other it runs again. Logging both with the same message
// and a `suppressed` field means one grep counts the redeliveries of either
// run, and the count is checkable against the metric rather than derived from
// it.
type observer struct {
	m   *metrics.Set
	log *slog.Logger
}

func (o observer) Applied(runner.Record) { o.m.Applied.Inc() }

func (o observer) Suppressed(r runner.Record) {
	o.m.Suppressed.Inc()
	o.log.Info("redelivery", "key", r.DedupeKey, "partition", r.Partition,
		"offset", r.Offset, "suppressed", true)
}

func (o observer) DoubleApplied(r runner.Record) {
	o.m.DoubleApplied.Inc()
	o.log.Info("redelivery", "key", r.DedupeKey, "partition", r.Partition,
		"offset", r.Offset, "suppressed", false)
}

func (o observer) NoKey(runner.Record) { o.m.NoDedupeKey.Inc() }

// lossOf builds the callback one idempotency store reports its forgotten keys
// through.
//
// EVERY LOSS IS BOTH COUNTED AND NAMED. The counter is what a dashboard reads
// and what decides whether a measured run is publishable at all — a double-apply
// figure taken from a run whose store was overflowing measures the store, not
// the delivery semantics. The log line names the key, so a loss can be traced to
// the record it belongs to instead of being an anonymous increment.
func lossOf(log *slog.Logger, m *metrics.Set, store metrics.Store) func(idem.Loss) {
	return func(l idem.Loss) {
		switch l.Reason {
		case idem.LossCapacity:
			m.Evictions.WithLabelValues(string(store)).Inc()
		case idem.LossTTL:
			m.Expiries.WithLabelValues(string(store)).Inc()
		}
		log.Warn("idempotency store forgot a key; its redelivery would read as new",
			"store", string(store), "reason", string(l.Reason), "key", l.Key)
	}
}

// fetcher adapts the kgo client to runner.Fetcher.
//
// COMMITTING IS WHAT MAKES LAG FALL. Lag is the distance between the log end
// and this group's COMMITTED offset, so a consumer that read everything and
// committed nothing would leave the panel pinned at maximum while the messages
// were long gone. The commit is the visible half of the drain.
//
// IT HOLDS THE CLIENT TWICE, and that is a seam rather than an accident. `cl`
// polls and commits, both of which need a broker and are exercised by running
// the lab. `seeker` is the same client behind a one-method interface, because
// the rewind is pure TRANSLATION — records to a topic/partition/epoch/offset map
// — and translating an epoch wrongly is a silent resume at the wrong place. That
// translation is worth asserting without eight containers.
type fetcher struct {
	cl     *kgo.Client
	seeker offsetSeeker
}

// offsetSeeker is the cursor seek as this file needs it. *kgo.Client satisfies it.
type offsetSeeker interface {
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
}

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

// Rewind seeks each target's partition back to that record's offset, so it and
// everything after it in that partition is delivered again.
//
// IT TAKES NO CONTEXT AND CANNOT FAIL. SetOffsets is a local re-assignment of
// the client's own consume cursor — it sends nothing to the broker and returns
// nothing — so an error return here would be a value that is always nil,
// checked at a call site that could never do anything about it.
//
// THE EPOCH IS THE RECORD'S OWN, not -1. franz-go documents LeaderEpoch as what
// "clients use for data loss detection": seeking with the real epoch lets the
// broker say that the log has been truncated beneath us, while -1 waives the
// check and resumes at whatever now sits at that offset.
func (f *fetcher) Rewind(to []runner.Record) {
	set := make(map[string]map[int32]kgo.EpochOffset, 1)
	for _, r := range to {
		byPartition, ok := set[r.Topic]
		if !ok {
			byPartition = make(map[int32]kgo.EpochOffset, len(to))
			set[r.Topic] = byPartition
		}
		byPartition[r.Partition] = kgo.EpochOffset{
			Epoch:  r.LeaderEpoch,
			Offset: r.Offset,
		}
	}
	f.seeker.SetOffsets(set)
}

// toRunnerRecords lifts franz-go records into the library-agnostic type the
// consume loop takes, so no kgo type crosses into internal/runner.
func toRunnerRecords(rs []*kgo.Record) []runner.Record {
	out := make([]runner.Record, len(rs))
	for i, r := range rs {
		out[i] = runner.Record{
			DedupeKey:   dedupeKeyOf(r),
			Topic:       r.Topic,
			Partition:   r.Partition,
			Offset:      r.Offset,
			LeaderEpoch: r.LeaderEpoch,
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
