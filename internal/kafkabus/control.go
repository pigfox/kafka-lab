package kafkabus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/fanout"
)

// Publisher writes settings to the control topic.
type Publisher struct {
	cl  *kgo.Client
	log *slog.Logger
}

// NewPublisher returns a publisher over cl.
func NewPublisher(cl *kgo.Client, log *slog.Logger) *Publisher {
	return &Publisher{cl: cl, log: log}
}

// Publish writes s synchronously. It is SYNCHRONOUS on purpose: the admin UI
// answers the browser only once the broker has the record, so a slider that
// reports success is a slider whose setting is durable. An asynchronous publish
// would let the UI confirm a change that a broker restart then loses.
func (p *Publisher) Publish(ctx context.Context, s control.Settings) error {
	value, err := control.Encode(s)
	if err != nil {
		return err
	}
	rec := &kgo.Record{
		Topic: control.Topic,
		Key:   []byte(control.Key),
		Value: value,
	}
	if err := p.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("kafkabus: publish settings: %w", err)
	}
	p.log.Info("settings published", "settings", s.String())
	return nil
}

// Watcher tails the control topic and holds the newest settings.
//
// IT IS A DIRECT PARTITION CONSUMER, NOT A GROUP MEMBER, and that is the whole
// design in one choice. Every service needs EVERY settings record — this is a
// broadcast, not a work queue — and a consumer group would hand the partition
// to exactly one member. Reading the partition directly from offset 0 also
// means a service that starts late walks the compacted history and arrives at
// the current state without anyone having to republish.
type Watcher struct {
	cl      *kgo.Client
	log     *slog.Logger
	latest  *fanout.Latest[control.Settings]
	applied func()
}

// NewWatcher returns a watcher whose initial value is control.Defaults(). The
// `applied` callback fires once per accepted record and is where the caller
// bumps its control_applied_total counter.
func NewWatcher(cl *kgo.Client, log *slog.Logger, applied func()) *Watcher {
	if applied == nil {
		applied = func() {}
	}
	return &Watcher{
		cl:      cl,
		log:     log,
		latest:  fanout.NewLatest(control.Defaults()),
		applied: applied,
	}
}

// Settings returns the newest settings and whether any record has been read.
func (w *Watcher) Settings() (control.Settings, bool) { return w.latest.Get() }

// Changed returns a channel closed when settings next change.
func (w *Watcher) Changed() <-chan struct{} { return w.latest.Changed() }

// Run tails the control topic until ctx is done.
//
// A MALFORMED RECORD IS LOGGED AND SKIPPED, never fatal. The control topic is
// writable by anything on the compose network — kafka-ui offers a produce form
// — and a lab that dies because someone typed into that form would be a lab
// that punishes exactly the exploration it exists to invite.
func (w *Watcher) Run(ctx context.Context) error {
	for {
		fetches := w.cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return e.Err
				}
				w.log.Warn("control fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Key) != control.Key {
				return
			}
			s, err := control.Decode(r.Value)
			if err != nil {
				w.log.Warn("skipping malformed control record",
					"offset", r.Offset, "error", err)
				return
			}
			prev, had := w.latest.Get()
			w.latest.Set(s)
			w.applied()
			if !had || !prev.Equal(s) {
				w.log.Info("settings applied", "settings", s.String(), "offset", r.Offset)
			}
		})
	}
}

// ControlConsumerOpts are the client options a control watcher needs: the
// control topic, read from the very beginning of the partition.
func ControlConsumerOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumeTopics(control.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
}
