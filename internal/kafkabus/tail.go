package kafkabus

import (
	"context"
	"errors"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/fanout"
)

// AdminTailGroup is the consumer group admin's live tail joins.
//
// IT IS ITS OWN GROUP, SEPARATE FROM THE CONSUMER'S, and that is the single
// most important line in this file. If the admin tail joined the consumer's
// group it would take partitions away from the consumer, so opening the UI
// would change the consume rate the UI is displaying — the observation would
// alter the measurement, on the one panel the whole demo is read from. A
// separate group has its own independent offsets and takes nothing.
const AdminTailGroup = "kafka-lab-admin-tail"

// Tailer streams the events topic to the admin UI.
type Tailer struct {
	cl   *kgo.Client
	log  *slog.Logger
	out  *fanout.Broadcast[Record]
	seen func(Record)
}

// NewTailer returns a tailer fanning records out with the given per-subscriber
// buffer.
func NewTailer(cl *kgo.Client, log *slog.Logger, buffer int, seen func(Record)) *Tailer {
	if seen == nil {
		seen = func(Record) {}
	}
	return &Tailer{cl: cl, log: log, out: fanout.NewBroadcast[Record](buffer), seen: seen}
}

// Subscribe returns a channel of tailed records and an unsubscribe function.
func (t *Tailer) Subscribe() (<-chan Record, func()) { return t.out.Subscribe() }

// Stats reports deliveries, drops and current subscriber count.
func (t *Tailer) Stats() (sent, dropped uint64, subscribers int) { return t.out.Stats() }

// Run tails until ctx is done, then ends every subscription.
func (t *Tailer) Run(ctx context.Context) error {
	defer t.out.Close()
	for {
		fetches := t.cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return e.Err
				}
				t.log.Warn("tail fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) {
			rec := lift(r)
			t.seen(rec)
			t.out.Send(rec)
		})
	}
}

// AdminTailOpts are the client options the live tail needs.
//
// THREE CHOICES, EACH LOAD-BEARING:
//
//   - AtEnd, so opening the UI shows what is happening NOW rather than
//     replaying ten minutes of retention at whatever speed the network allows.
//   - Offsets are NEVER COMMITTED, so an admin restart cannot resume from a
//     stale position and cannot leave committed state behind on the broker for
//     a group that exists only to watch.
//   - It is a group member rather than a direct consumer purely so the group
//     shows up in kafka-ui, where a reader can SEE that the observer is
//     separate from the consumer being observed.
func AdminTailOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumeTopics(control.EventsTopic),
		kgo.ConsumerGroup(AdminTailGroup),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
	}
}
