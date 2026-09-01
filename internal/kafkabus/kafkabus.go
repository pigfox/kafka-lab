// Package kafkabus is the franz-go glue: the only place in the lab that knows
// what a kgo.Client is.
//
// EVERYTHING ELSE TALKS TO INTERFACES, and that boundary is drawn here rather
// than for its own sake. The admin HTTP server, the producer loop and the
// consumer loop all have logic worth testing — a slider that clamps, a settings
// record that is ignored when it changes nothing, an SSE stream that drops
// rather than blocks — and none of that logic needs a broker to be true. So the
// Kafka-touching surface is pushed down to this package and named by the
// smallest interfaces the callers actually use.
//
// This package is consequently the LEAST unit-tested code in the repo, and that
// is a deliberate trade rather than an oversight: what remains here is almost
// entirely calls into franz-go, and a test that mocks franz-go to assert that
// this file calls franz-go proves nothing except that the mock was written to
// match. It is exercised end to end by ./run.sh, which is the level at which
// "the consumer group actually reports lag" is a real claim.
package kafkabus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/control"
)

// Record is one message lifted out of franz-go's types, so nothing downstream
// has to import kgo to read a message.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     []byte
	Timestamp time.Time
}

func lift(r *kgo.Record) Record {
	return Record{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       string(r.Key),
		Value:     r.Value,
		Timestamp: r.Timestamp,
	}
}

// Dial opens a client against brokers with the given options and blocks until
// the broker answers a metadata request or ctx expires.
//
// THE PING IS THE POINT. compose's service_healthy condition gets us a broker
// that has passed its own healthcheck, but a client that connects during a
// controller election still fails its first produce. Waiting for a successful
// round trip here turns a startup race into a startup delay.
func Dial(ctx context.Context, brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	opts = append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafkabus: new client: %w", err)
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafkabus: ping %v: %w", brokers, err)
	}
	return cl, nil
}

// DialRetry calls Dial until it succeeds or ctx expires, waiting `every`
// between attempts.
func DialRetry(ctx context.Context, log *slog.Logger, brokers []string, every time.Duration, opts ...kgo.Opt) (*kgo.Client, error) {
	var last error
	for {
		cl, err := Dial(ctx, brokers, opts...)
		if err == nil {
			return cl, nil
		}
		last = err
		log.Warn("broker not ready, retrying", "error", err, "retry_in", every)

		select {
		case <-ctx.Done():
			return nil, errors.Join(ctx.Err(), last)
		case <-time.After(every):
		}
	}
}

// EnsureTopics creates the lab's two topics if they are absent.
//
// `control` is COMPACTED and `events` is not, and that difference is the reason
// this function exists rather than relying on broker auto-creation: an
// auto-created control topic gets the broker's default delete-retention policy,
// the settings record ages out, and a lab restarted the next morning silently
// comes back on defaults. Auto-creation would work, right up until the one
// property the design depends on.
func EnsureTopics(ctx context.Context, cl *kgo.Client, partitions int32, log *slog.Logger) error {
	adm := kadm.NewClient(cl)

	specs := []struct {
		name       string
		partitions int32
		configs    map[string]*string
	}{
		{control.EventsTopic, partitions, map[string]*string{
			"retention.ms": ptr("600000"), // ten minutes: a lab, not an archive
		}},
		{control.Topic, 1, map[string]*string{
			"cleanup.policy": ptr("compact"),
			// Compact aggressively. The defaults would leave the settings
			// record uncompacted for hours, which is correct for a real
			// cluster and useless for a demo that wants to SHOW compaction.
			"min.cleanable.dirty.ratio": ptr("0.01"),
			"segment.ms":                ptr("60000"),
			"delete.retention.ms":       ptr("100"),
			"max.compaction.lag.ms":     ptr("60000"),
		}},
	}

	for _, s := range specs {
		_, err := adm.CreateTopic(ctx, s.partitions, 1, s.configs, s.name)

		// THE ORDER OF THESE TWO CASES IS THE WHOLE FUNCTION, and getting it
		// backwards is what made the first version of this file fail. See
		// isTopicAlreadyExists below: kadm.CreateTopic returns the TOPIC-LEVEL
		// error as its ordinary error return, so an `if err != nil` placed
		// first swallows the already-exists case and turns a successful no-op
		// into a fatal error.
		switch {
		case isTopicAlreadyExists(err):
			log.Info("topic already exists", "topic", s.name)
		case err != nil:
			return fmt.Errorf("kafkabus: create topic %s: %w", s.name, err)
		default:
			log.Info("topic created", "topic", s.name, "partitions", s.partitions, "configs", len(s.configs))
		}
	}
	return nil
}

func ptr(s string) *string { return &s }

// isTopicAlreadyExists reports whether err is Kafka's TOPIC_ALREADY_EXISTS.
//
// ── THE BUG THIS EXISTS TO PREVENT, WHICH SHIPPED AND WAS CAUGHT ON A COLD RUN
//
// `kadm.CreateTopic` (singular) ends with `return response, response.Err`. It
// returns the TOPIC-LEVEL error — the broker saying "that topic already
// exists" — through the SAME error return that carries transport failures. So
// the ordinary Go shape, written without reading kadm's source:
//
//	resp, err := adm.CreateTopic(...)
//	if err != nil {
//	    return fmt.Errorf(...)   // fires on an existing topic
//	}
//	switch {
//	case resp.Err == nil:            ...
//	case isTopicAlreadyExists(resp.Err): ...   // never reached
//	}
//
// …is wrong, and wrong in the most expensive way available: it compiles, it
// reads correctly, it PASSES EVERY TEST, and it passes every FIRST run, because
// on a fresh stack the topic does not exist and the guard is never taken. It
// fails the second time anyone runs `docker compose up` — topic-init exits 1,
// compose then refuses to start everything downstream of it, and the symptom
// presents three services away as "grafana will not start".
//
// The fix is the case ORDER in EnsureTopics above: ask "is this the benign
// already-exists answer?" before treating a non-nil error as fatal.
//
// ── WHY IT COMPARES THE CODE RATHER THAN THE VALUE
//
// A secondary point, and a hedge rather than a live bug. kadm builds this error
// with `kerr.ErrorForCode`, which today returns the package-level
// `kerr.TopicAlreadyExists` pointer, so `errors.Is` would in fact match. But
// `*kerr.Error` implements no `Is` method, so that match is POINTER IDENTITY —
// it holds only as long as kadm keeps handing back the shared value rather than
// constructing its own. Comparing `Code` is true for any *kerr.Error carrying
// that code, and survives a wrap. The tests pin both halves.
func isTopicAlreadyExists(err error) bool {
	var kerrErr *kerr.Error
	if !errors.As(err, &kerrErr) {
		return false
	}
	return kerrErr.Code == kerr.TopicAlreadyExists.Code
}
