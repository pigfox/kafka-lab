package kafkabus

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// LagReader asks the group coordinator how far behind a consumer group is.
//
// ADMIN IS THE ONLY SERVICE THAT DOES THIS, and it does it rather than the
// consumer reporting its own lag. A consumer publishing its own lag would be a
// process reporting on the thing it is itself the cause of — and worse, a
// consumer that has fallen over reports nothing, so the lag would go BLANK at
// exactly the moment it went infinite. Asking the coordinator gives an answer
// that survives the consumer's absence.
type LagReader struct {
	adm   *kadm.Client
	group string
}

// NewLagReader returns a reader for group over cl.
func NewLagReader(cl *kgo.Client, group string) *LagReader {
	return &LagReader{adm: kadm.NewClient(cl), group: group}
}

// Total returns the group's summed lag across every partition it is assigned.
//
// A group that has never committed has no lag to report, and that returns ZERO
// rather than an error: at boot the consumer has not committed yet, and an
// admin UI showing an error for the first few seconds of every run is an admin
// UI whose errors nobody reads.
func (l *LagReader) Total(ctx context.Context) (int64, error) {
	lags, err := l.adm.Lag(ctx, l.group)
	if err != nil {
		return 0, fmt.Errorf("kafkabus: describe lag for %s: %w", l.group, err)
	}

	described, ok := lags[l.group]
	if !ok {
		return 0, nil
	}
	if err := described.Error(); err != nil {
		return 0, fmt.Errorf("kafkabus: lag for %s: %w", l.group, err)
	}

	return SumLag(described.Lag), nil
}

// SumLag adds up per-partition lag, ignoring partitions whose lag is unknown.
//
// A NEGATIVE LAG IS NOT A NEGATIVE NUMBER OF MESSAGES. kadm reports -1 for a
// partition whose committed offset is unknown, and summing that straight would
// make a group with three unknown partitions read as -3 messages behind, which
// on a graph looks like the consumer running ahead of the producer. It is split
// out from Total so the arithmetic is testable without a broker.
func SumLag(byTopic map[string]map[int32]kadm.GroupMemberLag) int64 {
	var total int64
	for _, partitions := range byTopic {
		for _, l := range partitions {
			if l.Lag > 0 {
				total += l.Lag
			}
		}
	}
	return total
}
