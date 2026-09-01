package kafkabus

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/control"
)

// ConsumerGroup is the group the working consumer joins — the one whose lag the
// dashboard plots. It is named here rather than in the consumer binary because
// ADMIN ALSO NEEDS IT: admin asks the coordinator for this group's lag, and a
// group name duplicated across two binaries is a lag panel that silently reads
// zero the day one of them is edited.
const ConsumerGroup = "kafka-lab-consumer"

// ConsumerOpts are the client options the working consumer needs.
//
// OFFSETS ARE COMMITTED — unlike the admin tail's — because this group's
// progress IS the measurement. Lag is the distance between the log end and the
// committed offset, so a consumer that never committed would report the full
// topic as lag forever and the drain would never be visible.
//
// It starts AtEnd rather than AtStart so a restarted lab does not open on ten
// minutes of retained backlog, which would read as enormous lag that the
// operator did not cause and cannot explain.
func ConsumerOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumeTopics(control.EventsTopic),
		kgo.ConsumerGroup(ConsumerGroup),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	}
}
