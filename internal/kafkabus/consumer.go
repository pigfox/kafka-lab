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
//
// ── WHY AUTOCOMMIT IS DISABLED, WHICH IS A CORRECTION AND NOT A PREFERENCE
//
// franz-go turns autocommit ON by default for any client in a consumer group
// and commits every five seconds. This file used to leave that default in
// place while ALSO committing explicitly at the end of each batch, so two
// independent things were moving the offset and only one of them was visible in
// the code.
//
// It never corrupted anything — an autocommit and an explicit commit record the
// same progress — but it made the explicit commit UNRELIABLE AS A BOUNDARY.
// The consume loop applies a record's effect and then commits, so the window
// between those two steps is where at-least-once lives: stop the process inside
// it and the record is redelivered. With a background timer also committing,
// that window silently closed itself roughly every five seconds, and whether a
// given interruption produced a redelivery depended on a race against a clock
// nothing in this repository controlled. Any experiment about duplicates would
// have been measuring the timer.
//
// Disabling it makes the commit points exactly the two the code names: the
// end of every batch, and the final commit on shutdown.
func ConsumerOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumeTopics(control.EventsTopic),
		kgo.ConsumerGroup(ConsumerGroup),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
	}
}
