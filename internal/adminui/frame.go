package adminui

import (
	"time"

	"github.com/pigfox/kafka-lab/internal/event"
	"github.com/pigfox/kafka-lab/internal/kafkabus"
)

// maxRaw bounds the raw payload a tail frame carries.
//
// The producer can be run with padded events (KL_EVENT_FILLER_BYTES), so a
// record is not necessarily small — and the tail sends every record to every
// open browser tab. An unbounded raw field turns one curious operator with a
// big filler setting into megabytes per second of SSE.
const maxRaw = 512

// toFrame renders one record for the live tail.
//
// A RECORD THAT DOES NOT PARSE IS STILL SHOWN. The control topic is writable by
// hand from kafka-ui and so, for that matter, is the events topic; a tail that
// silently swallowed anything it could not parse would hide exactly the message
// an operator had just injected to see what happens. Unparseable records get
// their raw bytes and no summary, which is honest about what is known.
func toFrame(r kafkabus.Record, now time.Time) tailFrame {
	f := tailFrame{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       r.Key,
		Raw:       truncate(string(r.Value), maxRaw),
	}

	e, err := event.Parse(r.Value)
	if err != nil {
		f.Summary = "(unparsed)"
		return f
	}
	f.Summary = e.Summary()
	f.AgeMillis = e.Age(now).Milliseconds()
	return f
}

// truncate cuts s to at most n bytes and says so, rather than cutting silently.
// A payload that ends mid-token with no marker reads as a corrupt message
// rather than a shortened one.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…(truncated)"
}
