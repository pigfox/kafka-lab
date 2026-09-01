// Package control defines the settings the lab carries on its own bus.
//
// THE DEMO CONFIGURES ITSELF OVER KAFKA, AND THAT IS THE POINT. The admin
// service does not call the producer or the consumer over HTTP. It publishes a
// record to the compacted `control` topic; producer and consumer tail that
// topic and apply what arrives. Three consequences follow, and each is a thing
// the demo is meant to show:
//
//   - Settings SURVIVE RESTART with no database. Log compaction keeps the last
//     record for the key, so a container that comes back reads the topic from
//     offset 0 and arrives at the current state.
//   - There is no ordering problem between "admin is up" and "producer is up".
//     A service that starts late reads the history; a service that starts early
//     waits for the record.
//   - The control path is OBSERVABLE. Every setting change is a message you can
//     read in kafka-ui, which an HTTP PUT to a private endpoint is not.
//
// ONE KEY HOLDS THE WHOLE STATE. Splitting producer and consumer settings into
// two keys would be tidier to read and worse to reason about: compaction is
// per-key, so two keys means two independently-retained records and a reader
// that can observe a half-updated world. One key makes "the latest record IS
// the state" true by construction.
package control

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// Topic and Key name the single compacted record that holds lab state.
const (
	Topic = "control"
	Key   = "settings"

	// EventsTopic is the pipeline the lab actually measures. It lives here
	// so producer, consumer and admin cannot disagree about its name.
	EventsTopic = "events"
)

// Bounds. Both rates have a FLOOR OF ONE rather than zero, deliberately: a
// zero-rate token bucket does not throttle, it deadlocks, and a slider that can
// be dragged into a stall teaches the wrong lesson about backpressure. One
// message per second against a producer at fifty is already a 50:1 starvation
// and builds visible lag within seconds.
const (
	MinRatePerSec = 1.0
	MaxRatePerSec = 500.0
	MinWorkMillis = 0
	MaxWorkMillis = 500
)

// Settings is the full state of the lab's two adjustable knobs plus the
// consumer's simulated work delay.
type Settings struct {
	ProducerRatePerSec float64 `json:"producer_rate_per_sec"`
	ConsumerRatePerSec float64 `json:"consumer_rate_per_sec"`
	ConsumerWorkMillis int     `json:"consumer_work_ms"`
	UpdatedAtUnixMilli int64   `json:"updated_at_unix_ms"`
}

// Defaults are what the lab does before anyone touches a slider: both sides
// matched, so lag sits at zero and the first drag is what makes something
// happen.
func Defaults() Settings {
	return Settings{
		ProducerRatePerSec: 50,
		ConsumerRatePerSec: 50,
		ConsumerWorkMillis: 2,
	}
}

// Clamp forces s inside the declared bounds and replaces any non-finite value
// with the corresponding default.
//
// It is applied on BOTH SIDES of the bus — when admin publishes and again when
// producer or consumer applies. That is not belt-and-braces for its own sake:
// the topic is readable and writable by anything on the compose network
// (kafka-ui can publish to it by hand), so a consumer that trusted the record
// would take a rate of NaN from a typo in a browser form.
func Clamp(s Settings) Settings {
	d := Defaults()
	s.ProducerRatePerSec = clampFloat(s.ProducerRatePerSec, d.ProducerRatePerSec)
	s.ConsumerRatePerSec = clampFloat(s.ConsumerRatePerSec, d.ConsumerRatePerSec)
	if s.ConsumerWorkMillis < MinWorkMillis {
		s.ConsumerWorkMillis = MinWorkMillis
	}
	if s.ConsumerWorkMillis > MaxWorkMillis {
		s.ConsumerWorkMillis = MaxWorkMillis
	}
	return s
}

func clampFloat(v, def float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return def
	}
	if v < MinRatePerSec {
		return MinRatePerSec
	}
	if v > MaxRatePerSec {
		return MaxRatePerSec
	}
	return v
}

// Stamp returns s with its update time set, clamped.
func Stamp(s Settings, now time.Time) Settings {
	s = Clamp(s)
	s.UpdatedAtUnixMilli = now.UnixMilli()
	return s
}

// Encode renders s as the record value published to the control topic.
func Encode(s Settings) ([]byte, error) {
	b, err := marshal(s)
	if err != nil {
		return nil, fmt.Errorf("control: encode settings: %w", err)
	}
	return b, nil
}

// Decode parses a control record value. A malformed record is an error and the
// caller keeps the settings it already had — a bad message must not be able to
// reset the lab.
func Decode(b []byte) (Settings, error) {
	var s Settings
	if err := json.Unmarshal(b, &s); err != nil {
		return Settings{}, fmt.Errorf("control: decode settings: %w", err)
	}
	return Clamp(s), nil
}

// WorkDelay is the consumer's simulated per-message work.
func (s Settings) WorkDelay() time.Duration {
	return time.Duration(s.ConsumerWorkMillis) * time.Millisecond
}

// Equal reports whether two settings carry the same knob values. The update
// stamp is deliberately EXCLUDED: a republished identical record must not read
// as a change, or every restart would log a settings update that changed
// nothing.
func (s Settings) Equal(o Settings) bool {
	return s.ProducerRatePerSec == o.ProducerRatePerSec &&
		s.ConsumerRatePerSec == o.ConsumerRatePerSec &&
		s.ConsumerWorkMillis == o.ConsumerWorkMillis
}

// String renders the knobs for a log line.
func (s Settings) String() string {
	return fmt.Sprintf("producer=%.1f/s consumer=%.1f/s work=%dms",
		s.ProducerRatePerSec, s.ConsumerRatePerSec, s.ConsumerWorkMillis)
}

// marshal is a variable for the same reason event.marshal is: Settings has
// only scalar fields, so encoding/json cannot fail on it today, and a wrap that
// has never run has never been shown to say what it claims to say.
var marshal = json.Marshal
