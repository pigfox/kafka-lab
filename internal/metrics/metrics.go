// Package metrics declares what each service exposes on /metrics.
//
// EVERY FIGURE DECLARES WHETHER IT IS A MEASUREMENT OR A SETTING. That split is
// the one thing this package is careful about, because the lab shows both at
// once and they are easy to confuse on a graph:
//
//   - kafka_lab_produced_total and kafka_lab_consumed_total are MEASUREMENTS —
//     what actually happened.
//   - kafka_lab_producer_rate_limit and kafka_lab_consumer_rate_limit are
//     SETTINGS — what was ASKED for.
//
// They differ, and the difference is the demo. A consumer asked for 200/s that
// spends 20ms of simulated work on each message achieves 50/s, not 200/s, and a
// panel plotting the requested figure would render perfectly, stay internally
// consistent, and teach the reader something false. The metric names carry the
// distinction so a dashboard cannot quietly swap one for the other: a name
// ending _total is counted, a name ending _rate_limit is dialled.
//
// Every registry here is PRIVATE rather than prometheus.DefaultRegisterer. A
// private registry means a test can build a service's metrics twice in one
// process without a duplicate-registration panic, and it means /metrics carries
// this lab's series rather than the Go runtime's by accident.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every series this lab emits.
const Namespace = "kafka_lab"

// Set is one service's metrics plus the registry they live in.
type Set struct {
	Registry *prometheus.Registry

	// Produced counts records the producer handed to the broker and had
	// acknowledged. A record that failed to produce is counted in Errors and
	// NOT here — a produced_total that includes failures is not a throughput
	// measurement, it is an attempt count wearing a throughput name.
	Produced prometheus.Counter

	// Consumed counts records the consumer finished working on.
	Consumed prometheus.Counter

	// Errors counts failures on the service's hot path.
	Errors prometheus.Counter

	// RateLimit is the SETTING currently in force, in records per second.
	RateLimit prometheus.Gauge

	// WorkMillis is the consumer's simulated per-record work, a SETTING.
	WorkMillis prometheus.Gauge

	// Lag is the consumer group's total lag across partitions, published by
	// admin because admin is the only service that talks to the group
	// coordinator. It is a MEASUREMENT.
	Lag prometheus.Gauge

	// ControlApplied counts settings records this service has applied. It is
	// how you tell "the slider did nothing" from "the slider's message never
	// arrived" without reading a log.
	ControlApplied prometheus.Counter

	// ── delivery semantics ──────────────────────────────────────────────
	//
	// FOUR OUTCOMES, ALL MEASUREMENTS, AND EVERY RECORD LANDS IN EXACTLY ONE.
	// Consumed above says how many records went past, which cannot distinguish
	// a topic delivering every record twice from a topic carrying twice as many
	// records. These four can.

	// Applied counts records whose effect ran, first applications and repeats
	// alike. A MEASUREMENT.
	Applied prometheus.Counter

	// Suppressed counts records the idempotency store recognised as already
	// applied, so their effect did NOT run. It is zero unless dedupe is on.
	// A MEASUREMENT.
	Suppressed prometheus.Counter

	// DoubleApplied counts records whose effect ran for a key it had already
	// run for. It is the defect the dedupe store exists to remove, counted on
	// BOTH arms so the two are comparable, and it is a LOWER BOUND — a key
	// evicted from the bounded tally stops being recognisable as a repeat.
	// A MEASUREMENT.
	DoubleApplied prometheus.Counter

	// NoDedupeKey counts records that carried no identity header. They are
	// applied and counted here rather than dropped or quietly treated as
	// deduplicated. A MEASUREMENT.
	NoDedupeKey prometheus.Counter
}

// Role names which service a Set belongs to. It becomes the `service` label, so
// one Prometheus can hold all three without their series colliding.
type Role string

const (
	RoleProducer Role = "producer"
	RoleConsumer Role = "consumer"
	RoleAdmin    Role = "admin"
)

// New builds the metric set for a role. Metrics irrelevant to a role are still
// CONSTRUCTED but not registered, so a caller never has to nil-check before
// incrementing — a nil check on a metric is a branch that gets forgotten on the
// one path that matters.
func New(role Role) *Set {
	reg := prometheus.NewRegistry()
	labels := prometheus.Labels{"service": string(role)}

	s := &Set{
		Registry: reg,
		Produced: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "produced_total",
			Help:        "Records acknowledged by the broker. A MEASUREMENT.",
			ConstLabels: labels,
		}),
		Consumed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "consumed_total",
			Help:        "Records the consumer finished working on. A MEASUREMENT.",
			ConstLabels: labels,
		}),
		Errors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "errors_total",
			Help:        "Hot-path failures.",
			ConstLabels: labels,
		}),
		RateLimit: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "rate_limit_per_second",
			Help:        "Records per second currently ASKED for. A SETTING, not an achieved rate.",
			ConstLabels: labels,
		}),
		WorkMillis: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "work_milliseconds",
			Help:        "Simulated per-record work. A SETTING.",
			ConstLabels: labels,
		}),
		Lag: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "consumer_group_lag",
			Help:        "Total uncommitted records across the consumer group's partitions. A MEASUREMENT.",
			ConstLabels: labels,
		}),
		ControlApplied: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "control_applied_total",
			Help:        "Settings records read from the control topic and applied.",
			ConstLabels: labels,
		}),
		Applied: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "applied_total",
			Help:        "Records whose effect ran, repeats included. A MEASUREMENT.",
			ConstLabels: labels,
		}),
		Suppressed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "duplicates_suppressed_total",
			Help:        "Redeliveries the idempotency store recognised, whose effect did NOT run. A MEASUREMENT.",
			ConstLabels: labels,
		}),
		DoubleApplied: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "double_applied_total",
			Help:        "Records whose effect ran for a key it had already run for. A MEASUREMENT, and a LOWER BOUND.",
			ConstLabels: labels,
		}),
		NoDedupeKey: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "records_without_dedupe_key_total",
			Help:        "Records that carried no identity header. Applied, and counted here rather than dropped. A MEASUREMENT.",
			ConstLabels: labels,
		}),
	}

	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(s.Errors, s.ControlApplied)

	switch role {
	case RoleProducer:
		reg.MustRegister(s.Produced, s.RateLimit)
	case RoleConsumer:
		reg.MustRegister(s.Consumed, s.RateLimit, s.WorkMillis,
			s.Applied, s.Suppressed, s.DoubleApplied, s.NoDedupeKey)
	case RoleAdmin:
		reg.MustRegister(s.Lag)
	}
	return s
}

// Handler serves the set's registry.
func (s *Set) Handler() http.Handler {
	return promhttp.HandlerFor(s.Registry, promhttp.HandlerOpts{})
}

// Registered reports the metric names this set publishes. It exists so a test
// can assert that a role exposes what its dashboard queries, rather than a
// human checking a JSON file against a Go file by eye.
func (s *Set) Registered() ([]string, error) {
	families, err := s.Registry.Gather()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	return names, nil
}
