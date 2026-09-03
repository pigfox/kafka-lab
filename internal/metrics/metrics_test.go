package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func names(t *testing.T, s *Set) map[string]bool {
	t.Helper()
	got, err := s.Registered()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	m := map[string]bool{}
	for _, n := range got {
		m[n] = true
	}
	return m
}

// A role must expose what its dashboard queries, and must NOT expose the other
// roles' series — a producer publishing a zero consumed_total would put a flat
// line on the panel that reads as "the consumer stopped".
func TestEachRoleExposesItsOwnSeriesAndNoOthers(t *testing.T) {
	cases := []struct {
		role Role
		want []string
		deny []string
	}{
		{RoleProducer,
			[]string{"kafka_lab_produced_total", "kafka_lab_rate_limit_per_second"},
			[]string{"kafka_lab_consumed_total", "kafka_lab_consumer_group_lag", "kafka_lab_work_milliseconds"}},
		{RoleConsumer,
			[]string{"kafka_lab_consumed_total", "kafka_lab_rate_limit_per_second", "kafka_lab_work_milliseconds"},
			[]string{"kafka_lab_produced_total", "kafka_lab_consumer_group_lag"}},
		{RoleAdmin,
			[]string{"kafka_lab_consumer_group_lag"},
			[]string{"kafka_lab_produced_total", "kafka_lab_consumed_total", "kafka_lab_rate_limit_per_second"}},
	}
	for _, c := range cases {
		t.Run(string(c.role), func(t *testing.T) {
			s := New(c.role)
			// Counters register at zero but only appear in a gather once
			// they or their siblings exist; touch every one this role owns.
			s.Errors.Add(0)
			s.ControlApplied.Add(0)
			s.Produced.Add(0)
			s.Consumed.Add(0)
			got := names(t, s)
			for _, w := range c.want {
				if !got[w] {
					t.Fatalf("%s must expose %s; has %v", c.role, w, got)
				}
			}
			for _, d := range c.deny {
				if got[d] {
					t.Fatalf("%s must not expose %s", c.role, d)
				}
			}
		})
	}
}

func TestEveryRoleExposesErrorsAndControlApplied(t *testing.T) {
	for _, role := range []Role{RoleProducer, RoleConsumer, RoleAdmin} {
		s := New(role)
		s.Errors.Add(0)
		s.ControlApplied.Add(0)
		got := names(t, s)
		for _, w := range []string{"kafka_lab_errors_total", "kafka_lab_control_applied_total"} {
			if !got[w] {
				t.Fatalf("%s must expose %s", role, w)
			}
		}
	}
}

// A private registry is what lets a test build a service's metrics twice in one
// process. If New ever reached for the default registerer this would panic.
func TestNewIsSafeToCallTwice(t *testing.T) {
	_ = New(RoleProducer)
	_ = New(RoleProducer)
}

// Metrics irrelevant to a role are still CONSTRUCTED, so a caller never has to
// nil-check before incrementing.
func TestUnregisteredMetricsAreStillUsable(t *testing.T) {
	s := New(RoleAdmin)
	for _, c := range []prometheus.Counter{s.Produced, s.Consumed} {
		if c == nil {
			t.Fatal("an unregistered counter must still be constructed")
		}
		c.Inc()
	}
	for _, g := range []prometheus.Gauge{s.RateLimit, s.WorkMillis} {
		if g == nil {
			t.Fatal("an unregistered gauge must still be constructed")
		}
		g.Set(1)
	}
}

func TestHandlerServesTheRegistry(t *testing.T) {
	s := New(RoleConsumer)
	s.Consumed.Add(3)
	s.RateLimit.Set(17)

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `kafka_lab_consumed_total{service="consumer"} 3`) {
		t.Fatalf("consumed_total missing or wrong:\n%s", body)
	}
	if !strings.Contains(body, `kafka_lab_rate_limit_per_second{service="consumer"} 17`) {
		t.Fatalf("rate_limit missing or wrong:\n%s", body)
	}
}

// The `service` label is what lets one Prometheus hold all three roles without
// their series colliding.
func TestServiceLabelDistinguishesRoles(t *testing.T) {
	for _, role := range []Role{RoleProducer, RoleConsumer, RoleAdmin} {
		s := New(role)
		s.Errors.Inc()
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		want := `kafka_lab_errors_total{service="` + string(role) + `"}`
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("%s: missing %s", role, want)
		}
	}
}

// The help text is where the measurement/setting split is written down for
// whoever opens the metrics page instead of the source.
func TestHelpTextNamesMeasurementOrSetting(t *testing.T) {
	s := New(RoleConsumer)
	s.Consumed.Add(0)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		"# HELP kafka_lab_consumed_total Records the consumer finished working on. A MEASUREMENT.",
		"A SETTING, not an achieved rate.",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing help line %q", want)
		}
	}
}

func TestRegisteredSurfacesAGatherFailure(t *testing.T) {
	s := New(RoleProducer)
	s.Registry.MustRegister(brokenCollector{})
	if _, err := s.Registered(); err == nil {
		t.Fatal("a failing collector must surface as an error, not an empty list")
	}
}

// brokenCollector reports a metric that does not match its own description,
// which is the one thing Gather refuses.
type brokenCollector struct{}

var brokenDesc = prometheus.NewDesc("kafka_lab_broken", "broken", []string{"label"}, nil)

func (brokenCollector) Describe(ch chan<- *prometheus.Desc) { ch <- brokenDesc }
func (brokenCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(brokenDesc, prometheus.GaugeValue, 1, "a", "b")
}

func TestNamespaceIsStable(t *testing.T) {
	// The Grafana dashboard and prometheus.yml are written against this
	// prefix; changing it silently reds every panel.
	if Namespace != "kafka_lab" {
		t.Fatalf("namespace changed to %q; the dashboard queries kafka_lab", Namespace)
	}
}

// The four delivery-semantics counters belong to the CONSUMER alone. A producer
// publishing a flat zero double_applied_total would put a reassuring line on a
// panel about a property it does not participate in.
func TestOnlyTheConsumerExposesTheDeliverySemanticsCounters(t *testing.T) {
	deliverySeries := []string{
		"kafka_lab_applied_total",
		"kafka_lab_duplicates_suppressed_total",
		"kafka_lab_double_applied_total",
		"kafka_lab_records_without_dedupe_key_total",
	}

	consumer := New(RoleConsumer)
	touchAll(consumer)
	got := names(t, consumer)
	for _, w := range deliverySeries {
		if !got[w] {
			t.Fatalf("the consumer must expose %s; has %v", w, got)
		}
	}

	for _, role := range []Role{RoleProducer, RoleAdmin} {
		s := New(role)
		touchAll(s)
		got := names(t, s)
		for _, d := range deliverySeries {
			if got[d] {
				t.Fatalf("%s must not expose %s", role, d)
			}
		}
	}
}

// The four outcomes are distinct series, so a dashboard cannot conflate the
// defect (double applied) with the fix (suppressed) or with the honest gap
// (no dedupe key).
func TestTheFourOutcomesAreSeparatelyReadable(t *testing.T) {
	s := New(RoleConsumer)
	s.Applied.Add(10)
	s.Suppressed.Add(4)
	s.DoubleApplied.Add(3)
	s.NoDedupeKey.Add(2)

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		`kafka_lab_applied_total{service="consumer"} 10`,
		`kafka_lab_duplicates_suppressed_total{service="consumer"} 4`,
		`kafka_lab_double_applied_total{service="consumer"} 3`,
		`kafka_lab_records_without_dedupe_key_total{service="consumer"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}

// Consumed counts records that went past; Applied counts effects that ran. They
// are different questions and must not be the same series.
func TestConsumedAndAppliedAreDistinctSeries(t *testing.T) {
	s := New(RoleConsumer)
	s.Consumed.Add(7)
	s.Applied.Add(9)

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	if !strings.Contains(body, `kafka_lab_consumed_total{service="consumer"} 7`) {
		t.Fatalf("consumed_total is wrong:\n%s", body)
	}
	if !strings.Contains(body, `kafka_lab_applied_total{service="consumer"} 9`) {
		t.Fatalf("applied_total is wrong:\n%s", body)
	}
}

// touchAll makes every counter in a set appear in a gather. Counters register
// at zero but only surface once touched.
func touchAll(s *Set) {
	for _, c := range []prometheus.Counter{
		s.Errors, s.ControlApplied, s.Produced, s.Consumed,
		s.Applied, s.Suppressed, s.DoubleApplied, s.NoDedupeKey,
	} {
		c.Add(0)
	}
}
