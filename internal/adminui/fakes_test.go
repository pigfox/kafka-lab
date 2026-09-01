package adminui

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/fanout"
	"github.com/pigfox/kafka-lab/internal/kafkabus"
)

var errFake = errors.New("fake failure")

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// fakePublisher records what admin published, which is how the tests assert
// that a slider goes over THE BUS rather than anywhere else.
type fakePublisher struct {
	mu        sync.Mutex
	published []control.Settings
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, s control.Settings) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, s)
	return nil
}

func (f *fakePublisher) last(t *testing.T) control.Settings {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.published) == 0 {
		t.Fatal("nothing was published to the control topic")
	}
	return f.published[len(f.published)-1]
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

type fakeSettings struct {
	mu  sync.Mutex
	s   control.Settings
	had bool
}

func (f *fakeSettings) Settings() (control.Settings, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.s, f.had
}

func (f *fakeSettings) set(s control.Settings) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.s, f.had = s, true
}

type fakeRates struct {
	produced, consumed       float64
	producedErr, consumedErr error
}

func (f *fakeRates) ProducedPerSec(context.Context) (float64, error) {
	return f.produced, f.producedErr
}
func (f *fakeRates) ConsumedPerSec(context.Context) (float64, error) {
	return f.consumed, f.consumedErr
}

type fakeLag struct {
	total int64
	err   error
}

func (f *fakeLag) Total(context.Context) (int64, error) { return f.total, f.err }

// fakeTail is the real fanout.Broadcast behind the TailSource interface, so the
// SSE tests exercise the actual drop-rather-than-block policy.
type fakeTail struct {
	b *fanout.Broadcast[kafkabus.Record]
}

func newFakeTail(buf int) *fakeTail {
	return &fakeTail{b: fanout.NewBroadcast[kafkabus.Record](buf)}
}
func (f *fakeTail) Subscribe() (<-chan kafkabus.Record, func())    { return f.b.Subscribe() }
func (f *fakeTail) Stats() (sent, dropped uint64, subscribers int) { return f.b.Stats() }
func (f *fakeTail) send(r kafkabus.Record)                         { f.b.Send(r) }
func (f *fakeTail) close()                                         { f.b.Close() }

// harness is a Server plus handles on every fake behind it.
type harness struct {
	srv       *Server
	publisher *fakePublisher
	settings  *fakeSettings
	rates     *fakeRates
	lag       *fakeLag
	tail      *fakeTail
}

func newHarness(t *testing.T, mutate ...func(*Options)) *harness {
	t.Helper()
	h := &harness{
		publisher: &fakePublisher{},
		settings:  &fakeSettings{s: control.Defaults()},
		rates:     &fakeRates{},
		lag:       &fakeLag{},
		tail:      newFakeTail(8),
	}
	o := Options{
		Log:       quiet(),
		Publisher: h.publisher,
		Settings:  h.settings,
		Rates:     h.rates,
		Lag:       h.lag,
		Tail:      h.tail,
		Links: Links{
			Grafana:    "http://localhost:18081",
			Prometheus: "http://localhost:18082",
			KafkaUI:    "http://localhost:18083",
		},
		TailKeepalive: 50 * time.Millisecond,
	}
	for _, m := range mutate {
		m(&o)
	}
	srv, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.srv = srv
	return h
}
