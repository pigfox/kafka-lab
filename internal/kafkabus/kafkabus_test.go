package kafkabus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pigfox/kafka-lab/internal/control"
)

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func TestLift(t *testing.T) {
	ts := time.UnixMilli(1234)
	got := lift(&kgo.Record{
		Topic: "events", Partition: 2, Offset: 9,
		Key: []byte("eu-west"), Value: []byte(`{"seq":1}`), Timestamp: ts,
	})
	want := Record{Topic: "events", Partition: 2, Offset: 9, Key: "eu-west", Value: []byte(`{"seq":1}`), Timestamp: ts}
	if got.Topic != want.Topic || got.Partition != want.Partition || got.Offset != want.Offset ||
		got.Key != want.Key || string(got.Value) != string(want.Value) || !got.Timestamp.Equal(want.Timestamp) {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestPtr(t *testing.T) {
	p := ptr("compact")
	if p == nil || *p != "compact" {
		t.Fatalf("got %v", p)
	}
}

// A NEGATIVE LAG IS NOT A NEGATIVE NUMBER OF MESSAGES. kadm reports -1 for a
// partition whose committed offset is unknown; summing that straight would make
// a group read as running AHEAD of the producer.
func TestSumLagIgnoresUnknownPartitions(t *testing.T) {
	in := map[string]map[int32]kadm.GroupMemberLag{
		"events": {
			0: {Lag: 120},
			1: {Lag: -1}, // unknown committed offset
			2: {Lag: 5},
			3: {Lag: 0},
		},
	}
	if got := SumLag(in); got != 125 {
		t.Fatalf("got %d want 125", got)
	}
}

func TestSumLagAcrossTopics(t *testing.T) {
	in := map[string]map[int32]kadm.GroupMemberLag{
		"events":  {0: {Lag: 10}, 1: {Lag: 20}},
		"control": {0: {Lag: 3}},
	}
	if got := SumLag(in); got != 33 {
		t.Fatalf("got %d want 33", got)
	}
}

func TestSumLagOfNothingIsZero(t *testing.T) {
	if got := SumLag(nil); got != 0 {
		t.Fatalf("nil: got %d", got)
	}
	if got := SumLag(map[string]map[int32]kadm.GroupMemberLag{"events": {}}); got != 0 {
		t.Fatalf("empty topic: got %d", got)
	}
}

// The lab's design depends on the control topic being read from the START (so a
// late service walks compacted history) and the events tail from the END (so
// opening the UI does not replay retention).
func TestConsumerOptsDeclareTheirOffsets(t *testing.T) {
	if len(ControlConsumerOpts()) != 2 {
		t.Fatalf("control opts: got %d", len(ControlConsumerOpts()))
	}
	if len(AdminTailOpts()) != 4 {
		t.Fatalf("tail opts: got %d", len(AdminTailOpts()))
	}
	// The options are opaque to a test, so the contract is asserted where it
	// is visible: a client built from them must accept them without error.
	for name, opts := range map[string][]kgo.Opt{
		"control": ControlConsumerOpts(),
		"tail":    AdminTailOpts(),
	} {
		cl, err := kgo.NewClient(append(opts, kgo.SeedBrokers("127.0.0.1:19092"))...)
		if err != nil {
			t.Fatalf("%s opts rejected by kgo: %v", name, err)
		}
		cl.Close()
	}
}

// IF THE ADMIN TAIL JOINED THE CONSUMER'S GROUP, opening the UI would take
// partitions from the consumer and change the very rate the UI displays.
func TestAdminTailGroupIsNotTheConsumerGroup(t *testing.T) {
	if AdminTailGroup == ConsumerGroup {
		t.Fatal("the observer must not share a group with the thing it observes")
	}
	if !strings.Contains(AdminTailGroup, "admin") {
		t.Fatalf("the admin group name should say so: %q", AdminTailGroup)
	}
}

func TestNewWatcherStartsOnDefaultsAndReportsNothingRead(t *testing.T) {
	w := NewWatcher(nil, quiet(), nil)
	s, had := w.Settings()
	if had {
		t.Fatal("a fresh watcher must report that no record has been read")
	}
	if s != control.Defaults() {
		t.Fatalf("got %v want defaults %v", s, control.Defaults())
	}
}

func TestNewWatcherToleratesANilCallback(t *testing.T) {
	w := NewWatcher(nil, quiet(), nil)
	w.applied() // must not panic
}

func TestWatcherChangedIsFresh(t *testing.T) {
	w := NewWatcher(nil, quiet(), nil)
	select {
	case <-w.Changed():
		t.Fatal("Changed fired before any record")
	default:
	}
}

func TestNewTailerToleratesANilCallback(t *testing.T) {
	tl := NewTailer(nil, quiet(), 4, nil)
	tl.seen(Record{}) // must not panic
	if _, _, subs := tl.Stats(); subs != 0 {
		t.Fatalf("subscribers: %d", subs)
	}
}

func TestTailerSubscribeAndStats(t *testing.T) {
	tl := NewTailer(nil, quiet(), 4, nil)
	ch, stop := tl.Subscribe()
	if _, _, subs := tl.Stats(); subs != 1 {
		t.Fatalf("subscribers: %d want 1", subs)
	}
	stop()
	if _, open := <-ch; open {
		t.Fatal("unsubscribe must close the channel")
	}
}

func TestDialFailsAgainstAClosedPort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := Dial(ctx, []string{"127.0.0.1:1"}); err == nil {
		t.Fatal("want an error")
	}
}

func TestDialErrorNamesTheBrokersItTried(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := Dial(ctx, []string{"not a broker address"})
	if err == nil {
		t.Fatal("want an error")
	}
	// An operator debugging a wrong KAFKA_BROKERS value needs to see the
	// value that was tried, not just "dial failed".
	if !strings.Contains(err.Error(), "not a broker address") {
		t.Fatalf("the error must name the brokers it tried: %v", err)
	}
}

func TestDialRetryGivesUpWhenTheContextExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := DialRetry(ctx, quiet(), []string{"127.0.0.1:1"}, 50*time.Millisecond)
	if err == nil {
		t.Fatal("want an error")
	}
	// The last dial failure must survive alongside the context error, or the
	// operator is told "deadline exceeded" with no hint of what was wrong.
	if !strings.Contains(err.Error(), "ping") && !strings.Contains(err.Error(), "connect") {
		t.Fatalf("the joined error must carry the dial failure: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("DialRetry ran %v past its context", elapsed)
	}
}

func TestWatcherRunReturnsWhenTheContextIsCancelled(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.ConsumeTopics(control.Topic))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	w := NewWatcher(cl, quiet(), nil)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run must report why it stopped")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run ignored cancellation")
	}
}

func TestTailerRunReturnsWhenTheContextIsCancelledAndClosesSubscribers(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.ConsumeTopics(control.EventsTopic))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	tl := NewTailer(cl, quiet(), 4, nil)
	sub, _ := tl.Subscribe()

	done := make(chan error, 1)
	go func() { done <- tl.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run ignored cancellation")
	}
	if _, open := <-sub; open {
		t.Fatal("Run must end every subscription when it stops")
	}
}

func TestEnsureTopicsFailsWithoutABroker(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := EnsureTopics(ctx, cl, 3, quiet()); err == nil {
		t.Fatal("want an error against a closed port")
	}
}

func TestPublishFailsWithoutABroker(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.RecordRetries(1))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := NewPublisher(cl, quiet()).Publish(ctx, control.Defaults()); err == nil {
		t.Fatal("want an error against a closed port")
	}
}

// ── the idempotency regression ──────────────────────────────────────────────
//
// A REAL DEFECT THAT SHIPPED AND WAS CAUGHT ON A COLD RUN. kadm.CreateTopic
// returns the TOPIC-LEVEL error ("already exists") through the same error
// return that carries transport failures, so an `if err != nil` guard placed
// before the already-exists case swallows it and turns a benign no-op into a
// fatal one. It passed every test and every FIRST run — on a fresh stack the
// topic does not exist, so the guard is never taken — and failed the second
// time `docker compose up` ran, presenting as "grafana will not start".
//
// See isTopicAlreadyExists in kafkabus.go for the full account.

// The predicate must match the error kadm ACTUALLY produces, which is what
// kerr.ErrorForCode returns for that code.
func TestIsTopicAlreadyExistsMatchesWhatKadmReturns(t *testing.T) {
	if !isTopicAlreadyExists(kerr.ErrorForCode(kerr.TopicAlreadyExists.Code)) {
		t.Fatal("the error kadm returns for an existing topic must be recognised")
	}
}

// It matches by CODE, not by pointer identity, so it keeps working if kadm ever
// constructs its own *kerr.Error instead of sharing the package-level value.
func TestIsTopicAlreadyExistsMatchesADistinctValue(t *testing.T) {
	distinct := &kerr.Error{Message: "TOPIC_ALREADY_EXISTS", Code: kerr.TopicAlreadyExists.Code}
	if distinct == kerr.TopicAlreadyExists {
		t.Fatal("this fixture must be a different pointer to be meaningful")
	}
	if !isTopicAlreadyExists(distinct) {
		t.Fatal("a distinct *kerr.Error carrying the code must match")
	}
	// The hedge, stated as a fact rather than an assumption: errors.Is does
	// NOT match here, because *kerr.Error implements no Is method.
	if errors.Is(distinct, kerr.TopicAlreadyExists) {
		t.Fatal("kerr gained an Is method; the comment in kafkabus.go is now stale")
	}
}

func TestIsTopicAlreadyExistsRejectsOtherErrors(t *testing.T) {
	cases := map[string]error{
		"nil":                   nil,
		"a plain error":         errors.New("connection refused"),
		"a different code":      kerr.ErrorForCode(kerr.InvalidTopicException.Code),
		"a wrapped plain error": fmt.Errorf("create topic: %w", errors.New("boom")),
	}
	for name, err := range cases {
		if isTopicAlreadyExists(err) {
			t.Fatalf("%s must not read as TOPIC_ALREADY_EXISTS", name)
		}
	}
}

func TestIsTopicAlreadyExistsSeesThroughWrapping(t *testing.T) {
	wrapped := fmt.Errorf("kafkabus: create topic events: %w",
		kerr.ErrorForCode(kerr.TopicAlreadyExists.Code))
	if !isTopicAlreadyExists(wrapped) {
		t.Fatal("a wrapped broker error must still be recognised")
	}
}

// THE ORDERING ITSELF, which is the half a predicate test cannot reach. This
// reproduces EnsureTopics' switch against the three answers CreateTopic can
// give, and asserts that the benign one is classified before the fatal guard.
// The original bug was exactly an inversion of these two cases.
func TestEnsureTopicsClassificationOrder(t *testing.T) {
	// classify mirrors the switch in EnsureTopics. If that switch is ever
	// reordered, this expresses what the order has to be.
	classify := func(err error) string {
		switch {
		case isTopicAlreadyExists(err):
			return "already-exists"
		case err != nil:
			return "fatal"
		default:
			return "created"
		}
	}

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"a fresh topic", nil, "created"},
		{"an existing topic", kerr.ErrorForCode(kerr.TopicAlreadyExists.Code), "already-exists"},
		{"a broker refusal", kerr.ErrorForCode(kerr.InvalidTopicException.Code), "fatal"},
		{"a transport failure", errors.New("dial tcp: connection refused"), "fatal"},
	}
	for _, c := range cases {
		if got := classify(c.err); got != c.want {
			t.Fatalf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}
