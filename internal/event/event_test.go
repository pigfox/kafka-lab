package event

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNextIsDeterministicForASeed(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	a := New(7, 0)
	b := New(7, 0)
	for i := 0; i < 50; i++ {
		if got, want := a.Next(now), b.Next(now); got != want {
			t.Fatalf("event %d diverged: %v vs %v", i, got, want)
		}
	}
}

func TestNextIncrementsSequenceAndStampsTime(t *testing.T) {
	now := time.UnixMilli(4242)
	g := New(1, 0)
	for i := uint64(1); i <= 5; i++ {
		e := g.Next(now)
		if e.Seq != i {
			t.Fatalf("seq: got %d want %d", e.Seq, i)
		}
		if e.EmittedAtMilli != 4242 {
			t.Fatalf("stamp: got %d", e.EmittedAtMilli)
		}
	}
	if got := g.Count(); got != 5 {
		t.Fatalf("count: got %d want 5", got)
	}
}

func TestNextDrawsFromTheDeclaredSets(t *testing.T) {
	g := New(99, 0)
	kinds := map[string]bool{}
	regions := map[string]bool{}
	for i := 0; i < 2000; i++ {
		e := g.Next(time.Now())
		if !contains(Kinds, e.Kind) {
			t.Fatalf("kind %q is outside the declared set", e.Kind)
		}
		if !contains(Regions, e.Region) {
			t.Fatalf("region %q is outside the declared set", e.Region)
		}
		if e.AmountCents < 100 || e.AmountCents > 50_100 {
			t.Fatalf("amount %d is outside the declared range", e.AmountCents)
		}
		kinds[e.Kind] = true
		regions[e.Region] = true
	}
	if len(kinds) != len(Kinds) {
		t.Fatalf("2000 draws covered %d of %d kinds", len(kinds), len(Kinds))
	}
	if len(regions) != len(Regions) {
		t.Fatalf("2000 draws covered %d of %d regions", len(regions), len(Regions))
	}
}

func TestFillerPadsAndNegativePaddingIsZero(t *testing.T) {
	g := New(1, 64)
	if got := len(g.Next(time.Now()).Filler); got != 64 {
		t.Fatalf("filler: got %d bytes want 64", got)
	}
	if got := New(1, 0).Next(time.Now()).Filler; got != "" {
		t.Fatalf("zero padding must add nothing, got %d bytes", len(got))
	}
	if got := New(1, -5).Next(time.Now()).Filler; got != "" {
		t.Fatalf("negative padding must add nothing, got %d bytes", len(got))
	}
}

// Partitioning by REGION is what makes the topic worth browsing in kafka-ui:
// keys repeat, so a partition carries related records.
func TestKeyIsTheRegion(t *testing.T) {
	e := Event{Region: "eu-west"}
	if string(e.Key()) != "eu-west" {
		t.Fatalf("got %q", e.Key())
	}
}

func TestJSONParseRoundTrip(t *testing.T) {
	in := New(3, 8).Next(time.UnixMilli(1234))
	b, err := in.JSON()
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	out, err := Parse(b)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if out != in {
		t.Fatalf("round trip: got %+v want %+v", out, in)
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	if _, err := Parse([]byte("<not json>")); err == nil {
		t.Fatal("want an error")
	}
}

func TestSummaryIsOneReadableLine(t *testing.T) {
	s := Event{Seq: 12, Kind: "order.paid", Region: "us-east", AmountCents: 4567}.Summary()
	if s != "#12 order.paid us-east 45.67" {
		t.Fatalf("got %q", s)
	}
	if strings.Contains(s, "\n") {
		t.Fatalf("summary must be one line: %q", s)
	}
}

func TestAge(t *testing.T) {
	now := time.UnixMilli(10_000)
	if got := (Event{EmittedAtMilli: 9_000}).Age(now); got != time.Second {
		t.Fatalf("got %v want 1s", got)
	}
	// An unstamped event has no age, rather than an age measured from 1970.
	if got := (Event{}).Age(now); got != 0 {
		t.Fatalf("unstamped: got %v want 0", got)
	}
	// Clock skew between producer and admin must not render as a negative age.
	if got := (Event{EmittedAtMilli: 11_000}).Age(now); got != 0 {
		t.Fatalf("future stamp: got %v want 0", got)
	}
}

func TestConcurrentNextIsSafeAndLosesNothing(t *testing.T) {
	g := New(5, 0)
	const workers, each = 8, 200
	done := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < each; j++ {
				g.Next(time.Now())
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < workers; i++ {
		<-done
	}
	if got := g.Count(); got != workers*each {
		t.Fatalf("count: got %d want %d", got, workers*each)
	}
}

func contains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}

// JSON marshalling of this struct cannot fail — every field is a scalar or a
// string — so the error branch has no reachable input. It is asserted here as
// a shape rather than exercised, and the branch stays because a future field
// (a map with non-string keys, say) would make it live.
func TestJSONSucceedsForEveryGeneratedEvent(t *testing.T) {
	g := New(11, 4)
	for i := 0; i < 100; i++ {
		if _, err := g.Next(time.Now()).JSON(); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
}

func TestJSONWrapsAMarshalFailureAndNamesTheSequence(t *testing.T) {
	orig := marshal
	t.Cleanup(func() { marshal = orig })
	marshal = func(any) ([]byte, error) { return nil, errBoom }

	_, err := Event{Seq: 77}.JSON()
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "77") {
		t.Fatalf("the wrap must name the sequence it failed on: %v", err)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("the wrap must keep the cause: %v", err)
	}
}

var errBoom = errors.New("boom")
