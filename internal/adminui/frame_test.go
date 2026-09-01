package adminui

import (
	"strings"
	"testing"
	"time"

	"github.com/pigfox/kafka-lab/internal/event"
	"github.com/pigfox/kafka-lab/internal/kafkabus"
)

func TestToFrameCarriesTheRecordCoordinates(t *testing.T) {
	value, err := event.Event{Seq: 9, Kind: "cart.updated", Region: "ap-south", AmountCents: 250,
		EmittedAtMilli: time.UnixMilli(1000).UnixMilli()}.JSON()
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	got := toFrame(kafkabus.Record{
		Topic: "events", Partition: 3, Offset: 100, Key: "ap-south", Value: value,
	}, time.UnixMilli(3000))

	if got.Topic != "events" || got.Partition != 3 || got.Offset != 100 || got.Key != "ap-south" {
		t.Fatalf("coordinates: %+v", got)
	}
	if !strings.Contains(got.Summary, "cart.updated") {
		t.Fatalf("summary: %q", got.Summary)
	}
	if got.AgeMillis != 2000 {
		t.Fatalf("age: got %d want 2000", got.AgeMillis)
	}
	if got.Raw != string(value) {
		t.Fatalf("raw: %q", got.Raw)
	}
}

// A RECORD THAT DOES NOT PARSE IS STILL SHOWN — hiding it would hide exactly
// the message an operator just injected from kafka-ui to see what happens.
func TestToFrameShowsUnparseableRecordsWithoutASummary(t *testing.T) {
	got := toFrame(kafkabus.Record{Topic: "events", Offset: 4, Value: []byte("{{{")}, time.Now())
	if got.Summary != "(unparsed)" {
		t.Fatalf("summary: %q", got.Summary)
	}
	if got.Raw != "{{{" {
		t.Fatalf("raw: %q", got.Raw)
	}
	if got.AgeMillis != 0 {
		t.Fatalf("an unparsed record has no known age, got %d", got.AgeMillis)
	}
}

// The tail sends every record to every open tab, so an operator running with a
// large KL_EVENT_FILLER_BYTES must not turn one curious browser into megabytes
// per second of SSE.
func TestToFrameBoundsTheRawPayload(t *testing.T) {
	big := strings.Repeat("x", maxRaw*4)
	got := toFrame(kafkabus.Record{Value: []byte(big)}, time.Now())
	if len(got.Raw) > maxRaw+32 {
		t.Fatalf("raw is %d bytes; the bound is %d", len(got.Raw), maxRaw)
	}
	if !strings.HasSuffix(got.Raw, "(truncated)") {
		t.Fatalf("a shortened payload must say so, got %q", got.Raw[len(got.Raw)-20:])
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Fatalf("got %q", got)
	}
	if got := truncate("exactlyten", 10); got != "exactlyten" {
		t.Fatalf("a payload at the bound must be untouched, got %q", got)
	}
	got := truncate("abcdefghijk", 10)
	if !strings.HasPrefix(got, "abcdefghij") || !strings.HasSuffix(got, "(truncated)") {
		t.Fatalf("got %q", got)
	}
}
