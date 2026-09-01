package control

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestDefaultsAreBalancedAndInBounds(t *testing.T) {
	d := Defaults()
	if d.ProducerRatePerSec != d.ConsumerRatePerSec {
		t.Fatalf("defaults must start balanced so lag begins at zero: %v", d)
	}
	if Clamp(d) != d {
		t.Fatalf("defaults must already be in bounds; Clamp changed them: %v -> %v", d, Clamp(d))
	}
}

func TestClampBounds(t *testing.T) {
	cases := []struct {
		name string
		in   Settings
		want Settings
	}{
		{"below floor", Settings{0, 0, -5, 0}, Settings{MinRatePerSec, MinRatePerSec, MinWorkMillis, 0}},
		{"above ceiling", Settings{9999, 9999, 9999, 0}, Settings{MaxRatePerSec, MaxRatePerSec, MaxWorkMillis, 0}},
		{"inside", Settings{10, 20, 30, 7}, Settings{10, 20, 30, 7}},
		{"at bounds", Settings{MinRatePerSec, MaxRatePerSec, MaxWorkMillis, 0}, Settings{MinRatePerSec, MaxRatePerSec, MaxWorkMillis, 0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Clamp(c.in); got != c.want {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}

// The control topic is writable by anything on the compose network, so a
// hand-published record carrying NaN must not be able to wedge a service.
func TestClampRejectsNonFinite(t *testing.T) {
	d := Defaults()
	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		got := Clamp(Settings{ProducerRatePerSec: bad, ConsumerRatePerSec: bad})
		if got.ProducerRatePerSec != d.ProducerRatePerSec {
			t.Fatalf("producer %v: got %v want default %v", bad, got.ProducerRatePerSec, d.ProducerRatePerSec)
		}
		if got.ConsumerRatePerSec != d.ConsumerRatePerSec {
			t.Fatalf("consumer %v: got %v want default %v", bad, got.ConsumerRatePerSec, d.ConsumerRatePerSec)
		}
	}
}

func TestStampSetsTimeAndClamps(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	got := Stamp(Settings{ProducerRatePerSec: 9999, ConsumerRatePerSec: 0, ConsumerWorkMillis: 5}, now)
	if got.UpdatedAtUnixMilli != now.UnixMilli() {
		t.Fatalf("stamp: got %d want %d", got.UpdatedAtUnixMilli, now.UnixMilli())
	}
	if got.ProducerRatePerSec != MaxRatePerSec || got.ConsumerRatePerSec != MinRatePerSec {
		t.Fatalf("stamp must clamp: %v", got)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := Stamp(Settings{ProducerRatePerSec: 120, ConsumerRatePerSec: 3, ConsumerWorkMillis: 25}, time.UnixMilli(42))
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("round trip: got %v want %v", out, in)
	}
}

func TestEncodeIsStableJSON(t *testing.T) {
	b, err := Encode(Settings{ProducerRatePerSec: 1, ConsumerRatePerSec: 2, ConsumerWorkMillis: 3, UpdatedAtUnixMilli: 4})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for _, want := range []string{"producer_rate_per_sec", "consumer_rate_per_sec", "consumer_work_ms", "updated_at_unix_ms"} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("encoded record missing field %q: %s", want, b)
		}
	}
}

func TestDecodeRejectsGarbageAndClampsHostileValues(t *testing.T) {
	if _, err := Decode([]byte("{not json")); err == nil {
		t.Fatal("want error on malformed record")
	}
	got, err := Decode([]byte(`{"producer_rate_per_sec":1e9,"consumer_rate_per_sec":-4,"consumer_work_ms":100000}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := Settings{MaxRatePerSec, MinRatePerSec, MaxWorkMillis, 0}
	if got != want {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestWorkDelay(t *testing.T) {
	if got := (Settings{ConsumerWorkMillis: 25}).WorkDelay(); got != 25*time.Millisecond {
		t.Fatalf("got %v", got)
	}
	if got := (Settings{}).WorkDelay(); got != 0 {
		t.Fatalf("zero work: got %v", got)
	}
}

// A republished identical record must not read as a change, or every restart
// would log a settings update that changed nothing.
func TestEqualIgnoresUpdateStamp(t *testing.T) {
	a := Settings{10, 20, 30, 111}
	b := Settings{10, 20, 30, 999}
	if !a.Equal(b) {
		t.Fatal("stamp difference must not count as a change")
	}
	for _, diff := range []Settings{{11, 20, 30, 111}, {10, 21, 30, 111}, {10, 20, 31, 111}} {
		if a.Equal(diff) {
			t.Fatalf("%v must differ from %v", a, diff)
		}
	}
}

func TestStringNamesEveryKnob(t *testing.T) {
	s := Settings{ProducerRatePerSec: 12, ConsumerRatePerSec: 3, ConsumerWorkMillis: 7}.String()
	for _, want := range []string{"12.0", "3.0", "7ms", "producer", "consumer", "work"} {
		if !strings.Contains(s, want) {
			t.Fatalf("String() = %q, missing %q", s, want)
		}
	}
}

func TestTopicNamesAreDistinct(t *testing.T) {
	if Topic == EventsTopic {
		t.Fatal("the control topic and the measured topic must not be the same topic")
	}
}

func TestEncodeWrapsAMarshalFailure(t *testing.T) {
	orig := marshal
	t.Cleanup(func() { marshal = orig })
	marshal = func(any) ([]byte, error) { return nil, errBoom }

	_, err := Encode(Defaults())
	if err == nil {
		t.Fatal("want an error")
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("the wrap must keep the cause: %v", err)
	}
	if !strings.Contains(err.Error(), "control") {
		t.Fatalf("the wrap must name its package: %v", err)
	}
}

var errBoom = errors.New("boom")
