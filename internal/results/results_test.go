package results

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

const sampleScrape = `# HELP kafka_lab_applied_total Records whose effect ran.
# TYPE kafka_lab_applied_total counter
kafka_lab_applied_total{service="consumer"} 6094
kafka_lab_double_applied_total{service="consumer"} 1445
kafka_lab_dedupe_evictions_total{service="consumer",store="seen"} 0
kafka_lab_dedupe_evictions_total{service="consumer",store="applied"} 0
kafka_lab_ratio 0.5
`

func TestValueReadsALabelledSample(t *testing.T) {
	s, err := ReadScrape(write(t, "m.txt", sampleScrape))
	if err != nil {
		t.Fatalf("ReadScrape: %v", err)
	}
	got, err := s.Value(`kafka_lab_double_applied_total{service="consumer"}`)
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if got != 1445 {
		t.Fatalf("got %v want 1445", got)
	}
}

// A LABELLED SERIES MUST BE SELECTED BY ITS WHOLE LABEL SET. The two eviction
// samples differ only in the store label, and a prefix match on the bare metric
// name would return whichever came first.
func TestValueDistinguishesLabelSetsOfOneSeries(t *testing.T) {
	body := sampleScrape +
		`kafka_lab_dedupe_expiries_total{service="consumer",store="seen"} 3` + "\n" +
		`kafka_lab_dedupe_expiries_total{service="consumer",store="applied"} 7` + "\n"
	s, err := ReadScrape(write(t, "m.txt", body))
	if err != nil {
		t.Fatalf("ReadScrape: %v", err)
	}
	seen, err := s.Value(`kafka_lab_dedupe_expiries_total{service="consumer",store="seen"}`)
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	applied, err := s.Value(`kafka_lab_dedupe_expiries_total{service="consumer",store="applied"}`)
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if seen != 3 || applied != 7 {
		t.Fatalf("seen=%v applied=%v, want 3 and 7", seen, applied)
	}
}

// AN ABSENT SERIES IS AN ERROR, NEVER A ZERO. A scrape missing a counter and a
// scrape reporting zero mean opposite things, and a guard that conflated them
// would pass over a capture that had lost the counter it was checking.
func TestAnAbsentSeriesIsAnErrorRatherThanZero(t *testing.T) {
	s, err := ReadScrape(write(t, "m.txt", sampleScrape))
	if err != nil {
		t.Fatalf("ReadScrape: %v", err)
	}
	if _, err := s.Value(`kafka_lab_nothing_total{service="consumer"}`); err == nil {
		t.Fatal("an absent series returned a value")
	} else if !strings.Contains(err.Error(), "absent and zero are not the same answer") {
		t.Fatalf("the error does not explain itself: %v", err)
	}
}

// A prefix of a longer series name must not match it, or applied_total would
// answer for double_applied_total.
func TestASelectorDoesNotMatchALongerSeriesName(t *testing.T) {
	s, err := ReadScrape(write(t, "m.txt", sampleScrape))
	if err != nil {
		t.Fatalf("ReadScrape: %v", err)
	}
	got, err := s.Value(`kafka_lab_applied_total{service="consumer"}`)
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if got != 6094 {
		t.Fatalf("got %v want 6094 — a longer series name was matched", got)
	}
}

func TestIntRejectsANonWholeValue(t *testing.T) {
	s, err := ReadScrape(write(t, "m.txt", sampleScrape))
	if err != nil {
		t.Fatalf("ReadScrape: %v", err)
	}
	if _, err := s.Int("kafka_lab_ratio"); err == nil {
		t.Fatal("0.5 was accepted as a record count")
	}
	got, err := s.Int(`kafka_lab_applied_total{service="consumer"}`)
	if err != nil {
		t.Fatalf("Int: %v", err)
	}
	if got != 6094 {
		t.Fatalf("got %d want 6094", got)
	}
}

func TestIntSurfacesAnAbsentSeries(t *testing.T) {
	s, err := ReadScrape(write(t, "m.txt", sampleScrape))
	if err != nil {
		t.Fatalf("ReadScrape: %v", err)
	}
	if _, err := s.Int("kafka_lab_nothing"); err == nil {
		t.Fatal("an absent series returned an int")
	}
}

func TestAnUnparseableValueIsAnError(t *testing.T) {
	s, err := ReadScrape(write(t, "m.txt", "kafka_lab_broken not_a_number\n"))
	if err != nil {
		t.Fatalf("ReadScrape: %v", err)
	}
	if _, err := s.Value("kafka_lab_broken"); err == nil {
		t.Fatal("a non-numeric sample parsed")
	}
}

func TestReadScrapeRejectsAMissingFile(t *testing.T) {
	if _, err := ReadScrape(filepath.Join(t.TempDir(), "absent.txt")); err == nil {
		t.Fatal("a missing capture read cleanly")
	}
}

// A capture with only comments carries no evidence; treating it as an empty but
// valid scrape would let every guard pass vacuously.
func TestReadScrapeRejectsACaptureWithNoSamples(t *testing.T) {
	if _, err := ReadScrape(write(t, "m.txt", "# HELP only\n# TYPE only counter\n\n")); err == nil {
		t.Fatal("a scrape with no samples was accepted")
	}
}

const sampleLog = `time=2026-09-03T23:18:04.367Z level=INFO msg="fault injected: rewinding instead of committing" key=pf-s313-graded:251 partition=2 offset=131 epoch=0
time=2026-09-03T23:18:04.483Z level=INFO msg=redelivery key=pf-s313-graded:251 partition=2 offset=131 suppressed=false
time=2026-09-03T23:18:04.484Z level=INFO msg=redelivery key=pf-s313-graded:252 partition=2 offset=132 suppressed=false
time=2026-09-03T23:18:05.001Z level=INFO msg="fault injected: rewinding instead of committing" key=pf-s313-graded:900 partition=1 offset=44 epoch=0
`

func TestCountLinesCountsOnlyMatchingLines(t *testing.T) {
	path := write(t, "c.log", sampleLog)
	got, err := CountLines(path, "fault injected")
	if err != nil {
		t.Fatalf("CountLines: %v", err)
	}
	if got != 2 {
		t.Fatalf("got %d want 2", got)
	}
	got, err = CountLines(path, "msg=redelivery")
	if err != nil {
		t.Fatalf("CountLines: %v", err)
	}
	if got != 2 {
		t.Fatalf("got %d want 2", got)
	}
}

func TestCountLinesOfSomethingAbsentIsZero(t *testing.T) {
	got, err := CountLines(write(t, "c.log", sampleLog), "forgot a key")
	if err != nil {
		t.Fatalf("CountLines: %v", err)
	}
	if got != 0 {
		t.Fatalf("got %d want 0", got)
	}
}

func TestCountLinesRejectsAMissingFile(t *testing.T) {
	if _, err := CountLines(filepath.Join(t.TempDir(), "absent.log"), "x"); err == nil {
		t.Fatal("a missing log counted cleanly")
	}
}

func TestFieldValuesReadsTheKeyOfEveryMatchingLine(t *testing.T) {
	got, err := FieldValues(write(t, "c.log", sampleLog), "fault injected", "key")
	if err != nil {
		t.Fatalf("FieldValues: %v", err)
	}
	want := []string{"pf-s313-graded:251", "pf-s313-graded:900"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

// A field at the end of a line has no trailing space to stop at.
func TestFieldValuesReadsAFieldAtTheEndOfALine(t *testing.T) {
	got, err := FieldValues(write(t, "c.log", sampleLog), "msg=redelivery", "suppressed")
	if err != nil {
		t.Fatalf("FieldValues: %v", err)
	}
	for _, v := range got {
		if v != "false" {
			t.Fatalf("got %q", v)
		}
	}
}

// A matched line missing the field is an ERROR rather than a silent skip: it
// means the log format moved, and quietly returning fewer keys would shrink a
// fault set without saying so.
func TestFieldValuesRefusesAMatchedLineWithNoSuchField(t *testing.T) {
	body := `time=2026-09-03T23:18:04Z level=INFO msg="fault injected" partition=2 offset=131` + "\n"
	if _, err := FieldValues(write(t, "c.log", body), "fault injected", "key"); err == nil {
		t.Fatal("a matched line with no key= was skipped silently")
	}
}

func TestFieldValuesRejectsAMissingFile(t *testing.T) {
	if _, err := FieldValues(filepath.Join(t.TempDir(), "absent.log"), "x", "key"); err == nil {
		t.Fatal("a missing log read cleanly")
	}
}

func TestFieldValuesOfNothingIsEmpty(t *testing.T) {
	got, err := FieldValues(write(t, "c.log", sampleLog), "no such message", "key")
	if err != nil {
		t.Fatalf("FieldValues: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

// The field lookup requires a leading space, so `key=` cannot be matched inside
// a longer field name such as `dedupe_key=`.
func TestFieldDoesNotMatchASuffixOfALongerFieldName(t *testing.T) {
	body := `time=x level=INFO msg=redelivery dedupe_key=wrong key=right suppressed=true` + "\n"
	got, err := FieldValues(write(t, "c.log", body), "msg=redelivery", "key")
	if err != nil {
		t.Fatalf("FieldValues: %v", err)
	}
	if len(got) != 1 || got[0] != "right" {
		t.Fatalf("got %v want [right]", got)
	}
}

func TestFieldReportsAnEmptyValueAsAbsent(t *testing.T) {
	if _, ok := field("msg=redelivery key= partition=1", "key"); ok {
		t.Fatal("an empty value was reported as present")
	}
}
