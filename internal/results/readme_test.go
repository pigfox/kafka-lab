package results

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The published run and the document that quotes it.
const (
	readmePath = "../../README.md"
	captureDir = "../../results/pf-s313/capture"
)

const (
	offMetrics  = captureDir + "/arm-off-metrics.txt"
	onMetrics   = captureDir + "/arm-on-metrics.txt"
	offLog      = captureDir + "/arm-off-consumer.log"
	onLog       = captureDir + "/arm-on-consumer.log"
	offFaultSet = captureDir + "/arm-off-faultset.txt"
	onFaultSet  = captureDir + "/arm-on-faultset.txt"

	// resultsSummary is the run's write-up. It is the only record of the
	// SIZING run, which produced no capture of its own.
	resultsSummary = "../../results/pf-s313/results.md"
)

func consumerSeries(name string) string {
	return fmt.Sprintf("kafka_lab_%s{service=\"consumer\"}", name)
}

func lossSeries(name, store string) string {
	return fmt.Sprintf("kafka_lab_%s{service=\"consumer\",store=%q}", name, store)
}

func readme(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("README unreadable: %v", err)
	}
	return string(raw)
}

func scrape(t *testing.T, path string) *Scrape {
	t.Helper()
	s, err := ReadScrape(path)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return s
}

func metric(t *testing.T, s *Scrape, selector string) int {
	t.Helper()
	n, err := s.Int(selector)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return n
}

func lines(t *testing.T, path, substr string) int {
	t.Helper()
	n, err := CountLines(path, substr)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return n
}

// ── THE DRIFT GUARD ────────────────────────────────────────────────────────
//
// EVERY FIGURE THE README PUBLISHES IS RE-DERIVED FROM THE COMMITTED CAPTURE
// AND REQUIRED TO APPEAR VERBATIM. Numbers typed into prose beside files nobody
// re-reads is how a document comes to describe a run that never happened, and
// the drift is invisible because both halves look fine on their own.
//
// EACH FIGURE IS BOUND TO ITS OWN ROW, not merely required to appear somewhere.
//
// The first draft of this guard asserted `strings.Contains(readme, "1948")` per
// figure, and it was unfalsifiable in the way that matters: 1948 is both the
// double-apply count and the redelivery count, so swapping the two — or typing
// 1950 into one cell while the other still read 1948 — passed. Planting that
// exact edit is what exposed it. A guard over a table has to read the table.
func TestEveryPublishedFigureMatchesTheCommittedCapture(t *testing.T) {
	off, on := scrape(t, offMetrics), scrape(t, onMetrics)
	rows := publishedTable(t, readme(t))

	want := []struct {
		label   string
		off, on int
	}{
		{"records the loop finished",
			metric(t, off, consumerSeries("consumed_total")),
			metric(t, on, consumerSeries("consumed_total"))},
		{"effects run",
			metric(t, off, consumerSeries("applied_total")),
			metric(t, on, consumerSeries("applied_total"))},
		{"**applied a second time**",
			metric(t, off, consumerSeries("double_applied_total")),
			metric(t, on, consumerSeries("double_applied_total"))},
		{"duplicates suppressed",
			metric(t, off, consumerSeries("duplicates_suppressed_total")),
			metric(t, on, consumerSeries("duplicates_suppressed_total"))},
		{"keys faulted",
			lines(t, offLog, `msg="fault fired"`),
			lines(t, onLog, `msg="fault fired"`)},
		{"cursor rewinds",
			lines(t, offLog, "rewinding instead of committing"),
			lines(t, onLog, "rewinding instead of committing")},
		{"redeliveries in the log",
			lines(t, offLog, "msg=redelivery"),
			lines(t, onLog, "msg=redelivery")},
		{"store evictions",
			metric(t, off, lossSeries("dedupe_evictions_total", "seen")),
			metric(t, on, lossSeries("dedupe_evictions_total", "seen"))},
		{"store expiries",
			metric(t, off, lossSeries("dedupe_expiries_total", "seen")),
			metric(t, on, lossSeries("dedupe_expiries_total", "seen"))},
	}

	for _, w := range want {
		got, published := rows[w.label]
		if !published {
			t.Errorf("the README's measured table has no row %q", w.label)
			continue
		}
		if got.off != fmt.Sprint(w.off) {
			t.Errorf("row %q, at-least-once arm: README says %q, the capture says %d",
				w.label, got.off, w.off)
		}
		if got.on != fmt.Sprint(w.on) {
			t.Errorf("row %q, idempotent arm: README says %q, the capture says %d",
				w.label, got.on, w.on)
		}
	}

	if len(rows) != len(want) {
		t.Errorf("the measured table has %d rows but %d are checked; an unchecked row can drift freely",
			len(rows), len(want))
	}
}

// ── THE LEDE'S FIGURES ARE BOUND TOO ───────────────────────────────────────
//
// THE TABLE GUARD ABOVE CANNOT SEE PROSE, and the section's most consequential
// claim now lives in prose: the tail multiplier, promoted above the table
// because "size for the fault rate" is the mistake a reader is most likely to
// make. A figure quoted in a lede and derived nowhere is exactly the drift this
// package exists to prevent — worse than a wrong table cell, because a reader
// takes a lede on trust.
//
// Both figures are DERIVED, neither is typed:
//
//   - 35× comes from the committed capture — double applies over keys fired.
//   - 4.5× comes from results.md's own record of the sizing run, which produced
//     no capture of its own. It is extracted from that file rather than
//     transcribed, so the README and the results write-up cannot disagree about
//     what the sizing run did.
func TestTheLedeFiguresAreDerivedFromTheEvidence(t *testing.T) {
	doc := readme(t)
	// FLATTENED FIRST. This is prose, so it is hard-wrapped, and a phrase the
	// guard looks for lands across a newline as often as not. Searching the raw
	// bytes would make the guard fail on rewrapping and pass on a deletion that
	// happened to leave the words on one line — noise in one direction and a
	// hole in the other.
	lede := flatten(ledeSection(t, doc))

	off := scrape(t, offMetrics)
	doubles := metric(t, off, consumerSeries("double_applied_total"))
	fired := lines(t, offLog, `msg="fault fired"`)
	if fired == 0 {
		t.Fatal("the capture records no fired keys; the multiplier is undefined")
	}

	graded := fmt.Sprintf("**%d duplicate applications per fault**",
		int(math.Round(float64(doubles)/float64(fired))))
	if !strings.Contains(lede, graded) {
		t.Errorf("the lede does not carry %q, derived from %d double applies over %d fired keys:\n%s",
			graded, doubles, fired, lede)
	}

	sizingFaults, sizingDuplicates := sizingRun(t)
	sizing := fmt.Sprintf("**%s×**", trimFloat(float64(sizingDuplicates)/float64(sizingFaults)))
	if !strings.Contains(lede, sizing) {
		t.Errorf("the lede does not carry %q, derived from the sizing run's %d faults and %d duplicates:\n%s",
			sizing, sizingFaults, sizingDuplicates, lede)
	}

	// The claim the figures exist to support. Without it the two numbers are
	// trivia; with it they are a sizing rule.
	for _, phrase := range []string{
		"wrong by more than an order of magnitude",
		"broker batch composition",
	} {
		if !strings.Contains(lede, phrase) {
			t.Errorf("the lede no longer states %q:\n%s", phrase, lede)
		}
	}
}

// THE OLD PLACEMENTS MUST STAY GONE. The finding used to be split — a bare
// order-of-magnitude claim above the table with no figure, and the 35× figure
// below it. Restating either would put the claim in two places, which is how
// two copies of a number start disagreeing.
func TestTheTailMultiplierIsStatedInExactlyOnePlace(t *testing.T) {
	doc := flatten(readme(t))
	if got := strings.Count(doc, "order of magnitude"); got != 1 {
		t.Errorf("the order-of-magnitude claim appears %d times; it belongs in the lede alone", got)
	}
	if got := strings.Count(doc, "duplicate applications per fault"); got != 1 {
		t.Errorf("the per-fault multiplier appears %d times; it belongs in the lede alone", got)
	}
}

// ledeSection returns the tail-multiplier section: from its heading to the next
// one. Bounding it matters — a figure found anywhere in a 380-line README is
// not evidence that the lede carries it.
func ledeSection(t *testing.T, doc string) string {
	t.Helper()
	const heading = "### A rewind redelivers a tail"
	start := strings.Index(doc, heading)
	if start < 0 {
		t.Fatalf("the README carries no %q section", heading)
	}
	rest := doc[start+len(heading):]
	if end := strings.Index(rest, "\n### "); end >= 0 {
		rest = rest[:end]
	}
	if strings.TrimSpace(rest) == "" {
		t.Fatal("the lede section is empty; this guard would pass vacuously")
	}
	return rest
}

// sizingRun reads the sizing run's faults and duplicates out of results.md.
//
// THAT RUN PRODUCED NO CAPTURE — it was the dry run that sized the graded one —
// so results.md is the only record of it, and this reads that record rather
// than letting the README carry a second copy of the numbers.
func sizingRun(t *testing.T) (faults, duplicates int) {
	t.Helper()
	raw, err := os.ReadFile(resultsSummary)
	if err != nil {
		t.Fatalf("the results summary is unreadable: %v", err)
	}
	m := sizingPattern.FindStringSubmatch(string(raw))
	if m == nil {
		t.Fatalf("%s no longer records the sizing run in the expected form", resultsSummary)
	}
	return atoi(t, m[1]), atoi(t, m[2])
}

var sizingPattern = regexp.MustCompile(`(\d+) faults producing (\d+)\s+duplicates`)

// flatten collapses every run of whitespace to one space, so a hard-wrapped
// sentence reads as one line.
func flatten(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// trimFloat renders a ratio the way the prose does: no trailing zeros.
func trimFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

type publishedRow struct{ off, on string }

// publishedTable reads the measured-results table out of the README. It is
// found by its header rather than by line number, so the guard survives the
// section moving.
func publishedTable(t *testing.T, doc string) map[string]publishedRow {
	t.Helper()

	const header = "| | at-least-once (`KL_DEDUPE=false`) | idempotent (`KL_DEDUPE=true`) |"
	start := strings.Index(doc, header)
	if start < 0 {
		t.Fatalf("the README carries no measured-results table with the expected header")
	}

	rows := map[string]publishedRow{}
	for _, line := range strings.Split(doc[start:], "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			break // the table has ended
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) != 3 {
			continue
		}
		label := strings.TrimSpace(cells[0])
		if label == "" || strings.HasPrefix(label, "---") {
			continue // the header row and the separator
		}
		rows[label] = publishedRow{
			off: strings.Trim(strings.TrimSpace(cells[1]), "*"),
			on:  strings.Trim(strings.TrimSpace(cells[2]), "*"),
		}
	}
	if len(rows) == 0 {
		t.Fatal("the measured-results table parsed to no rows; this guard would pass vacuously")
	}
	return rows
}

// ── THE INVARIANTS THE README ASSERTS, CHECKED AGAINST THE CAPTURE ─────────

// D5: a run whose store overflowed measures the store, not the delivery
// semantics. If either loss counter is non-zero on either arm, the published
// number is not publishable and this fails.
func TestNeitherArmLostAKeyFromTheIdempotencyStore(t *testing.T) {
	for name, path := range map[string]string{"off": offMetrics, "on": onMetrics} {
		s := scrape(t, path)
		for _, series := range []string{"dedupe_evictions_total", "dedupe_expiries_total"} {
			for _, store := range []string{"seen", "applied"} {
				if got := metric(t, s, lossSeries(series, store)); got != 0 {
					t.Fatalf("arm %s: %s{store=%q} is %d; the published figures are not publishable",
						name, series, store, got)
				}
			}
		}
	}
}

// D6: a zero double-apply count on the at-least-once arm means the injector
// produced no redelivery and every other figure is measuring nothing.
func TestTheAtLeastOnceArmActuallyDoubleApplied(t *testing.T) {
	off := scrape(t, offMetrics)
	if got := metric(t, off, consumerSeries("double_applied_total")); got == 0 {
		t.Fatal("the at-least-once arm double-applied nothing; the fault mechanism does not work")
	}
}

// The exact equality the directive's subtraction was reaching for: every
// redelivered record whose key had already been applied is one double apply and
// one log line.
func TestTheOffArmsDoubleAppliesEqualItsRedeliveryLog(t *testing.T) {
	off := scrape(t, offMetrics)
	counter := metric(t, off, consumerSeries("double_applied_total"))
	logged := lines(t, offLog, "msg=redelivery")
	if counter != logged {
		t.Fatalf("double_applied_total is %d but the log carries %d redeliveries", counter, logged)
	}
}

// With zero losses, every redelivered record's key is still remembered, so
// every one is suppressed and logged.
func TestTheDedupeArmsSuppressionsEqualItsRedeliveryLog(t *testing.T) {
	on := scrape(t, onMetrics)
	counter := metric(t, on, consumerSeries("duplicates_suppressed_total"))
	logged := lines(t, onLog, "msg=redelivery")
	if counter != logged {
		t.Fatalf("duplicates_suppressed_total is %d but the log carries %d redeliveries", counter, logged)
	}
}

// The dedupe arm's whole claim: the replay is harmless.
func TestTheDedupeArmDoubleAppliedNothing(t *testing.T) {
	on := scrape(t, onMetrics)
	if got := metric(t, on, consumerSeries("double_applied_total")); got != 0 {
		t.Fatalf("the dedupe arm double-applied %d records", got)
	}
}

// The at-least-once arm suppresses nothing, because its seen-set is never
// consulted.
func TestTheAtLeastOnceArmSuppressedNothing(t *testing.T) {
	off := scrape(t, offMetrics)
	if got := metric(t, off, consumerSeries("duplicates_suppressed_total")); got != 0 {
		t.Fatalf("the at-least-once arm suppressed %d records", got)
	}
}

// consumed_total counts the loop and applied_total counts effects; with nothing
// suppressed they increment on the same events.
func TestTheAtLeastOnceArmAppliedEveryRecordItConsumed(t *testing.T) {
	off := scrape(t, offMetrics)
	applied := metric(t, off, consumerSeries("applied_total"))
	consumed := metric(t, off, consumerSeries("consumed_total"))
	if applied != consumed {
		t.Fatalf("applied %d but consumed %d", applied, consumed)
	}
}

// On the dedupe arm every record is either applied or suppressed, exactly once.
func TestTheDedupeArmsOutcomesAccountForEveryRecord(t *testing.T) {
	on := scrape(t, onMetrics)
	applied := metric(t, on, consumerSeries("applied_total"))
	suppressed := metric(t, on, consumerSeries("duplicates_suppressed_total"))
	consumed := metric(t, on, consumerSeries("consumed_total"))
	if applied+suppressed != consumed {
		t.Fatalf("applied %d + suppressed %d = %d, but consumed %d",
			applied, suppressed, applied+suppressed, consumed)
	}
}

// ── THE FAULT SET, WHICH IS THE REPRODUCIBILITY CLAIM ──────────────────────

// THE FIRED SET IS A PURE FUNCTION OF THE SEED AND THE DELIVERED KEYS, so over
// the records BOTH arms delivered the two arms must have fired the identical
// set.
//
// The comparison is over the FIRED set and not the rewind targets. Targets picks
// at most one record per partition per batch, so the target set is a subset
// chosen by broker batching — which is what run 1 compared, and why it failed.
// See results/pf-s313/capture-run1/README.md.
//
// ── THE WINDOW IS PER PARTITION, AND THAT IS NOT A TECHNICALITY
//
// The obvious window — "sequence numbers up to the smaller arm's highest" — is
// WRONG in both directions, and the run-2 captures show both:
//
//   - AT THE BOTTOM. The consumer joins the group at the END of the topic
//     (ConsumeResetOffset AtEnd, so a restarted lab does not open on ten minutes
//     of backlog). Each arm therefore starts at whatever the producer had
//     reached, and they differ: the at-least-once arm's first fired key is
//     seq 474, the dedupe arm's is seq 251. Keys 251, 331 and 350 were never
//     DELIVERED to the first arm, so they cannot have fired in it.
//   - AT THE TOP. The sequence number is global but the topic has three
//     partitions, and at any instant they sit at different offsets. So a higher
//     sequence number being present does NOT imply every lower one is: the
//     dedupe arm fired at seq 6106 and had not yet been delivered seq 5821.
//
// A GLOBAL SEQUENCE NUMBER IS NOT A WATERMARK. What is comparable is a range of
// OFFSETS WITHIN ONE PARTITION: offsets are contiguous and consumed in order, so
// an arm that fired at offset o1 and at o2 > o1 in partition p was delivered
// everything between them. Both arms see identical records at identical
// partitions and offsets — same seeded producer, same pinned nonce, same fresh
// broker — so (partition, offset) is a stable coordinate across the two.
func TestBothArmsFiredTheIdenticalFaultSetOverTheRecordsBothDelivered(t *testing.T) {
	offFired := firedRecords(t, offLog)
	onFired := firedRecords(t, onLog)
	if len(offFired) == 0 || len(onFired) == 0 {
		t.Fatal("an arm fired no keys; the comparison proves nothing")
	}

	offWindows := coveredWindows(offFired)
	onWindows := coveredWindows(onFired)

	var compared int
	for partition, offWin := range offWindows {
		onWin, both := onWindows[partition]
		if !both {
			continue
		}
		lo, hi := offWin.lo, offWin.hi
		if onWin.lo > lo {
			lo = onWin.lo
		}
		if onWin.hi < hi {
			hi = onWin.hi
		}
		if lo > hi {
			continue
		}

		offSet := keysInWindow(offFired, partition, lo, hi)
		onSet := keysInWindow(onFired, partition, lo, hi)
		compared += len(offSet)

		for k := range offSet {
			if !onSet[k] {
				t.Errorf("partition %d offsets [%d,%d]: key %q fired on the at-least-once arm and not on the dedupe arm",
					partition, lo, hi, k)
			}
		}
		for k := range onSet {
			if !offSet[k] {
				t.Errorf("partition %d offsets [%d,%d]: key %q fired on the dedupe arm and not on the at-least-once arm",
					partition, lo, hi, k)
			}
		}
	}

	if compared == 0 {
		t.Fatal("no partition had a common offset window; the comparison proves nothing")
	}
	t.Logf("compared %d fired keys across %d partitions", compared, len(offWindows))
}

// firedRecord is one `fault fired` log line.
type firedRecord struct {
	key       string
	partition int
	offset    int
}

func firedRecords(t *testing.T, path string) []firedRecord {
	t.Helper()
	keys, err := FieldValues(path, `msg="fault fired"`, "key")
	if err != nil {
		t.Fatalf("%v", err)
	}
	partitions, err := FieldValues(path, `msg="fault fired"`, "partition")
	if err != nil {
		t.Fatalf("%v", err)
	}
	offsets, err := FieldValues(path, `msg="fault fired"`, "offset")
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(keys) != len(partitions) || len(keys) != len(offsets) {
		t.Fatalf("%s: %d keys, %d partitions, %d offsets", path, len(keys), len(partitions), len(offsets))
	}

	out := make([]firedRecord, len(keys))
	for i := range keys {
		out[i] = firedRecord{key: keys[i], partition: atoi(t, partitions[i]), offset: atoi(t, offsets[i])}
	}
	return out
}

type window struct{ lo, hi int }

// coveredWindows is, per partition, the offset range an arm demonstrably
// covered: offsets are contiguous within a partition and consumed in order, so
// everything between the first and last fired offset was delivered.
func coveredWindows(fired []firedRecord) map[int]window {
	out := map[int]window{}
	for _, f := range fired {
		w, seen := out[f.partition]
		if !seen {
			out[f.partition] = window{lo: f.offset, hi: f.offset}
			continue
		}
		if f.offset < w.lo {
			w.lo = f.offset
		}
		if f.offset > w.hi {
			w.hi = f.offset
		}
		out[f.partition] = w
	}
	return out
}

func keysInWindow(fired []firedRecord, partition, lo, hi int) map[string]bool {
	out := map[string]bool{}
	for _, f := range fired {
		if f.partition == partition && f.offset >= lo && f.offset <= hi {
			out[f.key] = true
		}
	}
	return out
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		t.Fatalf("not a number: %q", s)
	}
	return n
}

// The committed fault-set files must match the logs they were extracted from,
// so a stale file cannot stand in for the evidence.
func TestTheCommittedFaultSetFilesMatchTheirLogs(t *testing.T) {
	for _, c := range []struct{ logPath, setPath string }{
		{offLog, offFaultSet},
		{onLog, onFaultSet},
	} {
		fromLog, err := FieldValues(c.logPath, `msg="fault fired"`, "key")
		if err != nil {
			t.Fatalf("%v", err)
		}
		raw, err := os.ReadFile(c.setPath)
		if err != nil {
			t.Fatalf("%s: %v", c.setPath, err)
		}
		var fromFile []string
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				fromFile = append(fromFile, line)
			}
		}
		if len(fromLog) != len(fromFile) {
			t.Fatalf("%s carries %d keys but %s logs %d", c.setPath, len(fromFile), c.logPath, len(fromLog))
		}
	}
}
