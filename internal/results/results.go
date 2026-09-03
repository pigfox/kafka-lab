// Package results reads the raw captures of a measured run.
//
// IT EXISTS SO THE README CANNOT DRIFT FROM THE EVIDENCE. Every figure the
// README states about delivery semantics is re-derived from the committed
// capture files by a test, and a mismatch fails the build. The alternative —
// numbers typed into prose beside files nobody re-reads — is how a document
// comes to describe a run that never happened, and the drift is invisible
// precisely because both halves look fine on their own.
//
// The parsers are deliberately small and strict. A metric that is absent is an
// ERROR rather than a zero: a scrape missing a series and a scrape reporting
// zero mean opposite things, and silently conflating them would let a guard
// pass over a capture that had lost the very counter it was checking.
package results

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Scrape is one /metrics capture.
type Scrape struct {
	path  string
	lines []string
}

// ReadScrape loads a Prometheus text-format capture.
func ReadScrape(path string) (*Scrape, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("results: read scrape: %w", err)
	}
	// SPLIT RATHER THAN SCAN. The whole file is already in memory, and
	// bufio.Scanner imposes a maximum token size that would turn one long line
	// into an error the caller has no way to act on. A capture is evidence; a
	// reader of evidence should not have a line-length limit of its own.
	var lines []string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("results: scrape %s carries no samples", path)
	}
	return &Scrape{path: path, lines: lines}, nil
}

// Value returns the sample whose series name and label set match selector,
// which is written exactly as it appears in the capture — for example
// `kafka_lab_double_applied_total{service="consumer"}`.
//
// An absent series is an error, never a zero.
func (s *Scrape) Value(selector string) (float64, error) {
	prefix := selector + " "
	for _, line := range s.lines {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		field := strings.TrimSpace(strings.TrimPrefix(line, prefix))
		v, err := strconv.ParseFloat(field, 64)
		if err != nil {
			return 0, fmt.Errorf("results: %s in %s: value %q: %w", selector, s.path, field, err)
		}
		return v, nil
	}
	return 0, fmt.Errorf("results: %s is absent from %s; absent and zero are not the same answer", selector, s.path)
}

// Int is Value for a counter, refusing a value that is not a whole number.
func (s *Scrape) Int(selector string) (int, error) {
	v, err := s.Value(selector)
	if err != nil {
		return 0, err
	}
	n := int(v)
	if float64(n) != v {
		return 0, fmt.Errorf("results: %s in %s is %v, which is not a whole number of records", selector, s.path, v)
	}
	return n, nil
}

// CountLines returns how many lines of the file contain substr.
//
// It is what turns the consumer's own log into evidence: the fault and
// redelivery counts the README publishes are counted from the log rather than
// read from the counter they are supposed to corroborate.
func CountLines(path, substr string) (int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("results: read log: %w", err)
	}
	var n int
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n, nil
}

// FieldValues returns the value of key= for every line containing substr, in
// file order. slog writes unquoted values for tokens with no spaces, which is
// what every field this reads is.
func FieldValues(path, substr, key string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("results: read log: %w", err)
	}
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.Contains(line, substr) {
			continue
		}
		v, ok := field(line, key)
		if !ok {
			return nil, fmt.Errorf("results: %s: line matched %q but carries no %s=: %s", path, substr, key, line)
		}
		out = append(out, v)
	}
	return out, nil
}

// field reads key= from one slog line, stopping at the next space.
func field(line, key string) (string, bool) {
	at := strings.Index(line, " "+key+"=")
	if at < 0 {
		return "", false
	}
	rest := line[at+len(key)+2:]
	if end := strings.IndexByte(rest, ' '); end >= 0 {
		rest = rest[:end]
	}
	if rest == "" {
		return "", false
	}
	return rest, true
}
