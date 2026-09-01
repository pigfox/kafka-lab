package adminui

import (
	"errors"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pigfox/kafka-lab/internal/kafkabus"
)

var errWrite = errors.New("connection reset")

// failingWriter accepts headers and then refuses every write, which is what a
// browser tab closed mid-stream looks like from the handler's side.
type failingWriter struct {
	hdr     http.Header
	code    int
	flushed int
	afterN  int // writes to allow before failing
	writes  int
}

func newFailingWriter(afterN int) *failingWriter {
	return &failingWriter{hdr: http.Header{}, afterN: afterN}
}

func (w *failingWriter) Header() http.Header { return w.hdr }
func (w *failingWriter) WriteHeader(c int)   { w.code = c }
func (w *failingWriter) Flush()              { w.flushed++ }
func (w *failingWriter) Write(b []byte) (int, error) {
	w.writes++
	if w.writes > w.afterN {
		return 0, errWrite
	}
	return len(b), nil
}

// A TEMPLATE FAILURE MUST STOP THE SERVER AT BOOT, not surface as a 500 the
// first time somebody opens the page in front of an audience.
func TestNewFailsWhenTheTemplateWillNotParse(t *testing.T) {
	orig := parseTemplate
	t.Cleanup(func() { parseTemplate = orig })
	parseTemplate = func() (*template.Template, error) { return nil, errWrite }

	_, err := New(Options{
		Log: quiet(), Publisher: &fakePublisher{}, Settings: &fakeSettings{},
		Rates: &fakeRates{}, Lag: &fakeLag{}, Tail: newFakeTail(1),
	})
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "parse embedded page") {
		t.Fatalf("got %v", err)
	}
}

// The status is already on the wire by the time a render fails, so the only
// honest thing left is a log line — and the handler must not panic trying to
// do more.
func TestPageRenderFailureIsSurvivable(t *testing.T) {
	h := newHarness(t)
	w := newFailingWriter(0)
	h.srv.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	// No assertion on the body is possible; the assertion is that the handler
	// returned rather than panicking, and that it did try to write.
	if w.writes == 0 {
		t.Fatal("the handler never attempted to render")
	}
}

// A FRAME THAT WILL NOT ENCODE SKIPS ONE MESSAGE AND LEAVES THE STREAM OPEN.
// Tearing the connection down would lose every subsequent record over one bad
// one.
func TestTailSkipsAnUnencodableFrameAndKeepsStreaming(t *testing.T) {
	orig := marshalFrame
	t.Cleanup(func() { marshalFrame = orig })

	fail := true
	marshalFrame = func(f tailFrame) ([]byte, error) {
		if fail {
			fail = false
			return nil, errWrite
		}
		return orig(f)
	}

	h := newHarness(t)
	srv := httptest.NewServer(h.srv.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/tail")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer resp.Body.Close()
	waitForSubscribers(t, h.tail, 1)

	h.tail.send(kafkabus.Record{Offset: 1, Value: []byte(`{"seq":1}`)}) // skipped
	h.tail.send(kafkabus.Record{Offset: 2, Value: []byte(`{"seq":2}`)}) // must arrive

	frame := readFrame(t, resp)
	if frame.Offset != 2 {
		t.Fatalf("got offset %d; a skipped frame must not close the stream", frame.Offset)
	}
}

// A write failure on a data frame ends the stream, because the client is gone.
func TestTailReturnsWhenADataFrameCannotBeWritten(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.TailKeepalive = time.Hour })
	w := newFailingWriter(0)

	done := make(chan struct{})
	go func() {
		h.srv.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/api/tail", nil))
		close(done)
	}()

	waitForSubscribers(t, h.tail, 1)
	h.tail.send(kafkabus.Record{Offset: 1, Value: []byte(`{"seq":1}`)})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a dead client must end the stream")
	}
	waitForSubscribers(t, h.tail, 0)
}

// Same for a keepalive comment: it is the cheapest possible probe of whether
// the client is still there, and its failure is the answer.
func TestTailReturnsWhenAKeepaliveCannotBeWritten(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.TailKeepalive = 10 * time.Millisecond })
	w := newFailingWriter(0)

	done := make(chan struct{})
	go func() {
		h.srv.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/api/tail", nil))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a failed keepalive must end the stream")
	}
	waitForSubscribers(t, h.tail, 0)
}
