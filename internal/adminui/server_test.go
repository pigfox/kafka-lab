package adminui

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/event"
	"github.com/pigfox/kafka-lab/internal/kafkabus"
)

func do(t *testing.T, h *harness, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.srv.Handler().ServeHTTP(rec, r)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decoding %s: %v", rec.Body.String(), err)
	}
	return v
}

// ── construction ────────────────────────────────────────────────────────────

// A page that 500s the first time somebody opens it is a demo that is broken in
// front of an audience, so the template is parsed at boot.
func TestNewParsesTheEmbeddedPageUpFront(t *testing.T) {
	h := newHarness(t)
	if h.srv.tmpl == nil {
		t.Fatal("New must parse the page before serving")
	}
}

func TestNewRequiresEveryDependency(t *testing.T) {
	cases := map[string]func(*Options){
		"logger":    func(o *Options) { o.Log = nil },
		"publisher": func(o *Options) { o.Publisher = nil },
		"settings":  func(o *Options) { o.Settings = nil },
		"rates":     func(o *Options) { o.Rates = nil },
		"lag":       func(o *Options) { o.Lag = nil },
		"tail":      func(o *Options) { o.Tail = nil },
	}
	for name, drop := range cases {
		t.Run(name, func(t *testing.T) {
			o := Options{
				Log: quiet(), Publisher: &fakePublisher{}, Settings: &fakeSettings{},
				Rates: &fakeRates{}, Lag: &fakeLag{}, Tail: newFakeTail(1),
			}
			drop(&o)
			if _, err := New(o); err == nil {
				t.Fatalf("a missing %s must be refused at construction", name)
			}
		})
	}
}

func TestNewSuppliesAKeepaliveDefault(t *testing.T) {
	for _, given := range []time.Duration{0, -time.Second} {
		h := newHarness(t, func(o *Options) { o.TailKeepalive = given })
		if h.srv.tailKeepalive <= 0 {
			t.Fatalf("keepalive %v produced %v", given, h.srv.tailKeepalive)
		}
	}
}

// ── the page ────────────────────────────────────────────────────────────────

func TestPageRenders(t *testing.T) {
	rec := do(t, newHarness(t), "GET", "/", "")
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("content type %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "<title>kafka-lab") {
		t.Fatal("the page did not render")
	}
}

// THE BOUNDS ARE SERVER-RENDERED from the Go constants. A page carrying its own
// copy is a page that disagrees with the server the first time a bound moves.
func TestPageSlidersCarryTheGoBounds(t *testing.T) {
	body := do(t, newHarness(t), "GET", "/", "").Body.String()

	rate := fmt.Sprintf(`min="%v" max="%v"`, control.MinRatePerSec, control.MaxRatePerSec)
	if strings.Count(body, rate) != 2 {
		t.Fatalf("both rate sliders must carry %s; found %d", rate, strings.Count(body, rate))
	}
	work := fmt.Sprintf(`min="%d" max="%d"`, control.MinWorkMillis, control.MaxWorkMillis)
	if !strings.Contains(body, work) {
		t.Fatalf("the work slider must carry %s", work)
	}
}

// The script must READ the bounds from the DOM, not restate them. A literal
// bound in the script is the drift this guards against.
func TestPageScriptDoesNotRestateTheBounds(t *testing.T) {
	body := do(t, newHarness(t), "GET", "/", "").Body.String()
	i := strings.Index(body, "<script>")
	if i < 0 {
		t.Fatal("the page must render its script")
	}
	script := body[i:]

	for _, literal := range []string{
		fmt.Sprintf("%v", control.MaxRatePerSec),
		fmt.Sprintf("%d", control.MaxWorkMillis),
	} {
		if strings.Contains(script, literal) {
			t.Fatalf("the script restates the bound %s instead of reading it from the DOM", literal)
		}
	}
}

// The browser rendering this page is OUTSIDE the compose network, so a link to
// http://prometheus:9090 would be dead.
func TestPageLinksAreTheHostSideURLs(t *testing.T) {
	body := do(t, newHarness(t), "GET", "/", "").Body.String()
	for _, want := range []string{"http://localhost:18081", "http://localhost:18082", "http://localhost:18083"} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing link %s", want)
		}
	}
	for _, deny := range []string{"http://prometheus:", "http://grafana:", "http://kafka-ui:"} {
		if strings.Contains(body, deny) {
			t.Fatalf("the page carries a compose-network URL the browser cannot reach: %s", deny)
		}
	}
}

// A CDN link makes the demo require internet and degrade behind a proxy.
func TestPageLoadsNothingExternal(t *testing.T) {
	body := do(t, newHarness(t), "GET", "/", "").Body.String()
	for _, deny := range []string{"<script src=", "<link rel=\"stylesheet\"", "cdn.", "unpkg", "jsdelivr", "googleapis"} {
		if strings.Contains(body, deny) {
			t.Fatalf("the page must be self-contained; found %q", deny)
		}
	}
}

// The observer must be visibly separate from the thing observed, and the page
// must name the right group when it says so.
func TestPageNamesBothGroupsCorrectly(t *testing.T) {
	body := do(t, newHarness(t), "GET", "/", "").Body.String()
	for _, want := range []string{kafkabus.AdminTailGroup, kafkabus.ConsumerGroup} {
		if !strings.Contains(body, want) {
			t.Fatalf("the page must name the group %q", want)
		}
	}
}

// EVERY FIGURE DECLARES WHETHER IT IS A MEASUREMENT OR A SETTING. A panel
// showing the requested rate under a "consumed" caption would render perfectly
// and teach the reader something false.
func TestPagePanelsDeclareMeasuredOrSetting(t *testing.T) {
	body := do(t, newHarness(t), "GET", "/", "").Body.String()
	for _, panel := range []struct{ id, kind string }{
		{"produced", "measured"},
		{"consumed", "measured"},
		{"lag", "measured"},
		{"producer-panel", "setting"},
		{"consumer-panel", "setting"},
	} {
		i := strings.Index(body, `id="`+panel.id+`"`)
		if i < 0 {
			t.Fatalf("panel %s is missing", panel.id)
		}
		head := body[i:min(i+400, len(body))]
		if !strings.Contains(head, `<span class="kind">`+panel.kind+`</span>`) {
			t.Fatalf("panel %s must be marked %s", panel.id, panel.kind)
		}
	}
}

func TestUnknownRouteIs404(t *testing.T) {
	if rec := do(t, newHarness(t), "GET", "/nope", ""); rec.Code != 404 {
		t.Fatalf("status %d", rec.Code)
	}
	// "GET /{$}" must not swallow every path.
	if rec := do(t, newHarness(t), "GET", "/deeper/path", ""); rec.Code != 404 {
		t.Fatalf("status %d", rec.Code)
	}
}

// ── healthz ─────────────────────────────────────────────────────────────────

// run.sh polls this until it answers; it must not depend on the broker being
// reachable, or a slow Kafka would look like a broken admin.
func TestHealthzIsUnconditional(t *testing.T) {
	h := newHarness(t)
	h.lag.err = errFake
	h.rates.producedErr = errFake
	h.publisher.err = errFake

	rec := do(t, h, "GET", "/healthz", "")
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	got := decode[map[string]string](t, rec)
	if got["status"] != "ok" {
		t.Fatalf("got %v", got)
	}
}

// ── GET /api/settings ───────────────────────────────────────────────────────

// "Nobody has published yet" and "somebody chose exactly the defaults" are
// different facts, and the UI says which.
func TestGetSettingsReportsWhetherARecordHasBeenRead(t *testing.T) {
	h := newHarness(t)

	got := decode[settingsResponse](t, do(t, h, "GET", "/api/settings", ""))
	if got.FromControlTopic {
		t.Fatal("with no record read, the response must say so")
	}
	if got.Settings != control.Defaults() {
		t.Fatalf("got %v want defaults", got.Settings)
	}

	h.settings.set(control.Settings{ProducerRatePerSec: 7, ConsumerRatePerSec: 8, ConsumerWorkMillis: 9})
	got = decode[settingsResponse](t, do(t, h, "GET", "/api/settings", ""))
	if !got.FromControlTopic {
		t.Fatal("after a record, the response must say the settings came from the topic")
	}
	if got.ProducerRatePerSec != 7 || got.ConsumerRatePerSec != 8 || got.ConsumerWorkMillis != 9 {
		t.Fatalf("got %v", got.Settings)
	}
}

func TestSettingsResponsesAreNotCached(t *testing.T) {
	rec := do(t, newHarness(t), "GET", "/api/settings", "")
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control %q", got)
	}
}

// ── POST /api/settings ──────────────────────────────────────────────────────

// A SLIDER GOES OVER THE BUS. This is the assertion that the demo's central
// claim is true: admin's only outbound effect is a published record.
func TestPostSettingsPublishesToTheControlTopic(t *testing.T) {
	h := newHarness(t)
	rec := do(t, h, "POST", "/api/settings", `{"consumer_rate_per_sec":3}`)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if h.publisher.count() != 1 {
		t.Fatalf("published %d records, want 1", h.publisher.count())
	}
	if got := h.publisher.last(t).ConsumerRatePerSec; got != 3 {
		t.Fatalf("published consumer rate %v want 3", got)
	}
}

// A PARTIAL UPDATE. Two browser tabs open on the same lab is the case this
// protects: moving one slider must not reset the others to whatever this tab
// last read.
func TestPostSettingsLeavesUnsentFieldsAlone(t *testing.T) {
	h := newHarness(t)
	h.settings.set(control.Settings{ProducerRatePerSec: 100, ConsumerRatePerSec: 200, ConsumerWorkMillis: 30})

	do(t, h, "POST", "/api/settings", `{"consumer_rate_per_sec":5}`)

	got := h.publisher.last(t)
	if got.ProducerRatePerSec != 100 {
		t.Fatalf("producer rate was clobbered: %v", got.ProducerRatePerSec)
	}
	if got.ConsumerWorkMillis != 30 {
		t.Fatalf("work was clobbered: %v", got.ConsumerWorkMillis)
	}
	if got.ConsumerRatePerSec != 5 {
		t.Fatalf("consumer rate: got %v want 5", got.ConsumerRatePerSec)
	}
}

func TestPostSettingsAcceptsEveryField(t *testing.T) {
	h := newHarness(t)
	do(t, h, "POST", "/api/settings", `{"producer_rate_per_sec":11,"consumer_rate_per_sec":12,"consumer_work_ms":13}`)
	got := h.publisher.last(t)
	if got.ProducerRatePerSec != 11 || got.ConsumerRatePerSec != 12 || got.ConsumerWorkMillis != 13 {
		t.Fatalf("got %v", got)
	}
}

// THE RESPONSE IS THE CLAMPED, PUBLISHED RECORD, not the request. A browser
// that asked for 9999/s must get back what will actually run, or its slider
// displays a setting no service is honouring.
func TestPostSettingsClampsAndReturnsWhatWasPublished(t *testing.T) {
	h := newHarness(t)
	rec := do(t, h, "POST", "/api/settings", `{"producer_rate_per_sec":9999,"consumer_rate_per_sec":-4,"consumer_work_ms":99999}`)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}

	got := decode[settingsResponse](t, rec)
	if got.ProducerRatePerSec != control.MaxRatePerSec {
		t.Fatalf("producer: got %v want %v", got.ProducerRatePerSec, control.MaxRatePerSec)
	}
	if got.ConsumerRatePerSec != control.MinRatePerSec {
		t.Fatalf("consumer: got %v want %v", got.ConsumerRatePerSec, control.MinRatePerSec)
	}
	if got.ConsumerWorkMillis != control.MaxWorkMillis {
		t.Fatalf("work: got %v want %v", got.ConsumerWorkMillis, control.MaxWorkMillis)
	}
	if published := h.publisher.last(t); published != got.Settings {
		t.Fatalf("the response %v is not what was published %v", got.Settings, published)
	}
}

func TestPostSettingsStampsTheRecord(t *testing.T) {
	h := newHarness(t)
	before := time.Now().UnixMilli()
	do(t, h, "POST", "/api/settings", `{"consumer_rate_per_sec":5}`)
	got := h.publisher.last(t)
	if got.UpdatedAtUnixMilli < before {
		t.Fatalf("the published record was not stamped: %d", got.UpdatedAtUnixMilli)
	}
}

func TestPostSettingsRejectsMalformedJSON(t *testing.T) {
	h := newHarness(t)
	rec := do(t, h, "POST", "/api/settings", `{not json`)
	if rec.Code != 400 {
		t.Fatalf("status %d", rec.Code)
	}
	if h.publisher.count() != 0 {
		t.Fatal("a rejected request must publish nothing")
	}
}

// An unknown field is a typo or a stale client, and silently ignoring it means
// a slider that appears to work and changes nothing.
func TestPostSettingsRejectsUnknownFields(t *testing.T) {
	h := newHarness(t)
	rec := do(t, h, "POST", "/api/settings", `{"consumer_rate":5}`)
	if rec.Code != 400 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if h.publisher.count() != 0 {
		t.Fatal("a rejected request must publish nothing")
	}
}

func TestPostSettingsRejectsAnEmptyUpdate(t *testing.T) {
	h := newHarness(t)
	rec := do(t, h, "POST", "/api/settings", `{}`)
	if rec.Code != 400 {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "no setting supplied") {
		t.Fatalf("body %s", rec.Body)
	}
	if h.publisher.count() != 0 {
		t.Fatal("a rejected request must publish nothing")
	}
}

func TestPostSettingsBoundsTheRequestBody(t *testing.T) {
	h := newHarness(t)
	huge := `{"consumer_rate_per_sec":5,"pad":"` + strings.Repeat("a", maxSettingsBody*2) + `"}`
	rec := do(t, h, "POST", "/api/settings", huge)
	if rec.Code != 400 {
		t.Fatalf("status %d", rec.Code)
	}
	if h.publisher.count() != 0 {
		t.Fatal("a rejected request must publish nothing")
	}
}

// A failed publish must be reported as a failure. A UI that showed success for
// a setting the broker never accepted is worse than no UI.
func TestPostSettingsReportsAPublishFailure(t *testing.T) {
	h := newHarness(t)
	h.publisher.err = errFake

	rec := do(t, h, "POST", "/api/settings", `{"consumer_rate_per_sec":5}`)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d want 502", rec.Code)
	}
	got := decode[map[string]string](t, rec)
	if !strings.Contains(got["error"], "control topic") {
		t.Fatalf("the error must say what failed: %v", got)
	}
}

func TestGetOnPostRouteIsNotAllowed(t *testing.T) {
	// The mux registers GET and POST on /api/settings separately; a PUT must
	// not fall through to either.
	rec := do(t, newHarness(t), "PUT", "/api/settings", `{}`)
	if rec.Code == 200 {
		t.Fatalf("PUT was accepted: %d", rec.Code)
	}
}

// ── GET /api/stats ──────────────────────────────────────────────────────────

func TestStatsReportsMeasuredAndRequestedSeparately(t *testing.T) {
	h := newHarness(t)
	h.rates.produced = 50.5
	h.rates.consumed = 12.25
	h.lag.total = 4321
	h.settings.set(control.Settings{ProducerRatePerSec: 60, ConsumerRatePerSec: 200, ConsumerWorkMillis: 20})

	got := decode[statsResponse](t, do(t, h, "GET", "/api/stats", ""))

	if got.Measured.ProducedPerSec != 50.5 || got.Measured.ConsumedPerSec != 12.25 {
		t.Fatalf("measured: %+v", got.Measured)
	}
	if got.Measured.ConsumerLag != 4321 {
		t.Fatalf("lag: %d", got.Measured.ConsumerLag)
	}
	if got.Requested.ConsumerRatePerSec != 200 {
		t.Fatalf("requested: %+v", got.Requested)
	}
	// THE POINT OF THE SPLIT: 200 was asked for, 12.25 was achieved, and the
	// frame keeps them apart so no panel can print one under the other's name.
	if got.Requested.ConsumerRatePerSec == got.Measured.ConsumedPerSec {
		t.Fatal("this fixture must have the requested and achieved rates differ")
	}
}

// A panel that empties because a NEIGHBOURING query failed reports the wrong
// outage, so a failure names itself and the other figures still arrive.
func TestStatsDegradesPerFigure(t *testing.T) {
	cases := []struct {
		name    string
		break_  func(*harness)
		degrade string
	}{
		{"produced", func(h *harness) { h.rates.producedErr = errFake }, "produced_per_sec"},
		{"consumed", func(h *harness) { h.rates.consumedErr = errFake }, "consumed_per_sec"},
		{"lag", func(h *harness) { h.lag.err = errFake }, "consumer_group_lag"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			h.rates.produced, h.rates.consumed, h.lag.total = 11, 22, 33
			c.break_(h)

			rec := do(t, h, "GET", "/api/stats", "")
			if rec.Code != 200 {
				t.Fatalf("a partial failure must still answer 200, got %d", rec.Code)
			}
			got := decode[statsResponse](t, rec)
			if len(got.Degraded) != 1 || got.Degraded[0] != c.degrade {
				t.Fatalf("degraded: got %v want [%s]", got.Degraded, c.degrade)
			}
			// The surviving figures must still be present.
			present := 0
			if got.Measured.ProducedPerSec == 11 {
				present++
			}
			if got.Measured.ConsumedPerSec == 22 {
				present++
			}
			if got.Measured.ConsumerLag == 33 {
				present++
			}
			if present != 2 {
				t.Fatalf("one failure blanked the neighbours: %+v", got.Measured)
			}
		})
	}
}

func TestStatsDegradedIsAnEmptyListNotNull(t *testing.T) {
	// `null` and `[]` are different in JavaScript, and the page does
	// `new Set(stats.degraded || [])`; an explicit empty list keeps the
	// contract obvious rather than relying on that fallback.
	body := do(t, newHarness(t), "GET", "/api/stats", "").Body.String()
	if !strings.Contains(body, `"degraded":[]`) {
		t.Fatalf("want an empty list, got %s", body)
	}
}

func TestStatsReportsTailCounters(t *testing.T) {
	h := newHarness(t)
	ch, stop := h.tail.Subscribe()
	defer stop()
	_ = ch
	h.tail.send(kafkabus.Record{Offset: 1})

	got := decode[statsResponse](t, do(t, h, "GET", "/api/stats", ""))
	if got.Tail.Subscribers != 1 {
		t.Fatalf("subscribers: %d", got.Tail.Subscribers)
	}
	if got.Tail.Delivered != 1 {
		t.Fatalf("delivered: %d", got.Tail.Delivered)
	}
}

// A SILENT DROP IS A GRAPH THAT LIES. The UI shows the drop count, so it has to
// be in the frame.
func TestStatsReportsTailDrops(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.Tail = newFakeTail(1) })
	tail := h.srv.tail.(*fakeTail)
	_, stop := tail.Subscribe()
	defer stop()

	for i := 0; i < 10; i++ {
		tail.send(kafkabus.Record{Offset: int64(i)})
	}
	got := decode[statsResponse](t, do(t, h, "GET", "/api/stats", ""))
	if got.Tail.Dropped != 9 {
		t.Fatalf("dropped: got %d want 9", got.Tail.Dropped)
	}
}

// ── GET /api/tail (SSE) ─────────────────────────────────────────────────────

func TestTailStreamsRecordsAsSSE(t *testing.T) {
	h := newHarness(t)
	srv := httptest.NewServer(h.srv.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/tail")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("cache control %q", cc)
	}
	// Without this an intermediary that buffers holds the whole stream until
	// the connection closes, which for a live tail means forever.
	if b := resp.Header.Get("X-Accel-Buffering"); b != "no" {
		t.Fatalf("X-Accel-Buffering %q", b)
	}

	// Wait for the handler to actually subscribe before sending.
	waitForSubscribers(t, h.tail, 1)

	value, _ := event.Event{Seq: 42, Kind: "order.paid", Region: "eu-west", AmountCents: 1999,
		EmittedAtMilli: time.Now().Add(-3 * time.Second).UnixMilli()}.JSON()
	h.tail.send(kafkabus.Record{Topic: "events", Partition: 1, Offset: 77, Key: "eu-west", Value: value})

	frame := readFrame(t, resp)
	if frame.Offset != 77 || frame.Partition != 1 || frame.Key != "eu-west" {
		t.Fatalf("frame: %+v", frame)
	}
	if !strings.Contains(frame.Summary, "order.paid") {
		t.Fatalf("summary: %q", frame.Summary)
	}
	if frame.AgeMillis < 2500 {
		t.Fatalf("age: %d — the tail must carry how long the record waited", frame.AgeMillis)
	}
}

// A proxy or a sleeping laptop drops an idle connection with no notice to
// either end. A comment frame keeps it provably alive and costs two bytes.
func TestTailSendsKeepaliveComments(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.TailKeepalive = 20 * time.Millisecond })
	srv := httptest.NewServer(h.srv.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/tail")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer resp.Body.Close()

	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.HasPrefix(line, ":") {
		t.Fatalf("want an SSE comment frame, got %q", line)
	}
}

// A tail that swallowed what it could not parse would hide exactly the message
// an operator had just injected by hand from kafka-ui.
func TestTailShowsUnparseableRecords(t *testing.T) {
	h := newHarness(t)
	srv := httptest.NewServer(h.srv.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/tail")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer resp.Body.Close()
	waitForSubscribers(t, h.tail, 1)

	h.tail.send(kafkabus.Record{Topic: "events", Offset: 5, Value: []byte("hand-typed nonsense")})

	frame := readFrame(t, resp)
	if frame.Summary != "(unparsed)" {
		t.Fatalf("summary: %q", frame.Summary)
	}
	if frame.Raw != "hand-typed nonsense" {
		t.Fatalf("raw: %q", frame.Raw)
	}
}

func TestTailEndsWhenTheSourceCloses(t *testing.T) {
	h := newHarness(t)
	srv := httptest.NewServer(h.srv.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/tail")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer resp.Body.Close()
	waitForSubscribers(t, h.tail, 1)

	h.tail.close()

	done := make(chan struct{})
	go func() {
		_, _ = bufio.NewReader(resp.Body).ReadString('\n')
		io := make([]byte, 1)
		for {
			if _, err := resp.Body.Read(io); err != nil {
				close(done)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream must end when the tail source closes")
	}
}

func TestTailUnsubscribesWhenTheClientGoesAway(t *testing.T) {
	h := newHarness(t)
	srv := httptest.NewServer(h.srv.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/tail")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	waitForSubscribers(t, h.tail, 1)
	resp.Body.Close()

	waitForSubscribers(t, h.tail, 0)
}

func TestTailRefusesANonFlushingWriter(t *testing.T) {
	h := newHarness(t)
	w := nonFlushingWriter{httptest.NewRecorder()}
	h.srv.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/api/tail", nil))
	if w.rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d", w.rec.Code)
	}
}

// nonFlushingWriter hides the recorder's Flush method so the handler takes its
// unsupported-streaming branch.
type nonFlushingWriter struct{ rec *httptest.ResponseRecorder }

func (w nonFlushingWriter) Header() http.Header         { return w.rec.Header() }
func (w nonFlushingWriter) Write(b []byte) (int, error) { return w.rec.Write(b) }
func (w nonFlushingWriter) WriteHeader(c int)           { w.rec.WriteHeader(c) }

func waitForSubscribers(t *testing.T, tail *fakeTail, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, got := tail.Stats(); got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, _, got := tail.Stats()
	t.Fatalf("subscribers: got %d want %d", got, want)
}

func readFrame(t *testing.T, resp *http.Response) tailFrame {
	t.Helper()
	r := bufio.NewReader(resp.Body)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		payload, ok := strings.CutPrefix(strings.TrimRight(line, "\n"), "data: ")
		if !ok {
			continue // a keepalive comment or the blank separator
		}
		var f tailFrame
		if err := json.Unmarshal([]byte(payload), &f); err != nil {
			t.Fatalf("decoding frame %q: %v", payload, err)
		}
		return f
	}
	t.Fatal("no data frame arrived")
	return tailFrame{}
}

var _ = errors.Is
