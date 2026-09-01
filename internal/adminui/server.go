// Package adminui is the admin service's HTTP surface and its embedded page.
//
// THE UI IS ONE FILE, EMBEDDED, WITH NO CDN AND NO BUILD STEP. That is a hard
// constraint rather than a preference: this repo must run from a bare clone on
// a stranger's laptop, and every one of the usual conveniences breaks that. A
// CDN link makes the demo require internet and quietly degrade behind a
// corporate proxy. A build step makes `docker compose up` insufficient. A
// framework makes the page's behaviour something you read a bundle to
// understand rather than the file next to this one.
//
// EVERY DEPENDENCY THIS SERVER HAS IS AN INTERFACE, and they are declared here
// rather than in the packages that satisfy them. That is what lets the whole
// HTTP surface — the clamping, the SSE framing, the partial-failure behaviour
// of the stats endpoint — be tested with no broker and no Prometheus.
package adminui

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"time"

	"github.com/pigfox/kafka-lab/internal/control"
	"github.com/pigfox/kafka-lab/internal/kafkabus"
)

//go:embed assets/index.html
var assetsFS embed.FS

// Publisher writes settings to the control topic. Admin NEVER calls the
// producer or the consumer directly; this interface is the only way it changes
// anything, and it goes over the bus.
type Publisher interface {
	Publish(ctx context.Context, s control.Settings) error
}

// SettingsSource reports what the control topic currently says, and whether any
// record has been read yet.
type SettingsSource interface {
	Settings() (control.Settings, bool)
}

// RateSource reports achieved rates. It is backed by Prometheus, not by direct
// calls to the services — see internal/promquery for why.
type RateSource interface {
	ProducedPerSec(ctx context.Context) (float64, error)
	ConsumedPerSec(ctx context.Context) (float64, error)
}

// LagSource reports the consumer group's lag, asked of the group coordinator.
type LagSource interface {
	Total(ctx context.Context) (int64, error)
}

// TailSource is the live message stream shown in the UI.
type TailSource interface {
	Subscribe() (<-chan kafkabus.Record, func())
	Stats() (sent, dropped uint64, subscribers int)
}

// Links are the other UIs the page points at. They are the HOST-side URLs, not
// the compose-network ones: the browser rendering this page is outside the
// compose network, so http://prometheus:9090 would be a dead link.
type Links struct {
	Grafana    string `json:"grafana"`
	Prometheus string `json:"prometheus"`
	KafkaUI    string `json:"kafka_ui"`
}

// Server is the admin HTTP surface.
type Server struct {
	log       *slog.Logger
	publisher Publisher
	settings  SettingsSource
	rates     RateSource
	lag       LagSource
	tail      TailSource
	links     Links

	// tailKeepalive bounds how long an SSE connection may sit silent. A proxy
	// or a laptop sleeping will drop an idle connection with no notice to
	// either end, and the browser's EventSource then reconnects — but only
	// after its own timeout. A periodic comment frame keeps the connection
	// provably alive and costs two bytes.
	tailKeepalive time.Duration

	tmpl *template.Template
}

// Options configure a Server. Every field is required except tailKeepalive.
type Options struct {
	Log           *slog.Logger
	Publisher     Publisher
	Settings      SettingsSource
	Rates         RateSource
	Lag           LagSource
	Tail          TailSource
	Links         Links
	TailKeepalive time.Duration
}

// New builds a Server. It parses the embedded page up front so a template error
// fails at boot rather than on the first request — a page that 500s the first
// time somebody opens it is a demo that is broken in front of an audience.
func New(o Options) (*Server, error) {
	if o.Log == nil {
		return nil, errors.New("adminui: a logger is required")
	}
	for name, dep := range map[string]any{
		"publisher": o.Publisher, "settings": o.Settings,
		"rates": o.Rates, "lag": o.Lag, "tail": o.Tail,
	} {
		if dep == nil {
			return nil, fmt.Errorf("adminui: %s is required", name)
		}
	}

	tmpl, err := parseTemplate()
	if err != nil {
		return nil, fmt.Errorf("adminui: parse embedded page: %w", err)
	}

	keepalive := o.TailKeepalive
	if keepalive <= 0 {
		keepalive = 15 * time.Second
	}

	return &Server{
		log: o.Log, publisher: o.Publisher, settings: o.Settings,
		rates: o.Rates, lag: o.Lag, tail: o.Tail, links: o.Links,
		tailKeepalive: keepalive, tmpl: tmpl,
	}, nil
}

// Handler returns the mux. Routes are declared in one place so the page and the
// tests read the same list.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handlePage)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /api/settings", s.handleGetSettings)
	mux.HandleFunc("POST /api/settings", s.handlePostSettings)
	mux.HandleFunc("GET /api/stats", s.handleStats)
	mux.HandleFunc("GET /api/tail", s.handleTail)
	return mux
}

// pageData is what the template renders. THE BOUNDS ARE SERVER-RENDERED into
// the slider attributes rather than restated in the page's script, so the Go
// constants are the single source of truth for what the UI will let you ask
// for. A page carrying its own copy of the limits is a page that disagrees with
// the server the first time a bound moves.
type pageData struct {
	Links          Links
	MinRate        float64
	MaxRate        float64
	MinWork        int
	MaxWork        int
	ConsumerGroup  string
	AdminTailGroup string
	EventsTopic    string
	ControlTopic   string
}

func (s *Server) handlePage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	data := pageData{
		Links:          s.links,
		MinRate:        control.MinRatePerSec,
		MaxRate:        control.MaxRatePerSec,
		MinWork:        control.MinWorkMillis,
		MaxWork:        control.MaxWorkMillis,
		ConsumerGroup:  kafkabus.ConsumerGroup,
		AdminTailGroup: kafkabus.AdminTailGroup,
		EventsTopic:    control.EventsTopic,
		ControlTopic:   control.Topic,
	}
	if err := s.tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		// The status is already written by now, so this can only be logged.
		s.log.Error("rendering the admin page failed midway", "error", err)
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// settingsResponse carries the knobs plus whether they came from a real record.
// The UI shows "waiting for the control topic" rather than silently presenting
// the defaults as though somebody had chosen them.
type settingsResponse struct {
	control.Settings
	FromControlTopic bool `json:"from_control_topic"`
}

func (s *Server) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	got, had := s.settings.Settings()
	writeJSON(w, http.StatusOK, settingsResponse{Settings: got, FromControlTopic: had})
}

// settingsRequest is a PARTIAL update: every field is a pointer, so a UI that
// moves one slider sends one field and cannot accidentally reset the other two
// to whatever it last read. Two browser tabs open on the same lab is the case
// this protects.
type settingsRequest struct {
	ProducerRatePerSec *float64 `json:"producer_rate_per_sec"`
	ConsumerRatePerSec *float64 `json:"consumer_rate_per_sec"`
	ConsumerWorkMillis *int     `json:"consumer_work_ms"`
}

const maxSettingsBody = 4 << 10

func (s *Server) handlePostSettings(w http.ResponseWriter, r *http.Request) {
	var req settingsRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSettingsBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "malformed request: " + err.Error()})
		return
	}
	if req.ProducerRatePerSec == nil && req.ConsumerRatePerSec == nil && req.ConsumerWorkMillis == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no setting supplied"})
		return
	}

	next, _ := s.settings.Settings()
	if req.ProducerRatePerSec != nil {
		next.ProducerRatePerSec = *req.ProducerRatePerSec
	}
	if req.ConsumerRatePerSec != nil {
		next.ConsumerRatePerSec = *req.ConsumerRatePerSec
	}
	if req.ConsumerWorkMillis != nil {
		next.ConsumerWorkMillis = *req.ConsumerWorkMillis
	}
	next = control.Stamp(next, time.Now())

	if err := s.publisher.Publish(r.Context(), next); err != nil {
		s.log.Error("publishing settings failed", "error", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not publish to the control topic: " + err.Error()})
		return
	}

	// THE RESPONSE IS THE CLAMPED, PUBLISHED RECORD, not the request. A
	// browser that asked for 9999/s gets 500 back and its slider snaps to what
	// actually happened, rather than displaying a setting no service will run.
	writeJSON(w, http.StatusOK, settingsResponse{Settings: next, FromControlTopic: true})
}

// statsResponse is what the UI's counters read.
//
// EVERY FIGURE SAYS WHETHER IT IS A MEASUREMENT OR A SETTING, and the split is
// enforced by the FRAME rather than by a caption: the achieved rates and the
// lag live under `measured`, the knobs under `requested`, and neither object
// carries the other's fields. The reason is concrete — a consumer asked for
// 200/s that spends 20ms per message achieves 50/s, so a panel that showed the
// requested figure would render perfectly, stay internally consistent, and
// teach the reader something false.
type statsResponse struct {
	Measured struct {
		ProducedPerSec float64 `json:"produced_per_sec"`
		ConsumedPerSec float64 `json:"consumed_per_sec"`
		ConsumerLag    int64   `json:"consumer_group_lag"`
	} `json:"measured"`
	Requested struct {
		ProducerRatePerSec float64 `json:"producer_rate_per_sec"`
		ConsumerRatePerSec float64 `json:"consumer_rate_per_sec"`
		ConsumerWorkMillis int     `json:"consumer_work_ms"`
	} `json:"requested"`
	Tail struct {
		Delivered   uint64 `json:"delivered"`
		Dropped     uint64 `json:"dropped"`
		Subscribers int    `json:"subscribers"`
	} `json:"tail"`
	// Degraded names the figures that could not be read this poll. It is a
	// LIST rather than a boolean so the UI can grey out the one number that is
	// stale instead of blanking the panel — a panel that empties because a
	// neighbouring query failed reports the wrong outage.
	Degraded []string `json:"degraded"`
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var out statsResponse
	out.Degraded = []string{}

	if v, err := s.rates.ProducedPerSec(ctx); err != nil {
		s.log.Warn("produced rate unavailable", "error", err)
		out.Degraded = append(out.Degraded, "produced_per_sec")
	} else {
		out.Measured.ProducedPerSec = v
	}

	if v, err := s.rates.ConsumedPerSec(ctx); err != nil {
		s.log.Warn("consumed rate unavailable", "error", err)
		out.Degraded = append(out.Degraded, "consumed_per_sec")
	} else {
		out.Measured.ConsumedPerSec = v
	}

	if v, err := s.lag.Total(ctx); err != nil {
		s.log.Warn("lag unavailable", "error", err)
		out.Degraded = append(out.Degraded, "consumer_group_lag")
	} else {
		out.Measured.ConsumerLag = v
	}

	settings, _ := s.settings.Settings()
	out.Requested.ProducerRatePerSec = settings.ProducerRatePerSec
	out.Requested.ConsumerRatePerSec = settings.ConsumerRatePerSec
	out.Requested.ConsumerWorkMillis = settings.ConsumerWorkMillis

	out.Tail.Delivered, out.Tail.Dropped, out.Tail.Subscribers = s.tail.Stats()

	writeJSON(w, http.StatusOK, out)
}

// tailFrame is one line of the live tail.
type tailFrame struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
	Summary   string `json:"summary"`
	AgeMillis int64  `json:"age_ms"`
	Raw       string `json:"raw"`
}

func (s *Server) handleTail(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Without this an intermediary that buffers will hold the whole stream
	// until the connection closes, which for a live tail means forever.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	records, unsubscribe := s.tail.Subscribe()
	defer unsubscribe()

	keepalive := time.NewTicker(s.tailKeepalive)
	defer keepalive.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return

		case rec, open := <-records:
			if !open {
				return
			}
			frame, err := marshalFrame(toFrame(rec, time.Now()))
			if err != nil {
				s.log.Warn("skipping an unencodable tail record", "offset", rec.Offset, "error", err)
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", frame); err != nil {
				return
			}
			flusher.Flush()

		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// parseTemplate and marshalFrame are variables so their failure branches have a
// reachable input.
//
// Neither can fail in production — the template is embedded and compiled into
// the binary, and tailFrame is all scalars and strings — but "cannot fail" is
// the reason those branches would otherwise never be executed even once, and an
// error path that has never run is an error path nobody has checked does the
// right thing. Here the right thing is specific and worth pinning: a template
// failure must stop the SERVER at boot, while a frame failure must skip ONE
// message and leave the stream open.
var (
	parseTemplate = func() (*template.Template, error) {
		return template.ParseFS(assetsFS, "assets/index.html")
	}
	marshalFrame = func(f tailFrame) ([]byte, error) { return json.Marshal(f) }
)
