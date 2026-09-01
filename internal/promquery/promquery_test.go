package promquery

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func serve(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return New(srv.URL, 2*time.Second)
}

func successBody(value string) string {
	return fmt.Sprintf(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,%q]}]}}`, value)
}

func TestScalarReadsTheFirstSample(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, successBody("42.5"))
	})
	got, err := c.Scalar(context.Background(), "up")
	if err != nil {
		t.Fatalf("scalar: %v", err)
	}
	if got != 42.5 {
		t.Fatalf("got %v want 42.5", got)
	}
}

func TestScalarSendsTheQueryAsAParameter(t *testing.T) {
	var seen string
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		seen = r.URL.Query().Get("query")
		if r.URL.Path != "/api/v1/query" {
			t.Errorf("path: got %q want /api/v1/query", r.URL.Path)
		}
		fmt.Fprint(w, successBody("1"))
	})
	q := `rate(kafka_lab_produced_total{service="producer"}[30s])`
	if _, err := c.Scalar(context.Background(), q); err != nil {
		t.Fatalf("scalar: %v", err)
	}
	if seen != q {
		t.Fatalf("query: got %q want %q", seen, q)
	}
}

// FOR THE FIRST THIRTY SECONDS of the lab's life, rate() over a counter with
// one sample returns nothing. An error banner during normal startup trains the
// reader to ignore the banner.
func TestEmptyResultIsZeroNotAnError(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	})
	got, err := c.Scalar(context.Background(), "rate(x[30s])")
	if err != nil {
		t.Fatalf("an empty vector must not be an error: %v", err)
	}
	if got != 0 {
		t.Fatalf("got %v want 0", got)
	}
}

// Prometheus carries the sample value as a JSON STRING because NaN and +Inf are
// not JSON numbers. A client that expected a number would work in testing and
// fail on the first saturated counter.
func TestNonFiniteSampleValuesParse(t *testing.T) {
	for _, v := range []string{"NaN", "+Inf", "-Inf"} {
		c := serve(t, func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, successBody(v))
		})
		if _, err := c.Scalar(context.Background(), "x"); err != nil {
			t.Fatalf("%s must parse: %v", v, err)
		}
	}
}

func TestErrorStatusFromPrometheusIsReported(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"error","errorType":"bad_data","error":"parse error at char 1"}`)
	})
	_, err := c.Scalar(context.Background(), "((")
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "parse error at char 1") {
		t.Fatalf("the error must carry what Prometheus said: %v", err)
	}
}

func TestNonOKStatusIsReportedWithTheBody(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprint(w, "upstream is down")
	})
	_, err := c.Scalar(context.Background(), "up")
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"502", "upstream is down"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %v must contain %q", err, want)
		}
	}
}

func TestMalformedJSONIsReported(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "<html>not prometheus</html>")
	})
	if _, err := c.Scalar(context.Background(), "up"); err == nil {
		t.Fatal("want an error")
	}
}

func TestNonStringSampleValueIsReported(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1700000000,42]}]}}`)
	})
	_, err := c.Scalar(context.Background(), "up")
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "not a string") {
		t.Fatalf("got %v", err)
	}
}

func TestUnparseableSampleValueIsReported(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, successBody("not-a-float"))
	})
	_, err := c.Scalar(context.Background(), "up")
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "not-a-float") {
		t.Fatalf("the error must quote the sample it could not parse: %v", err)
	}
}

func TestTransportFailureIsReported(t *testing.T) {
	c := New("http://127.0.0.1:1", 200*time.Millisecond)
	if _, err := c.Scalar(context.Background(), "up"); err == nil {
		t.Fatal("want an error against a closed port")
	}
}

func TestCancelledContextIsReported(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, successBody("1"))
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Scalar(ctx, "up"); err == nil {
		t.Fatal("want an error for a cancelled context")
	}
}

func TestBadBaseURLIsReportedAtRequestBuild(t *testing.T) {
	c := New("http://\x7f invalid", time.Second)
	_, err := c.Scalar(context.Background(), "up")
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "build request") {
		t.Fatalf("got %v", err)
	}
}

func TestNewTrimsTrailingSlashes(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		fmt.Fprint(w, successBody("1"))
	}))
	defer srv.Close()

	c := New(srv.URL+"///", time.Second)
	if _, err := c.Scalar(context.Background(), "up"); err != nil {
		t.Fatalf("scalar: %v", err)
	}
	if path != "/api/v1/query" {
		t.Fatalf("a trailing slash produced %q", path)
	}
}

// A misconfigured URL pointing at something that is not Prometheus must not be
// able to exhaust admin's memory.
func TestResponseBodyIsBounded(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		chunk := strings.Repeat("a", 64*1024)
		for written := 0; written < 4*maxBody; written += len(chunk) {
			if _, err := w.Write([]byte(chunk)); err != nil {
				return
			}
		}
	})
	// It must FAIL (the truncated body is not valid JSON) rather than read
	// gigabytes. The assertion that matters is that it returns at all.
	done := make(chan error, 1)
	go func() { _, err := c.Scalar(context.Background(), "up"); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a truncated body must not parse as success")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Scalar did not bound the response body")
	}
}

func TestQueryEncodingSurvivesSpecialCharacters(t *testing.T) {
	q := `sum(rate(x{a="b c",d="e&f"}[1m]))`
	var seen string
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		seen = r.URL.Query().Get("query")
		fmt.Fprint(w, successBody("1"))
	})
	if _, err := c.Scalar(context.Background(), q); err != nil {
		t.Fatalf("scalar: %v", err)
	}
	if seen != q {
		t.Fatalf("got %q want %q", seen, q)
	}
	if _, err := url.Parse("?query=" + url.QueryEscape(q)); err != nil {
		t.Fatalf("encoding: %v", err)
	}
}

// A response that is cut off mid-body must be reported as a read failure, not
// parsed as whatever arrived. The server declares a length it does not deliver
// and hangs up, which is what a Prometheus container being restarted under a
// poll looks like from admin's side.
func TestTruncatedBodyIsReportedAsAReadFailure(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		conn, buf, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer conn.Close()
		fmt.Fprint(buf, "HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n")
		fmt.Fprint(buf, `{"status":"suc`)
		buf.Flush()
	})
	_, err := c.Scalar(context.Background(), "up")
	if err == nil {
		t.Fatal("want an error for a truncated body")
	}
	if !strings.Contains(err.Error(), "read body") {
		t.Fatalf("got %v, want a read-body failure", err)
	}
}
