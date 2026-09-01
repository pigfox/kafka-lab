// Package promquery reads instant queries from a Prometheus HTTP API.
//
// WHY ADMIN ASKS PROMETHEUS RATHER THAN THE SERVICES. The admin UI shows a
// produced/sec and a consumed/sec counter, and the obvious way to get them is
// for admin to scrape http://producer:2112/metrics itself and difference the
// counter between polls. That is rejected here for two reasons, and the second
// is the load-bearing one:
//
//  1. It would reimplement, badly, the rate() Prometheus already computes —
//     including counter resets on restart, which a naive difference reads as a
//     enormous negative spike.
//  2. IT WOULD BE A SECOND CONTROL-PLANE-SHAPED CHANNEL. This lab's whole claim
//     is that the services talk over their own bus. An admin holding direct
//     HTTP connections to producer and consumer makes that claim visibly
//     untrue to anyone reading the compose file, whatever the connections are
//     carrying. Metrics flow on the metrics plane; settings flow on the bus;
//     admin reads both and originates neither.
//
// The one figure admin does NOT get this way is consumer group lag, because
// that is a broker fact rather than a service fact — admin asks the group
// coordinator directly and publishes the answer as its own metric.
package promquery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is a minimal Prometheus instant-query reader. It deliberately does not
// depend on the official API client: this needs one endpoint and one response
// shape, and the dependency would be larger than the code.
type Client struct {
	base string
	http *http.Client
}

// New returns a client for a Prometheus base URL such as http://prometheus:9090.
func New(base string, timeout time.Duration) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		http: &http.Client{Timeout: timeout},
	}
}

// maxBody bounds what a response may be. Prometheus answering an instant query
// returns a few hundred bytes; anything unbounded here is a way for a
// misconfigured URL pointing at some other service to exhaust admin's memory.
const maxBody = 1 << 20

type apiResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value [2]json.RawMessage `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// Scalar runs an instant query and returns the first sample's value.
//
// AN EMPTY RESULT IS ZERO, NOT AN ERROR, and that is a decision rather than
// laziness: for the first thirty seconds of the lab's life rate() over a
// counter with one sample returns nothing at all, and an admin UI that showed
// an error banner during normal startup would train the reader to ignore it.
// The absent case and the zero case are genuinely the same thing here — nothing
// has been produced yet.
func (c *Client) Scalar(ctx context.Context, query string) (float64, error) {
	u := c.base + "/api/v1/query?" + url.Values{"query": {query}}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, fmt.Errorf("promquery: build request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("promquery: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return 0, fmt.Errorf("promquery: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("promquery: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed apiResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return 0, fmt.Errorf("promquery: decode response: %w", err)
	}
	if parsed.Status != "success" {
		return 0, fmt.Errorf("promquery: %s: %s", parsed.Status, parsed.Error)
	}
	if len(parsed.Data.Result) == 0 {
		return 0, nil
	}

	// The sample is [unix_time, "value"] — the value is a JSON STRING, because
	// Prometheus carries NaN and +Inf, which JSON numbers cannot express.
	var raw string
	if err := json.Unmarshal(parsed.Data.Result[0].Value[1], &raw); err != nil {
		return 0, fmt.Errorf("promquery: sample value is not a string: %w", err)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("promquery: parse sample %q: %w", raw, err)
	}
	return v, nil
}
