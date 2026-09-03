package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The provisioned Grafana dashboard, read from disk rather than restated here.
const dashboardPath = "../../grafana/dashboards/kafka-lab.json"

type dashboard struct {
	Panels []struct {
		ID      int    `json:"id"`
		Title   string `json:"title"`
		Targets []struct {
			Expr string `json:"expr"`
		} `json:"targets"`
	} `json:"panels"`
}

func loadDashboard(t *testing.T) dashboard {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(dashboardPath))
	if err != nil {
		t.Fatalf("the provisioned dashboard is unreadable: %v", err)
	}
	var d dashboard
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("the provisioned dashboard is not valid JSON: %v", err)
	}
	if len(d.Panels) == 0 {
		t.Fatal("the dashboard declares no panels; this guard would pass vacuously")
	}
	return d
}

// everyPublishedSeries is every metric name any role registers.
func everyPublishedSeries(t *testing.T) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, role := range []Role{RoleProducer, RoleConsumer, RoleAdmin} {
		s := New(role)
		touchAll(s)
		for n := range names(t, s) {
			out[n] = true
		}
	}
	return out
}

var seriesInExpr = regexp.MustCompile(`kafka_lab_[a-z0-9_]+`)

// A PANEL QUERYING A SERIES NOTHING PUBLISHES RENDERS AN EMPTY GRAPH, SILENTLY.
// That is the worst shape a dashboard failure can take: no error, no gap, just a
// flat absence that reads as "nothing happened" — which for a panel about
// duplicates and lost guarantees is the reassuring answer and the wrong one.
func TestEveryDashboardQueryNamesASeriesSomeRolePublishes(t *testing.T) {
	published := everyPublishedSeries(t)
	d := loadDashboard(t)

	var queried int
	for _, p := range d.Panels {
		for _, target := range p.Targets {
			for _, name := range seriesInExpr.FindAllString(target.Expr, -1) {
				queried++
				if !published[name] {
					t.Fatalf("panel %d (%q) queries %s, which no role registers; the panel would render empty",
						p.ID, p.Title, name)
				}
			}
		}
	}
	if queried == 0 {
		t.Fatal("no kafka_lab series were found in any panel expression")
	}
}

// THE DELIVERY-SEMANTICS PANEL MUST CARRY ALL FOUR LINES. The defect and the fix
// alone are not readable: a non-zero double-apply count on the dedupe arm means
// either a broken store or a store too small, and only the two loss lines say
// which. Dropping them would leave a panel that looks complete and cannot be
// interpreted.
func TestTheDeliverySemanticsPanelCarriesTheDefectTheFixAndBothLosses(t *testing.T) {
	d := loadDashboard(t)

	want := []string{
		"kafka_lab_double_applied_total",
		"kafka_lab_duplicates_suppressed_total",
		"kafka_lab_dedupe_evictions_total",
		"kafka_lab_dedupe_expiries_total",
	}

	for _, p := range d.Panels {
		var exprs []string
		for _, target := range p.Targets {
			exprs = append(exprs, target.Expr)
		}
		joined := strings.Join(exprs, " ")

		if !strings.Contains(joined, "kafka_lab_double_applied_total") {
			continue
		}
		var missing []string
		for _, w := range want {
			if !strings.Contains(joined, w) {
				missing = append(missing, w)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			t.Fatalf("panel %d (%q) plots the double-apply count without %v; the figure cannot be interpreted alone",
				p.ID, p.Title, missing)
		}
		return
	}
	t.Fatal("no panel plots kafka_lab_double_applied_total")
}

// Panel ids must be unique, or Grafana silently keeps one and drops the other.
func TestDashboardPanelIDsAreUnique(t *testing.T) {
	d := loadDashboard(t)
	seen := map[int]string{}
	for _, p := range d.Panels {
		if prior, dup := seen[p.ID]; dup {
			t.Fatalf("panel id %d is used by both %q and %q", p.ID, prior, p.Title)
		}
		seen[p.ID] = p.Title
	}
}
