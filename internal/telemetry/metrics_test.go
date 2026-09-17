package telemetry_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"courier-service/internal/telemetry"
)

func TestMetrics_RecordingAndScraping(t *testing.T) {
	// Record some sample metrics
	telemetry.RecordHTTPRequest("GET", "/orders/1/courier-location", 200, 2*time.Millisecond)
	telemetry.RecordHTTPRequest("GET", "/orders/2/courier-location", 504, 150*time.Millisecond)
	telemetry.RecordSLABreach()
	telemetry.IncCacheHit()
	telemetry.IncCacheMiss()
	telemetry.SetActiveOrders(42)
	telemetry.SetLastSyncTimestamp(time.Unix(1700000000, 0))

	// Scrape /metrics endpoint
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()

	handler := promhttp.Handler()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got: %d", rr.Code)
	}

	body := rr.Body.String()

	// Verify required metrics are exported
	expectedMetrics := []string{
		"courier_http_requests_total",
		"courier_http_request_duration_seconds",
		"courier_sla_breaches_total",
		"courier_cache_hits_total",
		"courier_cache_misses_total",
		"courier_active_orders_gauge",
		"courier_poller_last_sync_timestamp",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(body, metric) {
			t.Errorf("expected scraped metrics to contain %q, but was missing", metric)
		}
	}
}
