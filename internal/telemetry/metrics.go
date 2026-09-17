// Package telemetry provides Prometheus metrics and tracing abstractions.
package telemetry

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal counts total HTTP requests serviced.
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "courier_http_requests_total",
			Help: "Total number of HTTP requests processed by courier service.",
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestDuration tracks latency of HTTP requests with fine-grained SLA buckets.
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "courier_http_request_duration_seconds",
			Help: "Duration of HTTP requests in seconds.",
			Buckets: []float64{
				0.0005, // 0.5ms
				0.001,  // 1ms
				0.002,  // 2ms
				0.005,  // 5ms
				0.01,   // 10ms
				0.025,  // 25ms
				0.05,   // 50ms
				0.075,  // 75ms
				0.1,    // 100ms (SLA threshold)
				0.25,   // 250ms
				0.5,    // 500ms
				1.0,    // 1s
			},
		},
		[]string{"method", "path"},
	)

	// SLABreachesTotal counts the number of HTTP requests that breached the 100ms SLA limit.
	SLABreachesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "courier_sla_breaches_total",
			Help: "Total number of requests exceeding the SLA latency threshold (100ms).",
		},
	)

	// CacheHitsTotal tracks successful cache reads.
	CacheHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "courier_cache_hits_total",
			Help: "Total count of cache hits on location lookups.",
		},
	)

	// CacheMissesTotal tracks cache misses requiring async or sync population.
	CacheMissesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "courier_cache_misses_total",
			Help: "Total count of cache misses on location lookups.",
		},
	)

	// ActiveOrdersGauge indicates current count of tracked active orders.
	ActiveOrdersGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "courier_active_orders_gauge",
			Help: "Current number of active orders actively being tracked/polled.",
		},
	)

	// PollerLastSyncGauge stores the Unix epoch timestamp of the last poller sync.
	PollerLastSyncGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "courier_poller_last_sync_timestamp",
			Help: "Unix timestamp of the most recent successful background poller synchronization.",
		},
	)
)

// RecordHTTPRequest records latency, status code, and counter metrics for an HTTP request.
func RecordHTTPRequest(method, path string, statusCode int, duration time.Duration) {
	statusStr := strconv.Itoa(statusCode)
	HTTPRequestsTotal.WithLabelValues(method, path, statusStr).Inc()
	HTTPRequestDuration.WithLabelValues(method, path).Observe(duration.Seconds())
}

// RecordSLABreach increments the SLA violation counter.
func RecordSLABreach() {
	SLABreachesTotal.Inc()
}

// IncCacheHit increments the cache hit counter.
func IncCacheHit() {
	CacheHitsTotal.Inc()
}

// IncCacheMiss increments the cache miss counter.
func IncCacheMiss() {
	CacheMissesTotal.Inc()
}

// SetActiveOrders updates the active orders gauge.
func SetActiveOrders(count int) {
	ActiveOrdersGauge.Set(float64(count))
}

// SetLastSyncTimestamp updates the poller last sync timestamp gauge.
func SetLastSyncTimestamp(t time.Time) {
	PollerLastSyncGauge.Set(float64(t.Unix()))
}
