// Package http provides HTTP handlers and routing for the REST API.
//
// Endpoint Map:
// - GET  /orders/{orderId}/courier-location : High-performance hot read endpoint (< 100ms SLA, target ~1-5ms).
// - GET  /health, /healthz                 : Comprehensive health check (Redis connectivity + last sync time + uptime).
// - GET  /health/live                      : Kubernetes liveness probe (200 OK as long as HTTP process responds).
// - GET  /health/ready                     : Kubernetes readiness probe (checks Redis; 503 if downstream dependency down).
// - GET  /metrics                          : Prometheus pull metrics endpoint.
// - GET  /debug/pprof/*                    : Go runtime pprof profiling endpoints (enabled via PPROF_ENABLED=true).
//
// Middleware Execution Order (Outer -> Inner):
// 1. TraceMiddleware: Injects or propagates X-Trace-ID and context span.
// 2. Recovery: Catches panics, logs stack traces, and converts panics to 500 Internal Server Error.
// 3. LatencyLogger: Measures handler duration, appends X-Response-Time header, updates Prometheus, and warns on SLA breach.
package http

import (
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"courier-service/internal/handler/middleware"
)

// NewRouter sets up the HTTP multiplexer with middleware, registered application routes,
// Prometheus metrics, and optional pprof debugging endpoints.
func NewRouter(
	locationHandler *LocationHandler,
	healthHandler *HealthHandler,
	slaLimit time.Duration,
	pprofEnabled bool,
) http.Handler {
	mux := http.NewServeMux()

	// REST API endpoint: O(1) Redis read path
	mux.HandleFunc("GET /orders/{orderId}/courier-location", locationHandler.GetCourierLocation)

	// Health and Kubernetes liveness/readiness probes
	mux.HandleFunc("GET /health", healthHandler.HealthCheck)
	mux.HandleFunc("GET /healthz", healthHandler.HealthCheck)
	mux.HandleFunc("GET /health/live", healthHandler.LivenessCheck)
	mux.HandleFunc("GET /health/ready", healthHandler.ReadinessCheck)

	// Prometheus metrics endpoint (polled by Prometheus server)
	mux.Handle("GET /metrics", promhttp.Handler())

	// pprof performance profiling endpoints (PPROF_ENABLED)
	// Used for CPU, memory heap, goroutine, and execution trace profiling in dev and staging.
	if pprofEnabled {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("POST /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}

	// Middleware chain: Trace -> Recovery -> LatencyLogger
	// Wrap in reverse order so that Trace runs first, Recovery catches inner panics, and LatencyLogger measures total time.
	var handler http.Handler = mux
	handler = middleware.LatencyLogger(slaLimit)(handler)
	handler = middleware.Recovery(handler)
	handler = middleware.TraceMiddleware(handler)

	return handler
}
