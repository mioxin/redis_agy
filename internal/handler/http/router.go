// Package http provides HTTP handlers and routing for the REST API.
package http

import (
	"net/http"
	"time"

	"courier-service/internal/handler/middleware"
)

// NewRouter sets up the HTTP multiplexer with middleware and registered application routes.
func NewRouter(
	locationHandler *LocationHandler,
	healthHandler *HealthHandler,
	slaLimit time.Duration,
) http.Handler {
	mux := http.NewServeMux()

	// REST API endpoint
	mux.HandleFunc("GET /orders/{orderId}/courier-location", locationHandler.GetCourierLocation)

	// Health and observability endpoints
	mux.HandleFunc("GET /health", healthHandler.HealthCheck)
	mux.HandleFunc("GET /healthz", healthHandler.HealthCheck)

	// Middleware chain: Recovery -> LatencyLogger
	var handler http.Handler = mux
	handler = middleware.LatencyLogger(slaLimit)(handler)
	handler = middleware.Recovery(handler)

	return handler
}
