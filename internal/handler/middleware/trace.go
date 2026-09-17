// Package middleware provides HTTP middlewares for SLA monitoring, logging, and panic recovery.
package middleware

import (
	"net/http"

	"courier-service/internal/telemetry"
)

// TraceMiddleware extracts or generates X-Trace-ID and propagates it across the context and HTTP response.
//
// Tracing Architecture:
// 1. Checks incoming HTTP header "X-Trace-ID". If missing, generates a 32-character hexadecimal trace ID.
// 2. Generates a 16-character hexadecimal span ID for this request's local processing scope.
// 3. Injects both into the request context (ContextWithTrace).
// 4. Sets "X-Trace-ID" in the response header so callers can correlate client errors with server logs.
// 5. Downstream handlers use telemetry.LoggerWithTrace(ctx) so all slog log records automatically carry "trace_id" and "span_id".
func TraceMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.Header.Get("X-Trace-ID")
		if traceID == "" {
			traceID = telemetry.GenerateTraceID()
		}

		spanID := telemetry.GenerateSpanID()
		ctx := telemetry.ContextWithTrace(r.Context(), traceID, spanID)

		// Set header in response for end-to-end caller correlation
		w.Header().Set("X-Trace-ID", traceID)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
