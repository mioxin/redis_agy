// Package middleware provides HTTP middlewares for SLA monitoring, logging, and panic recovery.
package middleware

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"courier-service/internal/telemetry"
)

type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func newResponseWriterWrapper(w http.ResponseWriter) *responseWriterWrapper {
	return &responseWriterWrapper{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}
}

func (rw *responseWriterWrapper) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.statusCode = code
		rw.wroteHeader = true
		rw.ResponseWriter.WriteHeader(code)
	}
}

// LatencyLogger creates a middleware that calculates request latency, sets X-Response-Time header,
// records Prometheus metrics, and audits adherence to the SLA threshold (< 100ms).
//
// SLA Monitoring Mechanism:
// - Measures total elapsed time from handler entry to exit using time.Since(start).
// - Attaches "X-Response-Time: X.XXms" HTTP response header for client-side observability.
// - Increments telemetry.RecordHTTPRequest (Prometheus histogram & request counter).
// - If duration > slaLimit, fires a structured Warn log [SLA BREACH] and records telemetry.RecordSLABreach().
func LatencyLogger(slaLimit time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := newResponseWriterWrapper(w)

			// Execute downstream request handler
			next.ServeHTTP(wrapped, r)

			duration := time.Since(start)
			durationMs := float64(duration.Microseconds()) / 1000.0

			// Expose latency header to client
			w.Header().Set("X-Response-Time", fmt.Sprintf("%.2fms", durationMs))

			// Record Prometheus metrics
			telemetry.RecordHTTPRequest(r.Method, r.URL.Path, wrapped.statusCode, duration)

			logger := telemetry.LoggerWithTrace(r.Context())

			// SLA audit check (< 100ms)
			if duration > slaLimit {
				telemetry.RecordSLABreach()
				logger.Warn("[SLA BREACH]",
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Int("status", wrapped.statusCode),
					slog.Float64("duration_ms", durationMs),
					slog.Float64("sla_threshold_ms", float64(slaLimit.Milliseconds())),
				)
			} else {
				logger.Debug("[SLA OK]",
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Int("status", wrapped.statusCode),
					slog.Float64("duration_ms", durationMs),
				)
			}
		})
	}
}

