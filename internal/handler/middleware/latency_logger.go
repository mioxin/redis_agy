// Package middleware provides HTTP middlewares for SLA monitoring, logging, and panic recovery.
package middleware

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"
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
// and audits adherence to the SLA threshold (< 100ms).
func LatencyLogger(slaLimit time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := newResponseWriterWrapper(w)

			// Execute request handler
			next.ServeHTTP(wrapped, r)

			duration := time.Since(start)
			durationMs := float64(duration.Microseconds()) / 1000.0

			// Expose latency header to client
			w.Header().Set("X-Response-Time", fmt.Sprintf("%.2fms", durationMs))

			// SLA audit check (< 100ms)
			if duration > slaLimit {
				slog.Warn("[SLA BREACH]",
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Int("status", wrapped.statusCode),
					slog.Float64("duration_ms", durationMs),
					slog.Float64("sla_threshold_ms", float64(slaLimit.Milliseconds())),
				)
			} else {
				slog.Debug("[SLA OK]",
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Int("status", wrapped.statusCode),
					slog.Float64("duration_ms", durationMs),
				)
			}
		})
	}
}
