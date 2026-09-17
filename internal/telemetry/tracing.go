// Package telemetry provides Prometheus metrics and tracing abstractions.
package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"

	"github.com/google/uuid"
)

type contextKey string

const (
	TraceIDKey contextKey = "trace_id"
	SpanIDKey  contextKey = "span_id"
)

// GenerateTraceID returns a new UUID v4 trace identifier.
func GenerateTraceID() string {
	return uuid.New().String()
}

// GenerateSpanID returns a random 16-hex character span identifier.
func GenerateSpanID() string {
	bytes := make([]byte, 8)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// ContextWithTrace returns a derived context holding traceID and spanID.
func ContextWithTrace(ctx context.Context, traceID, spanID string) context.Context {
	ctx = context.WithValue(ctx, TraceIDKey, traceID)
	return context.WithValue(ctx, SpanIDKey, spanID)
}

// TraceIDFromContext extracts traceID from context, or empty string if not found.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(TraceIDKey).(string); ok {
		return v
	}
	return ""
}

// SpanIDFromContext extracts spanID from context, or empty string if not found.
func SpanIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(SpanIDKey).(string); ok {
		return v
	}
	return ""
}

// LoggerWithTrace returns an slog.Logger instance enriched with trace_id and span_id if present.
func LoggerWithTrace(ctx context.Context) *slog.Logger {
	logger := slog.Default()
	traceID := TraceIDFromContext(ctx)
	spanID := SpanIDFromContext(ctx)

	if traceID != "" && spanID != "" {
		return logger.With(slog.String("trace_id", traceID), slog.String("span_id", spanID))
	} else if traceID != "" {
		return logger.With(slog.String("trace_id", traceID))
	}
	return logger
}
