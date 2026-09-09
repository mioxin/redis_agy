package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"courier-service/internal/handler/middleware"
)

func TestLatencyLogger(t *testing.T) {
	innerHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	handler := middleware.LatencyLogger(100 * time.Millisecond)(innerHandler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	responseTime := rec.Header().Get("X-Response-Time")
	if responseTime == "" {
		t.Fatal("expected X-Response-Time header to be set, got empty")
	}

	if !strings.HasSuffix(responseTime, "ms") {
		t.Fatalf("expected X-Response-Time to end with 'ms', got: %q", responseTime)
	}
}
