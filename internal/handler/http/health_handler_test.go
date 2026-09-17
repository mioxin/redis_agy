package http_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpHandler "courier-service/internal/handler/http"
	"courier-service/internal/repository/cache"
)

type mockFailingCache struct {
	cache.LocationCacheRepository
	shouldFailPing bool
}

func (m *mockFailingCache) Ping(ctx context.Context) error {
	if m.shouldFailPing {
		return errors.New("redis connection refused")
	}
	return nil
}

func (m *mockFailingCache) GetLastSync(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

func TestHealthHandler_LivenessAndReadiness(t *testing.T) {
	mockCache := &mockFailingCache{shouldFailPing: false}
	h := httpHandler.NewHealthHandler(mockCache)

	// 1. Liveness check: always returns 200 OK
	liveReq := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	liveRec := httptest.NewRecorder()
	h.LivenessCheck(liveRec, liveReq)

	if liveRec.Code != http.StatusOK {
		t.Fatalf("expected liveness 200 OK, got: %d", liveRec.Code)
	}
	if !strings.Contains(liveRec.Body.String(), `"status":"alive"`) {
		t.Fatalf("unexpected liveness body: %s", liveRec.Body.String())
	}

	// 2. Readiness check when healthy: returns 200 OK
	readyReq := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	readyRec := httptest.NewRecorder()
	h.ReadinessCheck(readyRec, readyReq)

	if readyRec.Code != http.StatusOK {
		t.Fatalf("expected readiness 200 OK, got: %d", readyRec.Code)
	}
	if !strings.Contains(readyRec.Body.String(), `"status":"ready"`) {
		t.Fatalf("unexpected readiness body: %s", readyRec.Body.String())
	}

	// 3. Readiness check when Redis fails: returns 503 Service Unavailable
	mockCache.shouldFailPing = true
	readyFailReq := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	readyFailRec := httptest.NewRecorder()
	h.ReadinessCheck(readyFailRec, readyFailReq)

	if readyFailRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected readiness 503 Service Unavailable when Redis fails, got: %d", readyFailRec.Code)
	}
	if !strings.Contains(readyFailRec.Body.String(), `"status":"not ready"`) {
		t.Fatalf("unexpected readiness failure body: %s", readyFailRec.Body.String())
	}

	// 4. Backward compatible /health endpoint
	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthRec := httptest.NewRecorder()
	h.HealthCheck(healthRec, healthReq)

	if healthRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected healthcheck 503 when Redis is down, got: %d", healthRec.Code)
	}
}
