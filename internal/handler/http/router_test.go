package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"courier-service/internal/domain"
	httpHandler "courier-service/internal/handler/http"
)

func TestRouter_Integration(t *testing.T) {
	mockCache := &mockFailingCache{shouldFailPing: false}
	healthHdl := httpHandler.NewHealthHandler(mockCache)
	locMock := &mockLocationService{
		found: true,
		location: &domain.CourierLocation{
			OrderID:   1,
			CourierID: 101,
			Latitude:  55.75,
			Longitude: 37.61,
			UpdatedAt: time.Now(),
		},
	}
	locationHdl := httpHandler.NewLocationHandler(locMock)

	router := httpHandler.NewRouter(locationHdl, healthHdl, 100*time.Millisecond, true)

	// 1. Test X-Trace-ID propagation and X-Response-Time header
	req := httptest.NewRequest(http.MethodGet, "/orders/1/courier-location", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got: %d", rec.Code)
	}

	traceID := rec.Header().Get("X-Trace-ID")
	if traceID == "" {
		t.Errorf("expected X-Trace-ID header in response")
	}

	responseTime := rec.Header().Get("X-Response-Time")
	if responseTime == "" {
		t.Errorf("expected X-Response-Time header in response")
	}

	// 2. Test provided X-Trace-ID preservation
	customTraceID := "custom-client-trace-12345"
	reqWithTrace := httptest.NewRequest(http.MethodGet, "/orders/1/courier-location", nil)
	reqWithTrace.Header.Set("X-Trace-ID", customTraceID)
	recWithTrace := httptest.NewRecorder()

	router.ServeHTTP(recWithTrace, reqWithTrace)

	if recWithTrace.Header().Get("X-Trace-ID") != customTraceID {
		t.Errorf("expected X-Trace-ID %q, got: %q", customTraceID, recWithTrace.Header().Get("X-Trace-ID"))
	}

	// 3. Test /metrics endpoint routing
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recMetrics := httptest.NewRecorder()
	router.ServeHTTP(recMetrics, reqMetrics)

	if recMetrics.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on /metrics, got: %d", recMetrics.Code)
	}

	// 4. Test /health/live
	reqLive := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	recLive := httptest.NewRecorder()
	router.ServeHTTP(recLive, reqLive)

	if recLive.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on /health/live, got: %d", recLive.Code)
	}

	// 5. Test /health/ready
	reqReady := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	recReady := httptest.NewRecorder()
	router.ServeHTTP(recReady, reqReady)

	if recReady.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on /health/ready, got: %d", recReady.Code)
	}

	// 6. Test pprof endpoints
	for _, pprofPath := range []string{
		"/debug/pprof/",
		"/debug/pprof/heap",
		"/debug/pprof/goroutine",
		"/debug/pprof/cmdline",
	} {
		reqPprof := httptest.NewRequest(http.MethodGet, pprofPath, nil)
		recPprof := httptest.NewRecorder()
		router.ServeHTTP(recPprof, reqPprof)

		if recPprof.Code != http.StatusOK {
			t.Errorf("expected 200 OK on pprof %s, got: %d", pprofPath, recPprof.Code)
		}
	}

	// 7. Test router with pprof disabled
	routerNoPprof := httpHandler.NewRouter(locationHdl, healthHdl, 100*time.Millisecond, false)
	reqPprofDisabled := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	recPprofDisabled := httptest.NewRecorder()
	routerNoPprof.ServeHTTP(recPprofDisabled, reqPprofDisabled)

	if recPprofDisabled.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found when pprof disabled, got: %d", recPprofDisabled.Code)
	}
}

