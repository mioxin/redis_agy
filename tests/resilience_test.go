package tests_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"courier-service/internal/config"
	"courier-service/internal/domain"
	httpHandler "courier-service/internal/handler/http"
	"courier-service/internal/repository/cache"
	"courier-service/internal/repository/external"
	"courier-service/internal/service"
)

// TestResilience_SLAUnderChaos verifies that even during external chaos (10% failures in upstream services),
// client requests for cached orders consistently succeed with SLA < 100ms.
func TestResilience_SLAUnderChaos(t *testing.T) {
	cfg := &config.Config{
		SLALimit:                  100 * time.Millisecond,
		LocationTTL:               120 * time.Second,
		OrderCourierMappingTTL:    1 * time.Hour,
		HeartbeatTTL:              90 * time.Second,
		WorkerConcurrency:         5,
		UrgentQueueSize:           100,
		CircuitBreakerMaxFailures: 5,
		CircuitBreakerTimeout:     15 * time.Second,
	}

	orders := []domain.Order{
		{ID: 1001, CourierID: 501},
	}
	orderMock := external.NewOrderServiceMockFromOrders(orders, 0)
	courierMock := external.NewCourierServiceMockWithDelay(0)

	// Inject 10% chaos into courier service
	courierMock.SetChaosParams(0.10, 5*time.Millisecond)

	memCache := cache.NewMemoryCacheRepository()

	cbOrder := external.NewCircuitBreakerOrderService(orderMock, cfg.CircuitBreakerMaxFailures, cfg.CircuitBreakerTimeout)
	cbCourier := external.NewCircuitBreakerCourierService(courierMock, cfg.CircuitBreakerMaxFailures, cfg.CircuitBreakerTimeout)

	poller := service.NewPollerService(memCache, cbOrder, cbCourier, cfg)
	locationSvc := service.NewLocationService(memCache, poller, cfg)

	// Pre-populate cache for order 1001
	_ = memCache.SetOrderLocation(context.Background(), domain.CourierLocation{
		OrderID:   1001,
		CourierID: 501,
		Latitude:  55.75,
		Longitude: 37.61,
		UpdatedAt: time.Now(),
	}, cfg.LocationTTL)

	hdl := httpHandler.NewLocationHandler(locationSvc)
	healthHdl := httpHandler.NewHealthHandler(memCache)
	router := httpHandler.NewRouter(hdl, healthHdl, cfg.SLALimit, true)


	// Send 100 requests to test resilience & latency
	const totalRequests = 100
	for i := 0; i < totalRequests; i++ {
		start := time.Now()
		req := httptest.NewRequest(http.MethodGet, "/orders/1001/courier-location", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)
		elapsed := time.Since(start)

		if rec.Code != http.StatusOK {
			t.Fatalf("request %d failed with code %d: %s", i+1, rec.Code, rec.Body.String())
		}

		if elapsed > cfg.SLALimit {
			t.Fatalf("request %d violated SLA: took %v (limit %v)", i+1, elapsed, cfg.SLALimit)
		}

		// Fast path SLA check: should typically be < 10ms for in-memory / redis hit
		if elapsed > 10*time.Millisecond {
			t.Logf("Notice: request %d took %v", i+1, elapsed)
		}
	}
}
