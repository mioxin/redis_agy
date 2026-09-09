package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"courier-service/internal/domain"
	appHttp "courier-service/internal/handler/http"
	"courier-service/internal/handler/middleware"
)

func BenchmarkLocationHandler_CacheHit(b *testing.B) {
	loc := &domain.CourierLocation{
		OrderID:   42,
		CourierID: 7,
		Latitude:  55.75,
		Longitude: 37.61,
		UpdatedAt: time.Now().UTC(),
	}
	service := &mockLocationService{location: loc, found: true}
	handler := appHttp.NewLocationHandler(service)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{orderId}/courier-location", handler.GetCourierLocation)
	chain := middleware.LatencyLogger(100 * time.Millisecond)(mux)

	req := httptest.NewRequest(http.MethodGet, "/orders/42/courier-location", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		chain.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("expected 200 OK, got: %d", rec.Code)
		}
	}
}

func BenchmarkLocationHandler_CacheMiss(b *testing.B) {
	service := &mockLocationService{location: nil, found: false}
	handler := appHttp.NewLocationHandler(service)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{orderId}/courier-location", handler.GetCourierLocation)
	chain := middleware.LatencyLogger(100 * time.Millisecond)(mux)

	req := httptest.NewRequest(http.MethodGet, "/orders/999/courier-location", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		chain.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			b.Fatalf("expected 202 Accepted, got: %d", rec.Code)
		}
	}
}

func BenchmarkLocationHandler_Parallel_SLA(b *testing.B) {
	loc := &domain.CourierLocation{
		OrderID:   42,
		CourierID: 7,
		Latitude:  55.75,
		Longitude: 37.61,
		UpdatedAt: time.Now().UTC(),
	}
	service := &mockLocationService{location: loc, found: true}
	handler := appHttp.NewLocationHandler(service)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{orderId}/courier-location", handler.GetCourierLocation)
	chain := middleware.LatencyLogger(100 * time.Millisecond)(mux)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		req := httptest.NewRequest(http.MethodGet, "/orders/42/courier-location", nil)
		for pb.Next() {
			start := time.Now()
			rec := httptest.NewRecorder()
			chain.ServeHTTP(rec, req)
			duration := time.Since(start)

			// SLA Validation: Each request must respond in < 100ms
			if duration >= 100*time.Millisecond {
				b.Errorf("SLA violation: request took %v, must be < 100ms", duration)
			}
			if rec.Code != http.StatusOK {
				b.Errorf("unexpected status: %d", rec.Code)
			}
		}
	})
}
