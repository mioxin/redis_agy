package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"courier-service/internal/domain"
	appHttp "courier-service/internal/handler/http"
)

type mockLocationService struct {
	location *domain.CourierLocation
	found    bool
	err      error
}

func (m *mockLocationService) GetCourierLocation(ctx context.Context, orderID int64) (*domain.CourierLocation, bool, error) {
	if m.err != nil {
		return nil, false, m.err
	}
	return m.location, m.found, nil
}

func TestLocationHandler_GetCourierLocation(t *testing.T) {
	t.Run("Cache Hit returns 200 OK with location", func(t *testing.T) {
		loc := &domain.CourierLocation{
			OrderID:   42,
			CourierID: 7,
			Latitude:  55.75,
			Longitude: 37.61,
			UpdatedAt: time.Now().UTC(),
		}
		service := &mockLocationService{location: loc, found: true}
		handler := appHttp.NewLocationHandler(service)

		router := http.NewServeMux()
		router.HandleFunc("GET /orders/{orderId}/courier-location", handler.GetCourierLocation)

		req := httptest.NewRequest(http.MethodGet, "/orders/42/courier-location", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got: %d", rec.Code)
		}

		var resp domain.CourierLocation
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response body: %v", err)
		}
		if resp.OrderID != 42 || resp.CourierID != 7 {
			t.Fatalf("unexpected body: %+v", resp)
		}
	})

	t.Run("Cache Miss returns 202 Accepted with Retry-After header", func(t *testing.T) {
		service := &mockLocationService{location: nil, found: false}
		handler := appHttp.NewLocationHandler(service)

		router := http.NewServeMux()
		router.HandleFunc("GET /orders/{orderId}/courier-location", handler.GetCourierLocation)

		req := httptest.NewRequest(http.MethodGet, "/orders/42/courier-location", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected status 202, got: %d", rec.Code)
		}

		retryAfter := rec.Header().Get("Retry-After")
		if retryAfter != "1" {
			t.Fatalf("expected Retry-After: 1, got: %q", retryAfter)
		}

		var resp domain.TrackingResponse
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response body: %v", err)
		}
		if resp.Status != "pending" {
			t.Fatalf("expected status 'pending', got: %q", resp.Status)
		}
	})

	t.Run("Invalid order ID returns 400 Bad Request", func(t *testing.T) {
		service := &mockLocationService{}
		handler := appHttp.NewLocationHandler(service)

		router := http.NewServeMux()
		router.HandleFunc("GET /orders/{orderId}/courier-location", handler.GetCourierLocation)

		req := httptest.NewRequest(http.MethodGet, "/orders/not-a-number/courier-location", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got: %d", rec.Code)
		}
	})
}
