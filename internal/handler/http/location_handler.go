// Package http provides HTTP handlers and routing for the REST API.
package http

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"courier-service/internal/domain"
	"courier-service/internal/service"
)

// LocationHandler handles HTTP requests for courier location tracking.
type LocationHandler struct {
	locationService service.LocationService
}

// NewLocationHandler creates a new LocationHandler.
func NewLocationHandler(locationService service.LocationService) *LocationHandler {
	return &LocationHandler{
		locationService: locationService,
	}
}

// GetCourierLocation handles GET /orders/{orderId}/courier-location.
// Adheres strictly to the SLA < 100ms requirement:
// - Cache Hit: returns HTTP 200 OK with location (~2ms).
// - Cache Miss: returns HTTP 202 Accepted with Retry-After: 1 header (~2ms) and initiates async tracking.
func (h *LocationHandler) GetCourierLocation(w http.ResponseWriter, r *http.Request) {
	orderIDStr := r.PathValue("orderId")
	orderID, err := strconv.ParseInt(orderIDStr, 10, 64)
	if err != nil || orderID <= 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "invalid order id, must be a positive integer",
		})
		return
	}

	location, found, err := h.locationService.GetCourierLocation(r.Context(), orderID)
	if err != nil {
		slog.Error("Failed to get courier location",
			slog.Int64("order_id", orderID),
			slog.String("error", err.Error()),
		)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "internal server error",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if !found {
		// Cache Miss: inform client to retry shortly while background poller warms the cache
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(domain.TrackingResponse{
			Status:  "pending",
			Message: "Location tracking started, coordinates will be available shortly",
		})
		return
	}

	// Cache Hit: coordinates ready
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(location)
}
