// Package external provides mock implementations of external services.
package external

import (
	"context"

	"courier-service/internal/domain"
)

// OrderServiceClient defines the interface for interacting with the external Order Service.
// Per specification, Order Service only provides a single-item endpoint with ~200ms latency.
type OrderServiceClient interface {
	// GetOrderByID retrieves an order and its assigned courier by ID.
	GetOrderByID(ctx context.Context, orderID int64) (*domain.Order, error)
	// GetAllOrders returns all known orders for cache pre-warming at startup.
	GetAllOrders() []domain.Order
}

// CourierServiceClient defines the interface for interacting with the external Courier Service.
// Per specification, Courier Service only provides a single-item endpoint with ~500ms latency.
type CourierServiceClient interface {
	// GetCourierLocation retrieves current geographic coordinates of a courier by ID.
	GetCourierLocation(ctx context.Context, courierID int64) (*domain.Coordinates, error)
}
