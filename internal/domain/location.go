// Package domain defines the core business entities for the courier location service.
package domain

import (
	"errors"
	"time"
)

// Domain errors.
var (
	ErrOrderNotFound   = errors.New("order not found")
	ErrCourierNotFound = errors.New("courier not found")
	ErrInvalidOrderID  = errors.New("invalid order id")
)

// Coordinates represents geographic latitude and longitude.
type Coordinates struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// CourierLocation represents the current location of a courier fulfilling a specific order.
type CourierLocation struct {
	OrderID   int64     `json:"order_id"`
	CourierID int64     `json:"courier_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TrackingResponse represents the asynchronous pending response returned during Cache Miss (SLA < 100ms).
type TrackingResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}
