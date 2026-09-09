// Package domain defines the core business entities for the courier location service.
package domain

// Order represents an order and its associated courier.
type Order struct {
	ID        int64 `json:"id" yaml:"id"`
	CourierID int64 `json:"courier_id" yaml:"courier_id"`
}
