// Package external provides mock implementations of external services.
package external

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"courier-service/internal/domain"
)

// OrderServiceMock emulates the external Order Service with a fixed 200ms delay.
// It loads order mappings from orders.yml into an in-memory database.
type OrderServiceMock struct {
	mu     sync.RWMutex
	orders map[int64]*domain.Order
	delay  time.Duration
}

type ordersFile struct {
	Orders []domain.Order `yaml:"orders"`
}

// NewOrderServiceMock loads order data from the specified YAML file.
func NewOrderServiceMock(filePath string) (*OrderServiceMock, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		// Fallback check in current or parent directory
		altPath := "./orders.yml"
		if altData, altErr := os.ReadFile(altPath); altErr == nil {
			data = altData
		} else {
			return nil, fmt.Errorf("reading orders file %q: %w", filePath, err)
		}
	}

	var parsed ordersFile
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("parsing orders yaml: %w", err)
	}

	ordersMap := make(map[int64]*domain.Order, len(parsed.Orders))
	for _, o := range parsed.Orders {
		orderCopy := o
		ordersMap[o.ID] = &orderCopy
	}

	return &OrderServiceMock{
		orders: ordersMap,
		delay:  200 * time.Millisecond,
	}, nil
}

// NewOrderServiceMockFromOrders creates a mock with the provided in-memory order slice (primarily for testing).
func NewOrderServiceMockFromOrders(orders []domain.Order, delay time.Duration) *OrderServiceMock {
	ordersMap := make(map[int64]*domain.Order, len(orders))
	for _, o := range orders {
		orderCopy := o
		ordersMap[o.ID] = &orderCopy
	}
	return &OrderServiceMock{
		orders: ordersMap,
		delay:  delay,
	}
}

// GetOrderByID retrieves an order and its assigned courier, simulating 200ms latency.
func (m *OrderServiceMock) GetOrderByID(ctx context.Context, orderID int64) (*domain.Order, error) {
	if m.delay > 0 {
		timer := time.NewTimer(m.delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("order service context canceled: %w", ctx.Err())
		case <-timer.C:
		}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	order, exists := m.orders[orderID]
	if !exists {
		return nil, fmt.Errorf("%w: id %d", domain.ErrOrderNotFound, orderID)
	}

	// Return a copy to prevent mutation
	return &domain.Order{
		ID:        order.ID,
		CourierID: order.CourierID,
	}, nil
}
