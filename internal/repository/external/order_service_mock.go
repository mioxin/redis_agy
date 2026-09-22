// Package external provides mock implementations of external services.
package external

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"courier-service/internal/domain"
)

// ErrOrderServiceSimulatedFailure is returned when chaos injection triggers an error.
var ErrOrderServiceSimulatedFailure = errors.New("simulated order service 500 internal server error")

// OrderServiceMock emulates the external Order Service with a configurable delay and chaos injection.
// It loads order mappings from orders.yml into an in-memory database.
type OrderServiceMock struct {
	mu        sync.RWMutex
	orders    map[int64]*domain.Order
	delay     time.Duration
	errorRate float64
	jitter    time.Duration
	rng       *rand.Rand
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
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
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
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SetChaosParams configures failure rate and latency jitter for resilience testing.
func (m *OrderServiceMock) SetChaosParams(errorRate float64, jitter time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorRate = errorRate
	m.jitter = jitter
}

// GetOrderByID retrieves an order and its assigned courier, simulating latency and chaos injection.
func (m *OrderServiceMock) GetOrderByID(ctx context.Context, orderID int64) (*domain.Order, error) {
	m.mu.Lock()
	errRate := m.errorRate
	jitter := m.jitter
	delay := m.delay
	var shouldFail bool
	if errRate > 0 && m.rng.Float64() < errRate {
		shouldFail = true
	}
	if jitter > 0 {
		jitterOffset := time.Duration((m.rng.Float64()*2 - 1) * float64(jitter))
		delay += jitterOffset
		if delay < 0 {
			delay = 0
		}
	}
	m.mu.Unlock()

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("order service context canceled: %w", ctx.Err())
		case <-timer.C:
		}
	}

	if shouldFail {
		return nil, ErrOrderServiceSimulatedFailure
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

// GetAllOrders returns all orders loaded from orders.yml as a copy-safe slice.
// Used for cache pre-warming at server startup so that known orders are immediately
// available in the cache without requiring a client request (Cache Miss avoidance).
func (m *OrderServiceMock) GetAllOrders() []domain.Order {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]domain.Order, 0, len(m.orders))
	for _, o := range m.orders {
		result = append(result, domain.Order{
			ID:        o.ID,
			CourierID: o.CourierID,
		})
	}
	return result
}
