// Package external provides mock implementations and resilience wrappers for external services.
//
// Circuit Breakers prevent cascading failures when external upstream dependencies
// (Legacy Order Service or Courier Geolocation API) slow down, time out, or go offline.
//
// State Machine (gobreaker):
// - CLOSED: Normal operation. Requests pass through directly. If consecutive failures reach maxFailures (e.g. 5), trips to OPEN.
// - OPEN: Fast-fail state. All incoming calls fail immediately with ErrCircuitBreakerOpen without touching upstream network.
//   After the configured cooldown timeout (e.g. 10s), transitions to HALF-OPEN.
// - HALF-OPEN: Canary probe state. Allows MaxRequests (1 probe request) through. If successful, state resets to CLOSED;
//   if it fails, state immediately returns to OPEN for another cooldown period.
package external

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sony/gobreaker/v2"

	"courier-service/internal/domain"
)

// ErrCircuitBreakerOpen is returned immediately when the circuit breaker is in OPEN state.
var ErrCircuitBreakerOpen = errors.New("circuit breaker is open: external service unavailable")

// CircuitBreakerOrderService wraps an OrderServiceClient with gobreaker.
// Protects the application from hanging goroutines when the legacy order service is slow or unresponsive.
type CircuitBreakerOrderService struct {
	underlying OrderServiceClient
	cb         *gobreaker.CircuitBreaker[*domain.Order]
}

// NewCircuitBreakerOrderService wraps the given OrderServiceClient with a circuit breaker.
func NewCircuitBreakerOrderService(
	underlying OrderServiceClient,
	maxFailures uint32,
	timeout time.Duration,
) *CircuitBreakerOrderService {
	settings := gobreaker.Settings{
		Name:        "OrderServiceCircuitBreaker",
		MaxRequests: 1, // in half-open state, probe with 1 canary request
		Timeout:     timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= maxFailures
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			slog.Warn("[CIRCUIT BREAKER STATE CHANGE]",
				slog.String("name", name),
				slog.String("from", from.String()),
				slog.String("to", to.String()),
			)
		},
	}

	return &CircuitBreakerOrderService{
		underlying: underlying,
		cb:         gobreaker.NewCircuitBreaker[*domain.Order](settings),
	}
}

// GetOrderByID executes the order lookup protected by the circuit breaker.
func (c *CircuitBreakerOrderService) GetOrderByID(ctx context.Context, orderID int64) (*domain.Order, error) {
	res, err := c.cb.Execute(func() (*domain.Order, error) {
		return c.underlying.GetOrderByID(ctx, orderID)
	})
	if err != nil {
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			return nil, fmt.Errorf("%w: %v", ErrCircuitBreakerOpen, err)
		}
		return nil, err
	}
	return res, nil
}

// CircuitBreakerCourierService wraps a CourierServiceClient with gobreaker.
// Protects the sync worker pipeline from getting stalled by high latency in the courier geolocation provider.
type CircuitBreakerCourierService struct {
	underlying CourierServiceClient
	cb         *gobreaker.CircuitBreaker[*domain.Coordinates]
}

// NewCircuitBreakerCourierService wraps the given CourierServiceClient with a circuit breaker.
func NewCircuitBreakerCourierService(
	underlying CourierServiceClient,
	maxFailures uint32,
	timeout time.Duration,
) *CircuitBreakerCourierService {
	settings := gobreaker.Settings{
		Name:        "CourierServiceCircuitBreaker",
		MaxRequests: 1, // in half-open state, probe with 1 canary request
		Timeout:     timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= maxFailures
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			slog.Warn("[CIRCUIT BREAKER STATE CHANGE]",
				slog.String("name", name),
				slog.String("from", from.String()),
				slog.String("to", to.String()),
			)
		},
	}

	return &CircuitBreakerCourierService{
		underlying: underlying,
		cb:         gobreaker.NewCircuitBreaker[*domain.Coordinates](settings),
	}
}

// GetCourierLocation executes courier coordinates lookup protected by the circuit breaker.
// If the breaker is OPEN, ErrCircuitBreakerOpen is returned immediately without network I/O.
func (c *CircuitBreakerCourierService) GetCourierLocation(ctx context.Context, courierID int64) (*domain.Coordinates, error) {
	res, err := c.cb.Execute(func() (*domain.Coordinates, error) {
		return c.underlying.GetCourierLocation(ctx, courierID)
	})
	if err != nil {
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			return nil, fmt.Errorf("%w: %v", ErrCircuitBreakerOpen, err)
		}
		return nil, err
	}
	return res, nil
}
