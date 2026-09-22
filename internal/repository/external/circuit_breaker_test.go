package external_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"courier-service/internal/domain"
	"courier-service/internal/repository/external"
)

type mockFailingOrderService struct {
	callCount atomic.Int64
	fail      atomic.Bool
}

func (m *mockFailingOrderService) GetOrderByID(ctx context.Context, orderID int64) (*domain.Order, error) {
	m.callCount.Add(1)
	if m.fail.Load() {
		return nil, errors.New("underlying service failure")
	}
	return &domain.Order{ID: orderID, CourierID: 101}, nil
}

func (m *mockFailingOrderService) GetAllOrders() []domain.Order {
	return nil
}

func TestCircuitBreakerOrderService_TripsAndRecovers(t *testing.T) {
	mockSvc := &mockFailingOrderService{}
	mockSvc.fail.Store(true)

	// Configure circuit breaker with 5 failure threshold and 50ms cooldown for fast test
	cb := external.NewCircuitBreakerOrderService(mockSvc, 5, 50*time.Millisecond)
	ctx := context.Background()

	// 1. Induce 5 consecutive failures
	for i := 0; i < 5; i++ {
		_, err := cb.GetOrderByID(ctx, 1)
		if err == nil {
			t.Fatalf("expected error on attempt %d", i+1)
		}
	}

	if mockSvc.callCount.Load() != 5 {
		t.Fatalf("expected 5 calls to underlying service, got: %d", mockSvc.callCount.Load())
	}

	// 2. The 6th call should be intercepted by Circuit Breaker (OPEN state) immediately
	start := time.Now()
	_, err := cb.GetOrderByID(ctx, 1)
	elapsed := time.Since(start)

	if !errors.Is(err, external.ErrCircuitBreakerOpen) {
		t.Fatalf("expected ErrCircuitBreakerOpen, got: %v", err)
	}

	// Must fail fast (< 1ms)
	if elapsed > 10*time.Millisecond {
		t.Fatalf("expected fast circuit breaker rejection, took: %v", elapsed)
	}

	// Underlying service must NOT have been called on open state
	if mockSvc.callCount.Load() != 5 {
		t.Fatalf("expected underlying service to NOT be called when OPEN, got: %d calls", mockSvc.callCount.Load())
	}

	// 3. Wait for cooldown to transition to HALF-OPEN
	time.Sleep(60 * time.Millisecond)

	// Heal the underlying service
	mockSvc.fail.Store(false)

	// Probe request should succeed and CLOSE the circuit
	order, err := cb.GetOrderByID(ctx, 1)
	if err != nil {
		t.Fatalf("expected probe request to succeed, got: %v", err)
	}
	if order.CourierID != 101 {
		t.Fatalf("unexpected order courier: %d", order.CourierID)
	}

	// Subsequent request should also succeed cleanly
	_, err = cb.GetOrderByID(ctx, 1)
	if err != nil {
		t.Fatalf("expected subsequent request to succeed, got: %v", err)
	}
}

type mockFailingCourierService struct {
	callCount atomic.Int64
	fail      atomic.Bool
}

func (m *mockFailingCourierService) GetCourierLocation(ctx context.Context, courierID int64) (*domain.Coordinates, error) {
	m.callCount.Add(1)
	if m.fail.Load() {
		return nil, errors.New("courier upstream network unreachable")
	}
	return &domain.Coordinates{Latitude: 55.75, Longitude: 37.61}, nil
}

func TestCircuitBreakerCourierService_TripsAndRejects(t *testing.T) {
	mockSvc := &mockFailingCourierService{}
	mockSvc.fail.Store(true)

	cb := external.NewCircuitBreakerCourierService(mockSvc, 5, 50*time.Millisecond)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, _ = cb.GetCourierLocation(ctx, 101)
	}

	// Should be OPEN
	start := time.Now()
	_, err := cb.GetCourierLocation(ctx, 101)
	elapsed := time.Since(start)

	if !errors.Is(err, external.ErrCircuitBreakerOpen) {
		t.Fatalf("expected ErrCircuitBreakerOpen, got: %v", err)
	}
	if elapsed > 10*time.Millisecond {
		t.Fatalf("expected fast rejection, took: %v", elapsed)
	}
}
