package external_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"courier-service/internal/domain"
	"courier-service/internal/repository/external"
)

func TestChaos_OrderServiceErrorInjection(t *testing.T) {
	orders := []domain.Order{
		{ID: 1, CourierID: 101},
	}
	mock := external.NewOrderServiceMockFromOrders(orders, 0)

	// Set 100% error rate
	mock.SetChaosParams(1.0, 0)

	_, err := mock.GetOrderByID(context.Background(), 1)
	if err == nil {
		t.Fatalf("expected simulated error, got nil")
	}
	if !errors.Is(err, external.ErrOrderServiceSimulatedFailure) {
		t.Fatalf("expected ErrOrderServiceSimulatedFailure, got: %v", err)
	}

	// Turn off chaos
	mock.SetChaosParams(0.0, 0)
	order, err := mock.GetOrderByID(context.Background(), 1)
	if err != nil {
		t.Fatalf("expected success when chaos is disabled, got: %v", err)
	}
	if order.ID != 1 {
		t.Fatalf("unexpected order ID: %d", order.ID)
	}
}

func TestChaos_CourierServiceTimeoutInjection(t *testing.T) {
	mock := external.NewCourierServiceMockWithDelay(0)

	// Set 100% timeout rate
	mock.SetChaosParams(1.0, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := mock.GetCourierLocation(ctx, 101)
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
	if !errors.Is(err, external.ErrCourierServiceSimulatedTimeout) {
		t.Fatalf("expected ErrCourierServiceSimulatedTimeout, got: %v", err)
	}

	// Disable chaos
	mock.SetChaosParams(0.0, 0)
	coords, err := mock.GetCourierLocation(context.Background(), 101)
	if err != nil {
		t.Fatalf("expected success when chaos is disabled, got: %v", err)
	}
	if coords.Latitude == 0 {
		t.Fatalf("expected non-zero latitude")
	}
}
