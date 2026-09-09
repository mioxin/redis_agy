package external_test

import (
	"context"
	"testing"
	"time"

	"courier-service/internal/domain"
	"courier-service/internal/repository/external"
)

func TestOrderServiceMock_GetOrderByID(t *testing.T) {
	orders := []domain.Order{
		{ID: 1, CourierID: 101},
		{ID: 2, CourierID: 102},
	}
	mock := external.NewOrderServiceMockFromOrders(orders, 10*time.Millisecond)

	ctx := context.Background()

	t.Run("Found order", func(t *testing.T) {
		start := time.Now()
		order, err := mock.GetOrderByID(ctx, 1)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if order.ID != 1 || order.CourierID != 101 {
			t.Fatalf("unexpected order: %+v", order)
		}
		if duration < 10*time.Millisecond {
			t.Fatalf("expected at least 10ms delay, got: %v", duration)
		}
	})

	t.Run("Not found order", func(t *testing.T) {
		_, err := mock.GetOrderByID(ctx, 999)
		if err == nil {
			t.Fatal("expected error for non-existent order, got nil")
		}
	})

	t.Run("Context canceled", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := mock.GetOrderByID(cancelCtx, 1)
		if err == nil {
			t.Fatal("expected error on canceled context, got nil")
		}
	})
}
