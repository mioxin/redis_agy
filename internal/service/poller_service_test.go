package service_test

import (
	"context"
	"testing"
	"time"

	"courier-service/internal/config"
	"courier-service/internal/domain"
	"courier-service/internal/repository/cache"
	"courier-service/internal/repository/external"
	"courier-service/internal/service"
)

func TestPollerService_SyncOrder(t *testing.T) {
	cfg := &config.Config{
		LocationTTL:            120 * time.Second,
		OrderCourierMappingTTL: 1 * time.Hour,
		UrgentQueueSize:        10,
		WorkerConcurrency:      5,
	}

	orders := []domain.Order{
		{ID: 1, CourierID: 101},
	}
	orderMock := external.NewOrderServiceMockFromOrders(orders, 0)
	courierMock := external.NewCourierServiceMockWithDelay(0)
	memCache := cache.NewMemoryCacheRepository()

	poller := service.NewPollerService(memCache, orderMock, courierMock, cfg)
	ctx := context.Background()

	err := poller.SyncOrder(ctx, 1)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify location is cached
	loc, err := memCache.GetOrderLocation(ctx, 1)
	if err != nil {
		t.Fatalf("expected location to be cached, got: %v", err)
	}
	if loc.OrderID != 1 || loc.CourierID != 101 {
		t.Fatalf("unexpected cached location: %+v", loc)
	}

	// Verify mapping is cached
	courierID, err := memCache.GetOrderCourierMapping(ctx, 1)
	if err != nil {
		t.Fatalf("expected courier mapping to be cached: %v", err)
	}
	if courierID != 101 {
		t.Fatalf("expected courier 101, got: %d", courierID)
	}
}

func TestPollerService_SyncActiveOrders(t *testing.T) {
	cfg := &config.Config{
		LocationTTL:            120 * time.Second,
		OrderCourierMappingTTL: 1 * time.Hour,
		HeartbeatTTL:           90 * time.Second,
		WorkerConcurrency:      5,
		UrgentQueueSize:        10,
	}

	orders := []domain.Order{
		{ID: 1, CourierID: 101},
		{ID: 2, CourierID: 101}, // Same courier to test deduplication!
		{ID: 3, CourierID: 102},
	}
	orderMock := external.NewOrderServiceMockFromOrders(orders, 0)
	courierMock := external.NewCourierServiceMockWithDelay(0)
	memCache := cache.NewMemoryCacheRepository()

	ctx := context.Background()
	// Register active orders
	_ = memCache.RegisterActiveOrder(ctx, 1, cfg.HeartbeatTTL)
	_ = memCache.RegisterActiveOrder(ctx, 2, cfg.HeartbeatTTL)
	_ = memCache.RegisterActiveOrder(ctx, 3, cfg.HeartbeatTTL)

	poller := service.NewPollerService(memCache, orderMock, courierMock, cfg)

	err := poller.SyncActiveOrders(ctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify all 3 orders have locations cached
	for _, id := range []int64{1, 2, 3} {
		loc, err := memCache.GetOrderLocation(ctx, id)
		if err != nil {
			t.Fatalf("expected location for order %d: %v", id, err)
		}
		if loc.OrderID != id {
			t.Fatalf("expected order %d, got %d", id, loc.OrderID)
		}
	}

	// Verify last sync was recorded
	lastSync, err := memCache.GetLastSync(ctx)
	if err != nil || lastSync.IsZero() {
		t.Fatalf("expected valid last sync timestamp: %v", err)
	}
}
