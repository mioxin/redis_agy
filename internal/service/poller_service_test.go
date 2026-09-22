package service_test

import (
	"context"
	"sync"
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

type countingOrderClient struct {
	mu        sync.Mutex
	callCount int
	delay     time.Duration
}

func (c *countingOrderClient) GetOrderByID(ctx context.Context, orderID int64) (*domain.Order, error) {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.mu.Lock()
	c.callCount++
	c.mu.Unlock()
	return &domain.Order{ID: orderID, CourierID: 777}, nil
}

func (c *countingOrderClient) GetAllOrders() []domain.Order {
	return nil
}

func (c *countingOrderClient) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.callCount
}

func TestPollerService_PreWarmCache(t *testing.T) {
	cfg := &config.Config{
		LocationTTL:            120 * time.Second,
		OrderCourierMappingTTL: 1 * time.Hour,
		HeartbeatTTL:           90 * time.Second,
		WorkerConcurrency:      5,
		UrgentQueueSize:        10,
	}

	orders := []domain.Order{
		{ID: 1, CourierID: 101},
		{ID: 2, CourierID: 101},
		{ID: 3, CourierID: 102},
	}
	orderMock := external.NewOrderServiceMockFromOrders(orders, 0)
	cbOrderMock := external.NewCircuitBreakerOrderService(orderMock, 3, 1*time.Second)
	courierMock := external.NewCourierServiceMockWithDelay(0)
	memCache := cache.NewMemoryCacheRepository()

	poller := service.NewPollerService(memCache, cbOrderMock, courierMock, cfg)
	ctx := context.Background()

	err := poller.PreWarmCache(ctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify all orders are in active_orders set
	activeIDs, err := memCache.GetActiveOrderIDs(ctx)
	if err != nil {
		t.Fatalf("failed to get active order ids: %v", err)
	}
	if len(activeIDs) != len(orders) {
		t.Fatalf("expected %d active orders, got: %d", len(orders), len(activeIDs))
	}
	orderSet := make(map[int64]bool, len(orders))
	for _, id := range activeIDs {
		orderSet[id] = true
	}
	for _, o := range orders {
		if !orderSet[o.ID] {
			t.Fatalf("expected order %d in active_orders, not found", o.ID)
		}
	}

	// Verify all order->courier mappings are cached
	for _, o := range orders {
		courierID, err := memCache.GetOrderCourierMapping(ctx, o.ID)
		if err != nil {
			t.Fatalf("expected courier mapping for order %d: %v", o.ID, err)
		}
		if courierID != o.CourierID {
			t.Fatalf("expected courier %d for order %d, got %d", o.CourierID, o.ID, courierID)
		}
	}

	// Verify all locations are cached
	for _, o := range orders {
		loc, err := memCache.GetOrderLocation(ctx, o.ID)
		if err != nil {
			t.Fatalf("expected location for order %d: %v", o.ID, err)
		}
		if loc.OrderID != o.ID {
			t.Fatalf("expected order %d, got %d", o.ID, loc.OrderID)
		}
		if loc.CourierID != o.CourierID {
			t.Fatalf("expected courier %d, got %d", o.CourierID, loc.CourierID)
		}
	}
}

func TestPollerService_Singleflight_CacheStampedeProtection(t *testing.T) {
	cfg := &config.Config{
		LocationTTL:            120 * time.Second,
		OrderCourierMappingTTL: 1 * time.Hour,
		UrgentQueueSize:        100,
		WorkerConcurrency:      10,
	}

	countingClient := &countingOrderClient{delay: 20 * time.Millisecond}
	courierMock := external.NewCourierServiceMockWithDelay(10 * time.Millisecond)
	memCache := cache.NewMemoryCacheRepository()

	poller := service.NewPollerService(memCache, countingClient, courierMock, cfg)

	const concurrency = 50
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)

	// Launch 50 concurrent requests simultaneously for the same order ID
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := poller.SyncOrder(context.Background(), 999)
			if err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent sync failed: %v", err)
	}

	// Verify that countingOrderClient was invoked exactly 1 time despite 50 concurrent requests
	if countingClient.Calls() != 1 {
		t.Fatalf("expected GetOrderByID to be called strictly 1 time due to singleflight deduplication, got: %d", countingClient.Calls())
	}
}

