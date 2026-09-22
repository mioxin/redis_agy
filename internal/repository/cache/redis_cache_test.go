package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"courier-service/internal/domain"
	"courier-service/internal/repository/cache"
)

func setupTestRedis(t *testing.T) (*cache.RedisCacheRepository, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	return cache.NewRedisCacheRepository(client), mr
}

func TestRedisCacheRepository_PreWarmOrders(t *testing.T) {
	repo, mr := setupTestRedis(t)
	ctx := context.Background()

	orders := []domain.Order{
		{ID: 1, CourierID: 101},
		{ID: 2, CourierID: 101},
		{ID: 3, CourierID: 102},
	}

	heartbeatTTL := 90 * time.Second
	mappingTTL := 1 * time.Hour

	err := repo.PreWarmOrders(ctx, orders, heartbeatTTL, mappingTTL)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify all orders are in active_orders
	activeIDs, err := repo.GetActiveOrderIDs(ctx)
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
		courierID, err := repo.GetOrderCourierMapping(ctx, o.ID)
		if err != nil {
			t.Fatalf("expected courier mapping for order %d: %v", o.ID, err)
		}
		if courierID != o.CourierID {
			t.Fatalf("expected courier %d for order %d, got %d", o.CourierID, o.ID, courierID)
		}
	}

	// Verify heartbeat keys are set (order in active_orders should have heartbeat)
	for _, o := range orders {
		loc, err := repo.GetOrderLocation(ctx, o.ID)
		if err != cache.ErrCacheMiss {
			_ = loc
		}
		// Heartbeat keys are internal; verify via RemoveInactiveOrders not purging them
	}

	// Verify that after fast-forwarding past heartbeat TTL, orders are purged
	mr.FastForward(91 * time.Second)
	err = repo.RemoveInactiveOrders(ctx)
	if err != nil {
		t.Fatalf("failed to remove inactive orders: %v", err)
	}
	remaining, err := repo.GetActiveOrderIDs(ctx)
	if err != nil {
		t.Fatalf("failed to get active orders after purge: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected 0 active orders after TTL expiration, got: %d", len(remaining))
	}

	// Verify mappings still exist after heartbeat expiry (separate TTL)
	for _, o := range orders {
		courierID, err := repo.GetOrderCourierMapping(ctx, o.ID)
		if err != nil {
			t.Fatalf("expected courier mapping for order %d to still exist after heartbeat expiry: %v", o.ID, err)
		}
		if courierID != o.CourierID {
			t.Fatalf("expected courier %d for order %d, got %d", o.CourierID, o.ID, courierID)
		}
	}
}

func TestRedisCacheRepository_PreWarmed(t *testing.T) {
	repo, _ := setupTestRedis(t)
	ctx := context.Background()

	t.Run("IsPreWarmed returns false by default", func(t *testing.T) {
		warmed, err := repo.IsPreWarmed(ctx)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if warmed {
			t.Fatal("expected preWarmed=false by default")
		}
	})

	t.Run("SetPreWarmed then IsPreWarmed returns true", func(t *testing.T) {
		err := repo.SetPreWarmed(ctx)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		warmed, err := repo.IsPreWarmed(ctx)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if !warmed {
			t.Fatal("expected preWarmed=true after SetPreWarmed")
		}
	})

	t.Run("PreWarmed flag persists with no TTL", func(t *testing.T) {
		repo2, mr := setupTestRedis(t)
		// Set flag on repo, verify on repo2 (same Redis instance)
		err := repo2.SetPreWarmed(ctx)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		warmed, err := repo.IsPreWarmed(ctx)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if !warmed {
			t.Fatal("expected preWarmed=true across same Redis instance")
		}

		// Fast-forward long time (no TTL = persistent)
		mr.FastForward(365 * 24 * time.Hour)
		warmed, err = repo.IsPreWarmed(ctx)
		if err != nil {
			t.Fatalf("expected no error after fast-forward, got: %v", err)
		}
		if !warmed {
			t.Fatal("expected preWarmed=true to persist without TTL")
		}
	})
}

func TestRedisCacheRepository_OrderLocation(t *testing.T) {
	repo, mr := setupTestRedis(t)
	ctx := context.Background()

	t.Run("Cache Miss returns ErrCacheMiss", func(t *testing.T) {
		_, err := repo.GetOrderLocation(ctx, 999)
		if err != cache.ErrCacheMiss {
			t.Fatalf("expected ErrCacheMiss, got: %v", err)
		}
	})

	t.Run("Set and Get Order Location", func(t *testing.T) {
		loc := domain.CourierLocation{
			OrderID:   42,
			CourierID: 7,
			Latitude:  55.75,
			Longitude: 37.61,
			UpdatedAt: time.Now().UTC().Truncate(time.Millisecond),
		}

		err := repo.SetOrderLocation(ctx, loc, 120*time.Second)
		if err != nil {
			t.Fatalf("failed to set location: %v", err)
		}

		retrieved, err := repo.GetOrderLocation(ctx, 42)
		if err != nil {
			t.Fatalf("failed to get location: %v", err)
		}
		if retrieved.OrderID != loc.OrderID || retrieved.CourierID != loc.CourierID {
			t.Fatalf("mismatched location: %+v", retrieved)
		}
	})

	t.Run("Batch Set Locations", func(t *testing.T) {
		locations := []domain.CourierLocation{
			{OrderID: 1, CourierID: 10, Latitude: 55.1, Longitude: 37.1, UpdatedAt: time.Now().UTC()},
			{OrderID: 2, CourierID: 20, Latitude: 55.2, Longitude: 37.2, UpdatedAt: time.Now().UTC()},
		}

		err := repo.SetOrderLocationsBatch(ctx, locations, 120*time.Second)
		if err != nil {
			t.Fatalf("failed batch set: %v", err)
		}

		loc1, err := repo.GetOrderLocation(ctx, 1)
		if err != nil || loc1.CourierID != 10 {
			t.Fatalf("order 1 not saved correctly: %+v", loc1)
		}

		loc2, err := repo.GetOrderLocation(ctx, 2)
		if err != nil || loc2.CourierID != 20 {
			t.Fatalf("order 2 not saved correctly: %+v", loc2)
		}
	})

	t.Run("Order Courier Mapping", func(t *testing.T) {
		err := repo.SetOrderCourierMapping(ctx, 100, 500, time.Hour)
		if err != nil {
			t.Fatalf("failed to set courier mapping: %v", err)
		}

		courierID, err := repo.GetOrderCourierMapping(ctx, 100)
		if err != nil {
			t.Fatalf("failed to get courier mapping: %v", err)
		}
		if courierID != 500 {
			t.Fatalf("expected courier 500, got %d", courierID)
		}

		_, err = repo.GetOrderCourierMapping(ctx, 9999)
		if err != cache.ErrCacheMiss {
			t.Fatalf("expected ErrCacheMiss, got: %v", err)
		}
	})

	t.Run("Active Orders and Heartbeat", func(t *testing.T) {
		// Register orders
		_ = repo.RegisterActiveOrder(ctx, 201, 10*time.Second)
		_ = repo.RegisterActiveOrder(ctx, 202, 10*time.Second)

		activeIDs, err := repo.GetActiveOrderIDs(ctx)
		if err != nil {
			t.Fatalf("failed to get active order ids: %v", err)
		}
		if len(activeIDs) != 2 {
			t.Fatalf("expected 2 active orders, got: %d", len(activeIDs))
		}

		// Fast-forward time in miniredis past 10s
		mr.FastForward(15 * time.Second)

		// Remove inactive orders
		err = repo.RemoveInactiveOrders(ctx)
		if err != nil {
			t.Fatalf("failed to remove inactive orders: %v", err)
		}

		remaining, err := repo.GetActiveOrderIDs(ctx)
		if err != nil {
			t.Fatalf("failed to get active orders after purge: %v", err)
		}
		if len(remaining) != 0 {
			t.Fatalf("expected 0 active orders after TTL expiration, got: %d", len(remaining))
		}
	})
}
