package service_test

import (
	"context"
	"testing"
	"time"

	"courier-service/internal/config"
	"courier-service/internal/domain"
	"courier-service/internal/repository/cache"
	"courier-service/internal/service"
)

type dummyPollerService struct {
	urgentTriggered chan int64
}

func newDummyPollerService() *dummyPollerService {
	return &dummyPollerService{
		urgentTriggered: make(chan int64, 10),
	}
}

func (d *dummyPollerService) TriggerUrgentSync(orderID int64) {
	d.urgentTriggered <- orderID
}

func (d *dummyPollerService) UrgentQueue() <-chan int64 {
	return d.urgentTriggered
}

func (d *dummyPollerService) SyncOrder(ctx context.Context, orderID int64) error {
	return nil
}

func (d *dummyPollerService) SyncActiveOrders(ctx context.Context) error {
	return nil
}

func TestLocationService_GetCourierLocation(t *testing.T) {
	cfg := &config.Config{
		HeartbeatTTL: 90 * time.Second,
		LocationTTL:  120 * time.Second,
	}

	t.Run("Cache Hit returns location", func(t *testing.T) {
		memCache := cache.NewMemoryCacheRepository()
		poller := newDummyPollerService()
		svc := service.NewLocationService(memCache, poller, cfg)

		ctx := context.Background()
		orderID := int64(10)
		expected := domain.CourierLocation{
			OrderID:   orderID,
			CourierID: 5,
			Latitude:  55.75,
			Longitude: 37.61,
			UpdatedAt: time.Now(),
		}

		_ = memCache.SetOrderLocation(ctx, expected, cfg.LocationTTL)

		loc, found, err := svc.GetCourierLocation(ctx, orderID)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if !found {
			t.Fatal("expected cache hit (found=true), got found=false")
		}
		if loc.OrderID != expected.OrderID || loc.CourierID != expected.CourierID {
			t.Fatalf("unexpected location: %+v", loc)
		}
	})

	t.Run("Cache Miss registers active order and triggers urgent sync", func(t *testing.T) {
		memCache := cache.NewMemoryCacheRepository()
		poller := newDummyPollerService()
		svc := service.NewLocationService(memCache, poller, cfg)

		ctx := context.Background()
		orderID := int64(25)

		loc, found, err := svc.GetCourierLocation(ctx, orderID)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if found {
			t.Fatal("expected cache miss (found=false), got found=true")
		}
		if loc != nil {
			t.Fatalf("expected nil location on cache miss, got: %+v", loc)
		}

		// Verify order was registered in active orders
		activeIDs, err := memCache.GetActiveOrderIDs(ctx)
		if err != nil {
			t.Fatalf("failed to get active order ids: %v", err)
		}
		if len(activeIDs) != 1 || activeIDs[0] != orderID {
			t.Fatalf("expected active orders to contain %d, got: %v", orderID, activeIDs)
		}

		// Verify urgent sync was triggered
		select {
		case triggeredID := <-poller.urgentTriggered:
			if triggeredID != orderID {
				t.Fatalf("expected urgent trigger for %d, got: %d", orderID, triggeredID)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timed out waiting for urgent sync trigger")
		}
	})

	t.Run("Invalid order ID returns domain.ErrInvalidOrderID", func(t *testing.T) {
		memCache := cache.NewMemoryCacheRepository()
		poller := newDummyPollerService()
		svc := service.NewLocationService(memCache, poller, cfg)

		_, _, err := svc.GetCourierLocation(context.Background(), -1)
		if err == nil {
			t.Fatal("expected error on negative order ID, got nil")
		}
	})
}
