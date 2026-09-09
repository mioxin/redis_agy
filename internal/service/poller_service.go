// Package service coordinates business logic for location retrieval and background synchronization.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"courier-service/internal/config"
	"courier-service/internal/domain"
	"courier-service/internal/repository/cache"
	"courier-service/internal/repository/external"
)

// PollerService coordinates periodic and on-demand synchronization of courier locations.
type PollerService interface {
	// TriggerUrgentSync dispatches an immediate asynchronous synchronization request for an order.
	TriggerUrgentSync(orderID int64)
	// UrgentQueue returns the receive-only channel for urgent sync requests.
	UrgentQueue() <-chan int64
	// SyncOrder performs an end-to-end sync for a single order.
	SyncOrder(ctx context.Context, orderID int64) error
	// SyncActiveOrders performs batch synchronization of all currently active orders with courier deduplication.
	SyncActiveOrders(ctx context.Context) error
}

// pollerServiceImpl implements PollerService.
type pollerServiceImpl struct {
	cacheRepo     cache.LocationCacheRepository
	orderClient   external.OrderServiceClient
	courierClient external.CourierServiceClient
	cfg           *config.Config
	urgentQueue   chan int64
}

// NewPollerService creates a new PollerService instance.
func NewPollerService(
	cacheRepo cache.LocationCacheRepository,
	orderClient external.OrderServiceClient,
	courierClient external.CourierServiceClient,
	cfg *config.Config,
) PollerService {
	return &pollerServiceImpl{
		cacheRepo:     cacheRepo,
		orderClient:   orderClient,
		courierClient: courierClient,
		cfg:           cfg,
		urgentQueue:   make(chan int64, cfg.UrgentQueueSize),
	}
}

// TriggerUrgentSync queues an order for immediate background polling.
func (p *pollerServiceImpl) TriggerUrgentSync(orderID int64) {
	select {
	case p.urgentQueue <- orderID:
		slog.Debug("Urgent sync queued", slog.Int64("order_id", orderID))
	default:
		slog.Warn("Urgent sync queue is full, skipping duplicate push", slog.Int64("order_id", orderID))
	}
}

// UrgentQueue returns the receive-only channel.
func (p *pollerServiceImpl) UrgentQueue() <-chan int64 {
	return p.urgentQueue
}

// SyncOrder resolves an order's courier and fetches coordinates, updating Redis cache.
func (p *pollerServiceImpl) SyncOrder(ctx context.Context, orderID int64) error {
	// 1. Check order -> courier mapping in cache
	courierID, err := p.cacheRepo.GetOrderCourierMapping(ctx, orderID)
	if err != nil {
		if !errors.Is(err, cache.ErrCacheMiss) {
			slog.Warn("Failed to read order mapping from cache", slog.Int64("order_id", orderID), slog.String("error", err.Error()))
		}

		// Cache miss on mapping: query Order Service (200ms)
		order, oErr := p.orderClient.GetOrderByID(ctx, orderID)
		if oErr != nil {
			return fmt.Errorf("order service lookup for %d: %w", orderID, oErr)
		}

		courierID = order.CourierID
		// Cache mapping with long TTL (1h)
		if err := p.cacheRepo.SetOrderCourierMapping(ctx, orderID, courierID, p.cfg.OrderCourierMappingTTL); err != nil {
			slog.Warn("Failed to cache order courier mapping", slog.Int64("order_id", orderID), slog.String("error", err.Error()))
		}
	}

	// 2. Fetch coordinates from Courier Service (500ms)
	coords, cErr := p.courierClient.GetCourierLocation(ctx, courierID)
	if cErr != nil {
		return fmt.Errorf("courier service lookup for courier %d: %w", courierID, cErr)
	}

	// 3. Cache location in Redis (TTL 120s)
	loc := domain.CourierLocation{
		OrderID:   orderID,
		CourierID: courierID,
		Latitude:  coords.Latitude,
		Longitude: coords.Longitude,
		UpdatedAt: time.Now().UTC(),
	}

	if err := p.cacheRepo.SetOrderLocation(ctx, loc, p.cfg.LocationTTL); err != nil {
		return fmt.Errorf("saving location to cache for order %d: %w", orderID, err)
	}

	slog.Info("Successfully synced order location",
		slog.Int64("order_id", orderID),
		slog.Int64("courier_id", courierID),
		slog.Float64("lat", loc.Latitude),
		slog.Float64("lon", loc.Longitude),
	)

	return nil
}

// SyncActiveOrders performs periodic batch polling for all active orders with deduplication.
func (p *pollerServiceImpl) SyncActiveOrders(ctx context.Context) error {
	// 1. Evict inactive orders whose heartbeats expired
	if err := p.cacheRepo.RemoveInactiveOrders(ctx); err != nil {
		slog.Warn("Failed to clean up inactive orders", slog.String("error", err.Error()))
	}

	// 2. Get active orders
	orderIDs, err := p.cacheRepo.GetActiveOrderIDs(ctx)
	if err != nil {
		return fmt.Errorf("retrieving active orders: %w", err)
	}

	if len(orderIDs) == 0 {
		_ = p.cacheRepo.SetLastSync(ctx, time.Now().UTC())
		return nil
	}

	slog.Debug("Polling active orders", slog.Int("active_count", len(orderIDs)))

	// 3. Resolve courier IDs for all active orders (using cache or concurrent Order Service queries)
	orderToCourier := make(map[int64]int64, len(orderIDs))
	var missingOrders []int64

	for _, oID := range orderIDs {
		cID, err := p.cacheRepo.GetOrderCourierMapping(ctx, oID)
		if err == nil {
			orderToCourier[oID] = cID
		} else {
			missingOrders = append(missingOrders, oID)
		}
	}

	// For orders missing cached courier mappings, query Order Service concurrently with bounded workers
	if len(missingOrders) > 0 {
		resolved := p.fetchMissingOrderCouriers(ctx, missingOrders)
		for oID, cID := range resolved {
			orderToCourier[oID] = cID
			_ = p.cacheRepo.SetOrderCourierMapping(ctx, oID, cID, p.cfg.OrderCourierMappingTTL)
		}
	}

	if len(orderToCourier) == 0 {
		_ = p.cacheRepo.SetLastSync(ctx, time.Now().UTC())
		return nil
	}

	// 4. Deduplicate courier IDs: group order IDs by courier ID
	courierToOrders := make(map[int64][]int64)
	for oID, cID := range orderToCourier {
		courierToOrders[cID] = append(courierToOrders[cID], oID)
	}

	uniqueCouriers := make([]int64, 0, len(courierToOrders))
	for cID := range courierToOrders {
		uniqueCouriers = append(uniqueCouriers, cID)
	}

	// 5. Query Courier Service strictly once per unique courier with bounded concurrency
	courierCoordinates := p.fetchCourierCoordinatesConcurrently(ctx, uniqueCouriers)

	// 6. Build batch of locations for all associated orders
	now := time.Now().UTC()
	locations := make([]domain.CourierLocation, 0, len(orderToCourier))

	for cID, coords := range courierCoordinates {
		for _, oID := range courierToOrders[cID] {
			locations = append(locations, domain.CourierLocation{
				OrderID:   oID,
				CourierID: cID,
				Latitude:  coords.Latitude,
				Longitude: coords.Longitude,
				UpdatedAt: now,
			})
		}
	}

	// 7. Pipeline save to Redis
	if err := p.cacheRepo.SetOrderLocationsBatch(ctx, locations, p.cfg.LocationTTL); err != nil {
		return fmt.Errorf("batch setting order locations: %w", err)
	}

	_ = p.cacheRepo.SetLastSync(ctx, now)
	slog.Info("Completed active orders sync cycle",
		slog.Int("orders_updated", len(locations)),
		slog.Int("unique_couriers_polled", len(uniqueCouriers)),
	)

	return nil
}

// fetchMissingOrderCouriers queries Order Service for missing orders with bounded concurrency.
func (p *pollerServiceImpl) fetchMissingOrderCouriers(ctx context.Context, orderIDs []int64) map[int64]int64 {
	results := make(map[int64]int64)
	var mu sync.Mutex
	sem := make(chan struct{}, p.cfg.WorkerConcurrency)
	var wg sync.WaitGroup

orderLoop:
	for _, oID := range orderIDs {
		select {
		case <-ctx.Done():
			break orderLoop
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			defer func() { <-sem }()

			order, err := p.orderClient.GetOrderByID(ctx, id)
			if err != nil {
				slog.Warn("Failed to resolve order from Order Service", slog.Int64("order_id", id), slog.String("error", err.Error()))
				return
			}

			mu.Lock()
			results[id] = order.CourierID
			mu.Unlock()
		}(oID)
	}

	wg.Wait()
	return results
}

// fetchCourierCoordinatesConcurrently queries Courier Service for unique couriers with bounded concurrency.
func (p *pollerServiceImpl) fetchCourierCoordinatesConcurrently(ctx context.Context, courierIDs []int64) map[int64]*domain.Coordinates {
	results := make(map[int64]*domain.Coordinates)
	var mu sync.Mutex
	sem := make(chan struct{}, p.cfg.WorkerConcurrency)
	var wg sync.WaitGroup

courierLoop:
	for _, cID := range courierIDs {
		select {
		case <-ctx.Done():
			break courierLoop
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			defer func() { <-sem }()

			coords, err := p.courierClient.GetCourierLocation(ctx, id)
			if err != nil {
				slog.Warn("Failed to fetch courier coordinates", slog.Int64("courier_id", id), slog.String("error", err.Error()))
				return
			}

			mu.Lock()
			results[id] = coords
			mu.Unlock()
		}(cID)
	}

	wg.Wait()
	return results
}
