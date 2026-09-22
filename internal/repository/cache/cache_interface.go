// Package cache provides caching repositories and abstractions.
package cache

import (
	"context"
	"errors"
	"time"

	"courier-service/internal/domain"
)

// ErrCacheMiss indicates that the requested key does not exist in cache.
var ErrCacheMiss = errors.New("cache miss")

// LocationCacheRepository defines caching operations for courier locations,
// order-courier mappings, and active order tracking sets.
type LocationCacheRepository interface {
	// Read Path: O(1) location lookup
	GetOrderLocation(ctx context.Context, orderID int64) (*domain.CourierLocation, error)
	SetOrderLocation(ctx context.Context, location domain.CourierLocation, ttl time.Duration) error
	SetOrderLocationsBatch(ctx context.Context, locations []domain.CourierLocation, ttl time.Duration) error

	// Order -> Courier mapping cache (Order Service only called once per order)
	GetOrderCourierMapping(ctx context.Context, orderID int64) (int64, error)
	SetOrderCourierMapping(ctx context.Context, orderID int64, courierID int64, ttl time.Duration) error

	// Reactive tracking registry (Active Orders sliding window with heartbeats)
	RegisterActiveOrder(ctx context.Context, orderID int64, heartbeatTTL time.Duration) error
	GetActiveOrderIDs(ctx context.Context) ([]int64, error)
	RefreshOrderHeartbeat(ctx context.Context, orderID int64, heartbeatTTL time.Duration) error
	RemoveInactiveOrders(ctx context.Context) error

	// Health and synchronization status
	Ping(ctx context.Context) error
	SetLastSync(ctx context.Context, timestamp time.Time) error
	GetLastSync(ctx context.Context) (time.Time, error)

	// Distributed Leader Election (P3)
	AcquireLeaderLock(ctx context.Context, key string, instanceID string, ttl time.Duration) (bool, error)
	RenewLeaderLock(ctx context.Context, key string, instanceID string, ttl time.Duration) (bool, error)
	ReleaseLeaderLock(ctx context.Context, key string, instanceID string) error

	// Cache Pre-Warming (ADR-002)
	// PreWarmOrders atomically registers a batch of orders in the active tracking set,
	// initializes their heartbeats, and caches order->courier mappings in a single pipeline.
	PreWarmOrders(ctx context.Context, orders []domain.Order, heartbeatTTL time.Duration, mappingTTL time.Duration) error

	// SetPreWarmed/IsPreWarmed coordinate multi-pod startup so only the first replica
	// executes pre-warming. Flag is test-only; not needed in production.
	SetPreWarmed(ctx context.Context) error
	IsPreWarmed(ctx context.Context) (bool, error)
}

