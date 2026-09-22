// Package cache provides caching repositories and abstractions.
package cache

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"courier-service/internal/domain"
)

type memoryItem struct {
	value     interface{}
	expiresAt time.Time
}

func (i memoryItem) isExpired() bool {
	if i.expiresAt.IsZero() {
		return false
	}
	return time.Now().After(i.expiresAt)
}

type memoryLock struct {
	owner     string
	expiresAt time.Time
}

// MemoryCacheRepository provides a thread-safe in-memory implementation of LocationCacheRepository.
// Useful for integration testing and standalone benchmarking.
type MemoryCacheRepository struct {
	mu           sync.RWMutex
	locations    map[int64]memoryItem
	courierMaps  map[int64]memoryItem
	activeOrders map[int64]time.Time // orderID -> heartbeat expiresAt
	lastSync     time.Time
	leaderLocks  map[string]memoryLock
	preWarmed    bool
}

// NewMemoryCacheRepository initializes a new MemoryCacheRepository.
func NewMemoryCacheRepository() *MemoryCacheRepository {
	return &MemoryCacheRepository{
		locations:    make(map[int64]memoryItem),
		courierMaps:  make(map[int64]memoryItem),
		activeOrders: make(map[int64]time.Time),
		leaderLocks:  make(map[string]memoryLock),
	}
}


// GetOrderLocation returns cached courier location if present and not expired.
func (m *MemoryCacheRepository) GetOrderLocation(ctx context.Context, orderID int64) (*domain.CourierLocation, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item, exists := m.locations[orderID]
	if !exists || item.isExpired() {
		return nil, ErrCacheMiss
	}

	loc := item.value.(domain.CourierLocation)
	return &loc, nil
}

// SetOrderLocation saves a location in memory.
func (m *MemoryCacheRepository) SetOrderLocation(ctx context.Context, location domain.CourierLocation, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	m.locations[location.OrderID] = memoryItem{
		value:     location,
		expiresAt: exp,
	}
	return nil
}

// SetOrderLocationsBatch saves multiple locations in memory.
func (m *MemoryCacheRepository) SetOrderLocationsBatch(ctx context.Context, locations []domain.CourierLocation, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	for _, loc := range locations {
		m.locations[loc.OrderID] = memoryItem{
			value:     loc,
			expiresAt: exp,
		}
	}
	return nil
}

// GetOrderCourierMapping returns cached courier ID for an order.
func (m *MemoryCacheRepository) GetOrderCourierMapping(ctx context.Context, orderID int64) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item, exists := m.courierMaps[orderID]
	if !exists || item.isExpired() {
		return 0, ErrCacheMiss
	}

	return item.value.(int64), nil
}

// SetOrderCourierMapping saves order-to-courier mapping.
func (m *MemoryCacheRepository) SetOrderCourierMapping(ctx context.Context, orderID int64, courierID int64, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	m.courierMaps[orderID] = memoryItem{
		value:     courierID,
		expiresAt: exp,
	}
	return nil
}

// RegisterActiveOrder adds order to active tracking set with heartbeat.
func (m *MemoryCacheRepository) RegisterActiveOrder(ctx context.Context, orderID int64, heartbeatTTL time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeOrders[orderID] = time.Now().Add(heartbeatTTL)
	return nil
}

// GetActiveOrderIDs returns all active tracked orders.
func (m *MemoryCacheRepository) GetActiveOrderIDs(ctx context.Context) ([]int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]int64, 0, len(m.activeOrders))
	for id := range m.activeOrders {
		ids = append(ids, id)
	}
	return ids, nil
}

// RefreshOrderHeartbeat extends order heartbeat.
func (m *MemoryCacheRepository) RefreshOrderHeartbeat(ctx context.Context, orderID int64, heartbeatTTL time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeOrders[orderID] = time.Now().Add(heartbeatTTL)
	return nil
}

// RemoveInactiveOrders purges orders whose heartbeat has expired.
func (m *MemoryCacheRepository) RemoveInactiveOrders(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for id, exp := range m.activeOrders {
		if now.After(exp) {
			delete(m.activeOrders, id)
		}
	}
	return nil
}

// Ping checks repository liveness.
func (m *MemoryCacheRepository) Ping(ctx context.Context) error {
	return nil
}

// SetLastSync stores last sync time.
func (m *MemoryCacheRepository) SetLastSync(ctx context.Context, timestamp time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastSync = timestamp
	return nil
}

// GetLastSync returns last sync time.
func (m *MemoryCacheRepository) GetLastSync(ctx context.Context) (time.Time, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.lastSync, nil
}

// AcquireLeaderLock attempts to acquire leader lock in memory.
func (m *MemoryCacheRepository) AcquireLeaderLock(ctx context.Context, key string, instanceID string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	lock, exists := m.leaderLocks[key]
	if !exists || now.After(lock.expiresAt) {
		m.leaderLocks[key] = memoryLock{
			owner:     instanceID,
			expiresAt: now.Add(ttl),
		}
		return true, nil
	}

	if lock.owner == instanceID {
		m.leaderLocks[key] = memoryLock{
			owner:     instanceID,
			expiresAt: now.Add(ttl),
		}
		return true, nil
	}

	return false, nil
}

// RenewLeaderLock extends leader lock expiration if owned by instanceID.
func (m *MemoryCacheRepository) RenewLeaderLock(ctx context.Context, key string, instanceID string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	lock, exists := m.leaderLocks[key]
	if !exists || now.After(lock.expiresAt) {
		return false, nil
	}

	if lock.owner == instanceID {
		m.leaderLocks[key] = memoryLock{
			owner:     instanceID,
			expiresAt: now.Add(ttl),
		}
		return true, nil
	}

	return false, nil
}

// ReleaseLeaderLock releases leader lock if owned by instanceID.
func (m *MemoryCacheRepository) ReleaseLeaderLock(ctx context.Context, key string, instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	lock, exists := m.leaderLocks[key]
	if exists && lock.owner == instanceID {
		delete(m.leaderLocks, key)
	}
	return nil
}

// PreWarmOrders registers a batch of orders in the active tracking set with heartbeats
// and caches order->courier mappings in a single atomic critical section.
func (m *MemoryCacheRepository) PreWarmOrders(ctx context.Context, orders []domain.Order, heartbeatTTL time.Duration, mappingTTL time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var heartbeatExp time.Time
	if heartbeatTTL > 0 {
		heartbeatExp = now.Add(heartbeatTTL)
	}
	var mappingExp time.Time
	if mappingTTL > 0 {
		mappingExp = now.Add(mappingTTL)
	}

	for _, o := range orders {
		m.activeOrders[o.ID] = heartbeatExp
		m.courierMaps[o.ID] = memoryItem{
			value:     o.CourierID,
			expiresAt: mappingExp,
		}
	}

	slog.Info("Pre-warmed memory cache for orders", slog.Int("order_count", len(orders)))
	return nil
}

// SetPreWarmed marks the repository as pre-warmed (multi-pod coordination flag).
func (m *MemoryCacheRepository) SetPreWarmed(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.preWarmed = true
	return nil
}

// IsPreWarmed reports whether the pre-warm flag has been set.
func (m *MemoryCacheRepository) IsPreWarmed(ctx context.Context) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.preWarmed, nil
}

