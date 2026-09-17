// Package cache provides caching repositories and abstractions.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"courier-service/internal/domain"
)

// Redis key patterns and data structures:
// 1. "order:%d:location"   (String, JSON, TTL=10s) : Cached courier coordinates for an order (O(1) read).
// 2. "order:%d:courier_id" (String, int64, TTL=24h): Order -> Courier ID mapping cache.
// 3. "tracking:active_orders" (Set of order IDs)  : List of orders currently being polled by PollerWorker.
// 4. "tracking:order:%d:heartbeat" (String, TTL=90s): Sliding-window heartbeat; when expired, order is purged from active set.
// 5. "courier_poller:last_sync" (String, RFC3339) : Global timestamp of the last successful batch polling cycle.
const (
	keyPrefixLocation  = "order:%d:location"
	keyPrefixCourier   = "order:%d:courier_id"
	keyActiveOrders    = "tracking:active_orders"
	keyOrderHeartbeat  = "tracking:order:%d:heartbeat"
	keyPollerLastSync  = "courier_poller:last_sync"
)

// RedisCacheRepository implements LocationCacheRepository using Redis.
// It serves as the single source of truth for hot data reads (Read Path)
// and handles distributed lock synchronization for multi-replica deployments.
type RedisCacheRepository struct {
	client redis.UniversalClient
}

// NewRedisCacheRepository creates a new instance of RedisCacheRepository.
func NewRedisCacheRepository(client redis.UniversalClient) *RedisCacheRepository {
	return &RedisCacheRepository{
		client: client,
	}
}

// GetOrderLocation retrieves the courier location from Redis in O(1) time.
func (r *RedisCacheRepository) GetOrderLocation(ctx context.Context, orderID int64) (*domain.CourierLocation, error) {
	key := fmt.Sprintf(keyPrefixLocation, orderID)
	data, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCacheMiss
		}
		return nil, fmt.Errorf("redis get order location for %d: %w", orderID, err)
	}

	var loc domain.CourierLocation
	if err := json.Unmarshal(data, &loc); err != nil {
		return nil, fmt.Errorf("unmarshal courier location for %d: %w", orderID, err)
	}

	return &loc, nil
}

// SetOrderLocation saves a single courier location to Redis with the specified TTL.
func (r *RedisCacheRepository) SetOrderLocation(ctx context.Context, location domain.CourierLocation, ttl time.Duration) error {
	key := fmt.Sprintf(keyPrefixLocation, location.OrderID)
	data, err := json.Marshal(location)
	if err != nil {
		return fmt.Errorf("marshal courier location: %w", err)
	}

	if err := r.client.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("redis set order location for %d: %w", location.OrderID, err)
	}

	return nil
}

// SetOrderLocationsBatch saves multiple locations to Redis in a single pipeline.
// Using Redis Pipelining batches all network roundtrips into 1 socket write/read,
// keeping sync cycle overhead minimal even with hundreds of active orders.
func (r *RedisCacheRepository) SetOrderLocationsBatch(ctx context.Context, locations []domain.CourierLocation, ttl time.Duration) error {
	if len(locations) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()
	for _, loc := range locations {
		key := fmt.Sprintf(keyPrefixLocation, loc.OrderID)
		data, err := json.Marshal(loc)
		if err != nil {
			return fmt.Errorf("marshal courier location for order %d: %w", loc.OrderID, err)
		}
		pipe.Set(ctx, key, data, ttl)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis pipeline set order locations: %w", err)
	}

	return nil
}

// GetOrderCourierMapping retrieves the cached courier ID assigned to an order.
func (r *RedisCacheRepository) GetOrderCourierMapping(ctx context.Context, orderID int64) (int64, error) {
	key := fmt.Sprintf(keyPrefixCourier, orderID)
	val, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, ErrCacheMiss
		}
		return 0, fmt.Errorf("redis get order-courier mapping for %d: %w", orderID, err)
	}

	courierID, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse courier id %q: %w", val, err)
	}

	return courierID, nil
}

// SetOrderCourierMapping caches the order-courier mapping with the specified TTL.
func (r *RedisCacheRepository) SetOrderCourierMapping(ctx context.Context, orderID int64, courierID int64, ttl time.Duration) error {
	key := fmt.Sprintf(keyPrefixCourier, orderID)
	if err := r.client.Set(ctx, key, strconv.FormatInt(courierID, 10), ttl).Err(); err != nil {
		return fmt.Errorf("redis set order-courier mapping for %d: %w", orderID, err)
	}
	return nil
}

// RegisterActiveOrder adds the order to the active tracking set and initializes its heartbeat TTL.
// Both operations are performed atomically in a pipeline so an order in the active set
// always has an initial heartbeat.
func (r *RedisCacheRepository) RegisterActiveOrder(ctx context.Context, orderID int64, heartbeatTTL time.Duration) error {
	pipe := r.client.Pipeline()
	pipe.SAdd(ctx, keyActiveOrders, orderID)
	heartbeatKey := fmt.Sprintf(keyOrderHeartbeat, orderID)
	pipe.Set(ctx, heartbeatKey, "1", heartbeatTTL)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis register active order %d: %w", orderID, err)
	}

	return nil
}

// GetActiveOrderIDs retrieves all currently tracked order IDs from Redis.
func (r *RedisCacheRepository) GetActiveOrderIDs(ctx context.Context) ([]int64, error) {
	members, err := r.client.SMembers(ctx, keyActiveOrders).Result()
	if err != nil {
		return nil, fmt.Errorf("redis smembers active orders: %w", err)
	}

	orderIDs := make([]int64, 0, len(members))
	for _, m := range members {
		id, err := strconv.ParseInt(m, 10, 64)
		if err == nil {
			orderIDs = append(orderIDs, id)
		}
	}

	return orderIDs, nil
}

// RefreshOrderHeartbeat extends the heartbeat expiration for an active order.
// This is triggered on every user read request (LocationService.GetCourierLocation),
// extending active polling as long as clients are watching the order.
func (r *RedisCacheRepository) RefreshOrderHeartbeat(ctx context.Context, orderID int64, heartbeatTTL time.Duration) error {
	heartbeatKey := fmt.Sprintf(keyOrderHeartbeat, orderID)
	if err := r.client.Set(ctx, heartbeatKey, "1", heartbeatTTL).Err(); err != nil {
		return fmt.Errorf("redis refresh heartbeat for %d: %w", orderID, err)
	}
	return nil
}

// RemoveInactiveOrders cleans up the active tracking set by removing orders whose heartbeat key has expired.
// Flow:
// 1. Fetch all active order IDs via SMEMBERS.
// 2. Batch check existence of their heartbeat keys using a pipeline of EXISTS commands.
// 3. Remove all expired IDs in a single SREM call.
func (r *RedisCacheRepository) RemoveInactiveOrders(ctx context.Context) error {
	members, err := r.client.SMembers(ctx, keyActiveOrders).Result()
	if err != nil {
		return fmt.Errorf("redis smembers active orders: %w", err)
	}

	if len(members) == 0 {
		return nil
	}

	// Check heartbeat existence for all members via pipeline
	pipe := r.client.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(members))
	for _, m := range members {
		id, err := strconv.ParseInt(m, 10, 64)
		if err != nil {
			continue
		}
		heartbeatKey := fmt.Sprintf(keyOrderHeartbeat, id)
		cmds[m] = pipe.Exists(ctx, heartbeatKey)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("redis pipeline check heartbeats: %w", err)
	}

	// Identify and remove expired orders
	var toRemove []interface{}
	for m, cmd := range cmds {
		if cmd.Val() == 0 {
			toRemove = append(toRemove, m)
		}
	}

	if len(toRemove) > 0 {
		if err := r.client.SRem(ctx, keyActiveOrders, toRemove...).Err(); err != nil {
			return fmt.Errorf("redis srem inactive orders: %w", err)
		}
	}

	return nil
}

// Ping verifies connection to Redis.
func (r *RedisCacheRepository) Ping(ctx context.Context) error {
	if err := r.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

// SetLastSync stores the timestamp of the last successful synchronization cycle.
func (r *RedisCacheRepository) SetLastSync(ctx context.Context, timestamp time.Time) error {
	val := timestamp.Format(time.RFC3339Nano)
	if err := r.client.Set(ctx, keyPollerLastSync, val, 0).Err(); err != nil {
		return fmt.Errorf("redis set last sync timestamp: %w", err)
	}
	return nil
}

// GetLastSync retrieves the timestamp of the last successful synchronization cycle.
func (r *RedisCacheRepository) GetLastSync(ctx context.Context) (time.Time, error) {
	val, err := r.client.Get(ctx, keyPollerLastSync).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("redis get last sync timestamp: %w", err)
	}

	t, err := time.Parse(time.RFC3339Nano, val)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse last sync timestamp %q: %w", val, err)
	}

	return t, nil
}

// AcquireLeaderLock attempts to acquire the distributed leader lock atomically using SET NX EX.
// Only one service instance succeeds, becoming the elected leader responsible for running
// periodic external background sync cycles.
func (r *RedisCacheRepository) AcquireLeaderLock(ctx context.Context, key string, instanceID string, ttl time.Duration) (bool, error) {
	res, err := r.client.SetArgs(ctx, key, instanceID, redis.SetArgs{
		Mode: "NX",
		TTL:  ttl,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, fmt.Errorf("acquire leader lock %q: %w", key, err)
	}
	return res == "OK", nil
}

// RenewLeaderLock extends the TTL of the leader lock if it is currently held by instanceID.
// Executes an atomic Lua script to prevent accidentally renewing a lock that was lost or stolen.
func (r *RedisCacheRepository) RenewLeaderLock(ctx context.Context, key string, instanceID string, ttl time.Duration) (bool, error) {
	const renewScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end`

	ttlMs := int64(ttl / time.Millisecond)
	res, err := r.client.Eval(ctx, renewScript, []string{key}, instanceID, ttlMs).Int64()
	if err != nil {
		return false, fmt.Errorf("renew leader lock %q: %w", key, err)
	}
	return res == 1, nil
}

// ReleaseLeaderLock atomically releases the leader lock if held by instanceID.
// Executes an atomic Lua script to avoid deleting another replica's lock if the lease had expired.
func (r *RedisCacheRepository) ReleaseLeaderLock(ctx context.Context, key string, instanceID string) error {
	const releaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end`

	if err := r.client.Eval(ctx, releaseScript, []string{key}, instanceID).Err(); err != nil {
		return fmt.Errorf("release leader lock %q: %w", key, err)
	}
	return nil
}

