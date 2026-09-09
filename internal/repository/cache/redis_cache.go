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

const (
	keyPrefixLocation  = "order:%d:location"
	keyPrefixCourier   = "order:%d:courier_id"
	keyActiveOrders    = "tracking:active_orders"
	keyOrderHeartbeat  = "tracking:order:%d:heartbeat"
	keyPollerLastSync  = "courier_poller:last_sync"
)

// RedisCacheRepository implements LocationCacheRepository using Redis.
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
func (r *RedisCacheRepository) RefreshOrderHeartbeat(ctx context.Context, orderID int64, heartbeatTTL time.Duration) error {
	heartbeatKey := fmt.Sprintf(keyOrderHeartbeat, orderID)
	if err := r.client.Set(ctx, heartbeatKey, "1", heartbeatTTL).Err(); err != nil {
		return fmt.Errorf("redis refresh heartbeat for %d: %w", orderID, err)
	}
	return nil
}

// RemoveInactiveOrders removes orders from active tracking whose heartbeat has expired.
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
