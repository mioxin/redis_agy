// Package service coordinates business logic for location retrieval and background synchronization.
package service

import (
	"context"
	"errors"
	"fmt"

	"courier-service/internal/config"
	"courier-service/internal/domain"
	"courier-service/internal/repository/cache"
)

// LocationService defines the business logic contract for retrieving courier locations.
type LocationService interface {
	// GetCourierLocation retrieves courier coordinates for an order.
	// Returns (location, true, nil) on Cache Hit.
	// Returns (nil, false, nil) on Cache Miss (initiating async tracking).
	GetCourierLocation(ctx context.Context, orderID int64) (*domain.CourierLocation, bool, error)
}

// locationServiceImpl implements LocationService.
type locationServiceImpl struct {
	cacheRepo     cache.LocationCacheRepository
	pollerService PollerService
	cfg           *config.Config
}

// NewLocationService creates a new LocationService instance.
func NewLocationService(
	cacheRepo cache.LocationCacheRepository,
	pollerService PollerService,
	cfg *config.Config,
) LocationService {
	return &locationServiceImpl{
		cacheRepo:     cacheRepo,
		pollerService: pollerService,
		cfg:           cfg,
	}
}

// GetCourierLocation checks Redis cache O(1).
// On hit: refreshes heartbeat and returns location (<5ms).
// On miss: registers order into tracking set, triggers urgent background sync, and returns (nil, false, nil).
func (s *locationServiceImpl) GetCourierLocation(ctx context.Context, orderID int64) (*domain.CourierLocation, bool, error) {
	if orderID <= 0 {
		return nil, false, domain.ErrInvalidOrderID
	}

	loc, err := s.cacheRepo.GetOrderLocation(ctx, orderID)
	if err == nil {
		// Cache Hit: refresh sliding heartbeat window
		_ = s.cacheRepo.RefreshOrderHeartbeat(ctx, orderID, s.cfg.HeartbeatTTL)
		return loc, true, nil
	}

	if errors.Is(err, cache.ErrCacheMiss) {
		// Cache Miss: register active order for tracking
		if regErr := s.cacheRepo.RegisterActiveOrder(ctx, orderID, s.cfg.HeartbeatTTL); regErr != nil {
			return nil, false, fmt.Errorf("registering active order: %w", regErr)
		}

		// Trigger asynchronous urgent sync so coordinates will be available on subsequent requests
		if s.pollerService != nil {
			s.pollerService.TriggerUrgentSync(orderID)
		}

		return nil, false, nil
	}

	return nil, false, fmt.Errorf("fetching location from cache: %w", err)
}
