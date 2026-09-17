// Package service coordinates business logic for location retrieval and background synchronization.
//
// Роль пакета в системе:
// Реализует паттерн разделения контуров (CQRS):
// - LocationService: Fast Path чтения клиентских запросов O(1), управление Heartbeat и трекингом.
// - PollerService: Sync Path синхронизации, singleflight дедупликация, пакетный опрос внешних систем.
package service

import (
	"context"
	"errors"
	"fmt"

	"courier-service/internal/config"
	"courier-service/internal/domain"
	"courier-service/internal/repository/cache"
	"courier-service/internal/telemetry"
)

// LocationService defines the business logic contract for retrieving courier locations.
type LocationService interface {
	// GetCourierLocation retrieves courier coordinates for an order.
	// Returns (location, true, nil) on Cache Hit.
	// Returns (nil, false, nil) on Cache Miss (initiating async tracking).
	GetCourierLocation(ctx context.Context, orderID int64) (*domain.CourierLocation, bool, error)
}

// locationServiceImpl implements LocationService.
// Координирует доступ к RedisCacheRepository и передачу сигналов в PollerService.
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

// GetCourierLocation реализует реактивный паттерн отслеживания заказов (Reactive On-Demand Tracking):
//
// 1. Проверяет наличие координат в кэше Redis O(1).
// 2. Сценарий Cache Hit:
//    - Инкрементирует метрику courier_cache_hits_total в Prometheus.
//    - Продлевает sliding window интереса клиента к заказу (Heartbeat TTL 90 сек).
//    - Мгновенно отдает координаты клиенту (~1.6 мкс, SLA < 100 мс соблюден).
// 3. Сценарий Cache Miss (первое обращение по заказу):
//    - Инкрементирует метрику courier_cache_misses_total.
//    - Регистрирует orderId в Redis-множестве активных заказов (tracking:active_orders).
//    - Отправляет неблокирующий сигнал в очередь urgentQueue сервиса опроса.
//    - Возвращает found = false для формирования клиенту неблокирующего 202 Accepted.
func (s *locationServiceImpl) GetCourierLocation(ctx context.Context, orderID int64) (*domain.CourierLocation, bool, error) {
	if orderID <= 0 {
		return nil, false, domain.ErrInvalidOrderID
	}

	// 1. Попытка мгновенного чтения из Redis O(1)
	loc, err := s.cacheRepo.GetOrderLocation(ctx, orderID)
	if err == nil {
		// Cache Hit: заказ уже отслеживается, продлеваем heartbeat активности
		telemetry.IncCacheHit()
		_ = s.cacheRepo.RefreshOrderHeartbeat(ctx, orderID, s.cfg.HeartbeatTTL)
		return loc, true, nil
	}

	if errors.Is(err, cache.ErrCacheMiss) {
		// Cache Miss: ставим заказ на мониторинг (Reactive On-Demand)
		telemetry.IncCacheMiss()

		// Регистрация заказа в множестве active_orders и установка heartbeat таймера
		if regErr := s.cacheRepo.RegisterActiveOrder(ctx, orderID, s.cfg.HeartbeatTTL); regErr != nil {
			return nil, false, fmt.Errorf("registering active order: %w", regErr)
		}

		// Сигнал воркеру для срочного опроса курьера вне планового 30с цикла
		if s.pollerService != nil {
			s.pollerService.TriggerUrgentSync(orderID)
		}

		return nil, false, nil
	}

	return nil, false, fmt.Errorf("fetching location from cache: %w", err)
}
