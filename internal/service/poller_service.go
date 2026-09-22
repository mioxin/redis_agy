// Package service coordinates business logic for location retrieval and background synchronization.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"courier-service/internal/config"
	"courier-service/internal/domain"
	"courier-service/internal/repository/cache"
	"courier-service/internal/repository/external"
	"courier-service/internal/telemetry"
)

// PollerService координирует периодическую и срочную синхронизацию координат курьеров.
type PollerService interface {
	// TriggerUrgentSync ставит orderID в буферизованную очередь на немедленный опрос.
	TriggerUrgentSync(orderID int64)
	// UrgentQueue возвращает канал только для чтения для консьюмеров воркера.
	UrgentQueue() <-chan int64
	// SyncOrder выполняет точечную синхронизацию одного заказа с singleflight-дедупликацией.
	SyncOrder(ctx context.Context, orderID int64) error
	// SyncActiveOrders выполняет пакетный опрос всех активных заказов с дедупликацией курьеров.
	SyncActiveOrders(ctx context.Context) error
	// PreWarmCache предзагружает кэш при старте: загружает все заказы из orders.yml,
	// регистрирует их в active_orders + heartbeats + маппинги, группирует по courier_id,
	// конкурентно опрашивает Courier Service и пакетно сохраняет локации.
	PreWarmCache(ctx context.Context) error
}

// pollerServiceImpl реализует интерфейс PollerService.
//
// Связи с компонентами:
// - cacheRepo: чтение/запись кэша, маппингов и реестра активности заказов.
// - orderClient: клиент Order Service (обернут в Circuit Breaker).
// - courierClient: клиент Courier Service (обернут в Circuit Breaker).
// - syncSF: singleflight группа для предотвращения дублирующих одновременных SyncOrder.
// - orderSF: singleflight группа для предотвращения параллельных запросов в Order Service.
type pollerServiceImpl struct {
	cacheRepo     cache.LocationCacheRepository
	orderClient   external.OrderServiceClient
	courierClient external.CourierServiceClient
	cfg           *config.Config
	urgentQueue   chan int64
	syncSF        singleflight.Group
	orderSF       singleflight.Group
}

// NewPollerService создает экземпляр PollerService.
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

// TriggerUrgentSync отправляет заказ в неблокирующую очередь срочного опроса.
// Если очередь переполнена, повторный запрос пропускается во избежание блокировки вызывающего потока.
func (p *pollerServiceImpl) TriggerUrgentSync(orderID int64) {
	select {
	case p.urgentQueue <- orderID:
		slog.Debug("Urgent sync queued", slog.Int64("order_id", orderID))
	default:
		slog.Warn("Urgent sync queue is full, skipping duplicate push", slog.Int64("order_id", orderID))
	}
}

// UrgentQueue возвращает канал входящих срочных синхронизаций.
func (p *pollerServiceImpl) UrgentQueue() <-chan int64 {
	return p.urgentQueue
}

// getOrderWithSingleflight запрашивает связку заказа у Order Service, дедуплицируя параллельные вызовы.
// Если 50 горутин одновременно запросят заказ 42, Order Service будет вызван ровно 1 раз.
func (p *pollerServiceImpl) getOrderWithSingleflight(ctx context.Context, orderID int64) (*domain.Order, error) {
	res, err, _ := p.orderSF.Do(fmt.Sprintf("order:%d", orderID), func() (interface{}, error) {
		return p.orderClient.GetOrderByID(ctx, orderID)
	})
	if err != nil {
		return nil, err
	}
	return res.(*domain.Order), nil
}

// SyncOrder выполняет точечную синхронизацию одного заказа:
// 1. singleflight.Group: защищает от Cache Stampede (Thundering Herd) при одновременных вызовах.
// 2. Проверяет маппинг order -> courier в Redis (TTL 1 час). Если нет — опрашивает Order Service (200 мс).
// 3. Запрашивает текущие координаты в Courier Service (500 мс).
// 4. Сохраняет координаты в Redis (TTL 120 с) и обновляет кэш.
func (p *pollerServiceImpl) SyncOrder(ctx context.Context, orderID int64) error {
	key := fmt.Sprintf("sync:order:%d", orderID)
	_, err, _ := p.syncSF.Do(key, func() (interface{}, error) {
		// Шаг 1: Проверка маппинга order -> courier в кэше
		courierID, err := p.cacheRepo.GetOrderCourierMapping(ctx, orderID)
		if err != nil {
			if !errors.Is(err, cache.ErrCacheMiss) {
				slog.Warn("Failed to read order mapping from cache", slog.Int64("order_id", orderID), slog.String("error", err.Error()))
			}

			// Промах маппинга: вызываем Order Service строго один раз через singleflight
			order, oErr := p.getOrderWithSingleflight(ctx, orderID)
			if oErr != nil {
				return nil, fmt.Errorf("order service lookup for %d: %w", orderID, oErr)
			}

			courierID = order.CourierID
			// Кэшируем связку на 1 час (курьер не меняется во время доставки заказа)
			if err := p.cacheRepo.SetOrderCourierMapping(ctx, orderID, courierID, p.cfg.OrderCourierMappingTTL); err != nil {
				slog.Warn("Failed to cache order courier mapping", slog.Int64("order_id", orderID), slog.String("error", err.Error()))
			}
		}

		// Шаг 2: Запрос координат курьера в Courier Service (~500 мс)
		coords, cErr := p.courierClient.GetCourierLocation(ctx, courierID)
		if cErr != nil {
			return nil, fmt.Errorf("courier service lookup for courier %d: %w", courierID, cErr)
		}

		// Шаг 3: Сохранение координат в Redis (TTL 120s)
		loc := domain.CourierLocation{
			OrderID:   orderID,
			CourierID: courierID,
			Latitude:  coords.Latitude,
			Longitude: coords.Longitude,
			UpdatedAt: time.Now().UTC(),
		}

		if err := p.cacheRepo.SetOrderLocation(ctx, loc, p.cfg.LocationTTL); err != nil {
			return nil, fmt.Errorf("saving location to cache for order %d: %w", orderID, err)
		}

		slog.Info("Successfully synced order location",
			slog.Int64("order_id", orderID),
			slog.Int64("courier_id", courierID),
			slog.Float64("lat", loc.Latitude),
			slog.Float64("lon", loc.Longitude),
		)

		return nil, nil
	})

	return err
}

// SyncActiveOrders выполняет периодический пакетный опрос всех активных заказов:
// 1. Очищает неактивные заказы (чьи heartbeats старше 90 сек).
// 2. Извлекает список активных orderID из Redis Set.
// 3. Резолвит курьеров для заказов с помощью кэша маппинга.
// 4. ДЕДУПЛИКАЦИЯ КУРЬЕРОВ: если курьер везет 3 заказа, опрашиваем Courier Service строго 1 раз!
// 5. Конкурентно запрашивает координаты уникальных курьеров через пул воркеров.
// 6. Пакетно сохраняет обновленные локации в Redis за один сетевой вызов (Pipeline).
// 7. Обновляет метрики Prometheus (ActiveOrdersGauge, PollerLastSyncGauge).
func (p *pollerServiceImpl) SyncActiveOrders(ctx context.Context) error {
	// 1. Удаление протухших заказов из множества tracking:active_orders
	if err := p.cacheRepo.RemoveInactiveOrders(ctx); err != nil {
		slog.Warn("Failed to clean up inactive orders", slog.String("error", err.Error()))
	}

	// 2. Получение текущих активных заказов
	orderIDs, err := p.cacheRepo.GetActiveOrderIDs(ctx)
	if err != nil {
		return fmt.Errorf("retrieving active orders: %w", err)
	}

	if len(orderIDs) == 0 {
		now := time.Now().UTC()
		_ = p.cacheRepo.SetLastSync(ctx, now)
		telemetry.SetLastSyncTimestamp(now)
		telemetry.SetActiveOrders(0)
		return nil
	}

	telemetry.SetActiveOrders(len(orderIDs))
	slog.Debug("Polling active orders", slog.Int("active_count", len(orderIDs)))

	// 3. Разрешение courier_id для всех заказов (через кэш или параллельный вызов Order Service)
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

	// Если по новым заказам еще нет маппинга, опрашиваем Order Service с ограничением параллелизма
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

	// 4. ДЕДУПЛИКАЦИЯ: группируем заказы по courier_id
	courierToOrders := make(map[int64][]int64)
	for oID, cID := range orderToCourier {
		courierToOrders[cID] = append(courierToOrders[cID], oID)
	}

	uniqueCouriers := make([]int64, 0, len(courierToOrders))
	for cID := range courierToOrders {
		uniqueCouriers = append(uniqueCouriers, cID)
	}

	// 5. Опрашиваем Courier Service строго один раз на каждого уникального курьера
	courierCoordinates := p.fetchCourierCoordinatesConcurrently(ctx, uniqueCouriers)

	// 6. Формируем пачку обновленных координат для всех связанных заказов
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

	// 7. Атомарная пакетная запись в Redis через Pipeline
	if err := p.cacheRepo.SetOrderLocationsBatch(ctx, locations, p.cfg.LocationTTL); err != nil {
		return fmt.Errorf("batch setting order locations: %w", err)
	}

	// 8. Фиксация временной метки завершения цикла и обновление метрик Prometheus
	_ = p.cacheRepo.SetLastSync(ctx, now)
	telemetry.SetLastSyncTimestamp(now)
	slog.Info("Completed active orders sync cycle",
		slog.Int("orders_updated", len(locations)),
		slog.Int("unique_couriers_polled", len(uniqueCouriers)),
	)

	return nil
}

// fetchMissingOrderCouriers параллельно запрашивает Order Service с семафором WorkerConcurrency.
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

// fetchCourierCoordinatesConcurrently параллельно опрашивает Courier Service с семафором WorkerConcurrency.
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

// PreWarmCache предзагружает кэш при старте сервера (ADR-002).
// Поток:
// 1. Получает все заказы через orderClient.GetAllOrders().
// 2. Вызывает cacheRepo.PreWarmOrders() — единая точка записи: active_orders + heartbeats + маппинги.
// 3. Группирует заказы по courier_id, дедупликует.
// 4. Конкурентно опрашивает Courier Service через пул воркеров (WorkerConcurrency).
// 5. Пакетно сохраняет локации через SetOrderLocationsBatch (Pipeline, TTL 120с).
func (p *pollerServiceImpl) PreWarmCache(ctx context.Context) error {
	// Шаг 1: Получаем все заказы из orders.yml (источник данных для предзагрузки)
	allOrders := p.orderClient.GetAllOrders()
	if len(allOrders) == 0 {
		slog.Info("No orders to pre-warm")
		return nil
	}

	slog.Info("Starting cache pre-warm", slog.Int("order_count", len(allOrders)))

	// Шаг 2: Единая точка записи в кэш — active_orders + heartbeats + маппинги (один Pipeline)
	if err := p.cacheRepo.PreWarmOrders(ctx, allOrders, p.cfg.HeartbeatTTL, p.cfg.OrderCourierMappingTTL); err != nil {
		return fmt.Errorf("pre-warm orders cache: %w", err)
	}

	// Шаг 3: Группируем заказы по courier_id (дедупликация курьеров)
	courierToOrders := make(map[int64][]int64)
	for _, o := range allOrders {
		courierToOrders[o.CourierID] = append(courierToOrders[o.CourierID], o.ID)
	}

	uniqueCouriers := make([]int64, 0, len(courierToOrders))
	for cID := range courierToOrders {
		uniqueCouriers = append(uniqueCouriers, cID)
	}

	// Шаг 4: Конкурентно опрашиваем Courier Service с семафором WorkerConcurrency
	courierCoordinates := p.fetchCourierCoordinatesConcurrently(ctx, uniqueCouriers)

	// Шаг 5: Формируем пачку локаций и пакетно сохраняем
	now := time.Now().UTC()
	locations := make([]domain.CourierLocation, 0, len(allOrders))

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

	if len(locations) > 0 {
		if err := p.cacheRepo.SetOrderLocationsBatch(ctx, locations, p.cfg.LocationTTL); err != nil {
			return fmt.Errorf("batch setting pre-warm locations: %w", err)
		}
	}

	slog.Info("Cache pre-warm completed",
		slog.Int("orders_pre_warmed", len(allOrders)),
		slog.Int("unique_couriers_polled", len(uniqueCouriers)),
		slog.Int("locations_saved", len(locations)),
	)

	return nil
}
