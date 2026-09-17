// Package worker implements background polling and asynchronous synchronization routines.
//
// Роль пакета в системе:
// Управляет жизненным циклом фоновых горутин:
// 1. Distributed Leader Election: распределенный замок в Redis, гарантирующий, что в кластере
//    Kubernetes (Multi-Pod) только один под-лидер выполняет периодический опрос внешних систем.
// 2. Periodic Poller: регулярный опрос всех активных заказов по таймеру (POLL_INTERVAL).
// 3. Urgent Queue Consumer: пул локальных воркеров (WORKER_CONCURRENCY) для мгновенного прогрева
//    кэша при Cache Miss (работает на всех подах параллельно).
package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"courier-service/internal/config"
	"courier-service/internal/repository/cache"
	"courier-service/internal/service"
)

// Ключ распределенного замка лидера в Redis (SET NX EX)
const leaderLockKey = "courier_poller:leader"

// PollerWorker координирует фоновый опрос и выбор лидера в кластере.
type PollerWorker struct {
	pollerService service.PollerService
	cacheRepo     cache.LocationCacheRepository
	cfg           *config.Config
	instanceID    string
	isLeader      atomic.Bool
	stopChan      chan struct{}
	wg            sync.WaitGroup

	// inFlight предотвращает дублирование активных срочных синхронизаций для одного orderID
	inFlightMu sync.Mutex
	inFlight   map[int64]struct{}
}

// NewPollerWorker создает экземпляр PollerWorker с уникальным instanceID (UUID v4).
func NewPollerWorker(
	pollerService service.PollerService,
	cacheRepo cache.LocationCacheRepository,
	cfg *config.Config,
) *PollerWorker {
	w := &PollerWorker{
		pollerService: pollerService,
		cacheRepo:     cacheRepo,
		cfg:           cfg,
		instanceID:    uuid.New().String(),
		stopChan:      make(chan struct{}),
		inFlight:      make(map[int64]struct{}),
	}
	return w
}

// InstanceID возвращает уникальный идентификатор этого пода/инстанса.
func (w *PollerWorker) InstanceID() string {
	return w.instanceID
}

// IsLeader возвращает true, если данный инстанс удерживает замок лидера в Redis.
func (w *PollerWorker) IsLeader() bool {
	return w.isLeader.Load()
}

// Start запускает 3 фоновых контура:
// 1. runLeaderElection: цикл захвата и продления аренды лидерства (heartbeat).
// 2. runPeriodicSync: периодический опрос активных заказов (активен только у лидера).
// 3. runUrgentQueueConsumer: конкурентный пул консьюмеров срочной очереди (активен на всех подах).
func (w *PollerWorker) Start(ctx context.Context) {
	slog.Info("Starting PollerWorker",
		slog.String("instance_id", w.instanceID),
		slog.Duration("poll_interval", w.cfg.PollInterval),
		slog.Int("concurrency", w.cfg.WorkerConcurrency),
		slog.Duration("leader_lock_ttl", w.cfg.LeaderLockTTL),
	)

	// 1. Контур распределенного лидерства (Distributed Leader Election)
	if w.cacheRepo != nil {
		w.wg.Add(1)
		go w.runLeaderElection(ctx)
	} else {
		// Режим изоляции (например, для локальных мок-тестов без Redis)
		w.isLeader.Store(true)
	}

	// 2. Периодический опрос активных заказов (строго для лидера)
	w.wg.Add(1)
	go w.runPeriodicSync(ctx)

	// 3. Локальные консьюмеры срочной очереди (обрабатываются всеми подами локально)
	w.wg.Add(1)
	go w.runUrgentQueueConsumer(ctx)
}

// Stop осуществляет плавную остановку:
// - Если инстанс был лидером, атомарно освобождает замок в Redis, чтобы standby-под не ждал истечения TTL.
// - Дожидается завершения всех активных горутин в пуле.
func (w *PollerWorker) Stop() {
	slog.Info("Stopping PollerWorker gracefully...", slog.String("instance_id", w.instanceID))
	close(w.stopChan)

	// Освобождение замка лидера в Redis (атомарный Lua DEL)
	if w.isLeader.Load() && w.cacheRepo != nil {
		relCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := w.cacheRepo.ReleaseLeaderLock(relCtx, leaderLockKey, w.instanceID); err != nil {
			slog.Warn("Failed to release leader lock on shutdown", slog.String("error", err.Error()))
		} else {
			slog.Info("Released distributed leader lock cleanly", slog.String("instance_id", w.instanceID))
		}
		cancel()
	}

	w.wg.Wait()
	slog.Info("PollerWorker stopped.", slog.String("instance_id", w.instanceID))
}

// runLeaderElection периодически продлевает аренду замка или пытается перехватить лидерство.
func (w *PollerWorker) runLeaderElection(ctx context.Context) {
	defer w.wg.Done()

	// Первая попытка захвата замка сразу при старте
	w.tryLeaderElection(ctx)

	ticker := time.NewTicker(w.cfg.LeaderRenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tryLeaderElection(ctx)
		}
	}
}

// tryLeaderElection проверяет статус лидерства:
// - Если мы лидер: продлеваем TTL (RenewLeaderLock через PEXPIRE).
// - Если мы standby: пробуем захватить замок (AcquireLeaderLock через SET NX EX 25).
func (w *PollerWorker) tryLeaderElection(ctx context.Context) {
	if w.isLeader.Load() {
		// Продление существующей аренды
		renewed, err := w.cacheRepo.RenewLeaderLock(ctx, leaderLockKey, w.instanceID, w.cfg.LeaderLockTTL)
		if err != nil || !renewed {
			w.isLeader.Store(false)
			slog.Warn("Lost distributed leader status",
				slog.String("instance_id", w.instanceID),
				slog.Any("error", err),
			)
		} else {
			slog.Debug("Renewed distributed leader lease", slog.String("instance_id", w.instanceID))
		}
	} else {
		// Попытка захвата замка
		acquired, err := w.cacheRepo.AcquireLeaderLock(ctx, leaderLockKey, w.instanceID, w.cfg.LeaderLockTTL)
		if err != nil {
			slog.Debug("Failed to acquire leader lock", slog.String("error", err.Error()))
			return
		}

		if acquired {
			w.isLeader.Store(true)
			slog.Info("[LEADER ELECTED] Node acquired distributed poller leadership",
				slog.String("instance_id", w.instanceID),
				slog.Duration("ttl", w.cfg.LeaderLockTTL),
			)
		}
	}
}

// runPeriodicSync запускает периодический пакетный опрос.
// Если данный узел является Standby, он пропускает итерацию, избегая дублирования трафика.
func (w *PollerWorker) runPeriodicSync(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	// Стартовый опрос при запуске (если узел сразу стал лидером)
	if w.isLeader.Load() {
		if err := w.pollerService.SyncActiveOrders(ctx); err != nil {
			slog.Warn("Initial periodic sync completed with warning", slog.String("error", err.Error()))
		}
	}

	for {
		select {
		case <-w.stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Только узел-лидер выполняет синхронизацию
			if !w.isLeader.Load() {
				slog.Debug("Standby node, skipping periodic active orders sync", slog.String("instance_id", w.instanceID))
				continue
			}

			if err := w.pollerService.SyncActiveOrders(ctx); err != nil {
				slog.Error("Periodic active orders sync error", slog.String("error", err.Error()))
			}
		}
	}
}

// runUrgentQueueConsumer обрабатывает срочные задачи из urgentQueue.
// Ограничивает параллелизм через канал-семафор емкостью WorkerConcurrency.
func (w *PollerWorker) runUrgentQueueConsumer(ctx context.Context) {
	defer w.wg.Done()

	sem := make(chan struct{}, w.cfg.WorkerConcurrency)
	queue := w.pollerService.UrgentQueue()

	for {
		select {
		case <-w.stopChan:
			return
		case <-ctx.Done():
			return
		case orderID, ok := <-queue:
			if !ok {
				return
			}

			// Дедупликация в пределах узла: если опрос для orderID уже выполняется, повторно не запускаем
			w.inFlightMu.Lock()
			if _, running := w.inFlight[orderID]; running {
				w.inFlightMu.Unlock()
				continue
			}
			w.inFlight[orderID] = struct{}{}
			w.inFlightMu.Unlock()

			// Захват слота в пуле воркеров (ограничение параллелизма)
			select {
			case sem <- struct{}{}:
			case <-w.stopChan:
				return
			case <-ctx.Done():
				return
			}

			w.wg.Add(1)
			go func(id int64) {
				defer w.wg.Done()
				defer func() { <-sem }()
				defer func() {
					w.inFlightMu.Lock()
					delete(w.inFlight, id)
					w.inFlightMu.Unlock()
				}()

				syncCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				if err := w.pollerService.SyncOrder(syncCtx, id); err != nil {
					slog.Warn("Urgent sync failed for order", slog.Int64("order_id", id), slog.String("error", err.Error()))
				}
			}(orderID)
		}
	}
}
