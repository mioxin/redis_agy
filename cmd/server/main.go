// Package main is the entrypoint for Courier Location Service.
//
// Архитектура приложения и поток данных (Data Flow):
//
// 1. Входящий HTTP-запрос от клиента:
//    Client -> TraceMiddleware (X-Trace-ID) -> Recovery -> LatencyLogger (SLA audit + Prometheus)
//           -> LocationHandler (GET /orders/{orderId}/courier-location)
//
// 2. Read Path (Быстрый путь, O(1), латентность ~1.6 мкс):
//    LocationHandler -> LocationService -> RedisCacheRepository
//    - Cache Hit:  возврат 200 OK + геокоординаты + продление Heartbeat (90 сек).
//    - Cache Miss: возврат 202 Accepted (Retry-After: 1), заказ ставится в Redis-реестр
//                  активных заказов и в неблокирующую очередь urgentQueue воркера.
//
// 3. Sync Path (Фоновый контур синхронизации и опрос внешних систем):
//    PollerWorker (горутины под Distributed Leader Lock) -> PollerService:
//    - Дедупликация через singleflight.Group (защита от Cache Stampede / Thundering Herd).
//    - Защита внешних вызовов через Circuit Breakers (gobreaker/v2).
//    - Разрешение связки Order -> Courier в Order Service (200 мс, кэшируется на 1 час).
//    - Дедупликация курьеров и опрос уникальных курьеров в Courier Service (500 мс).
//    - Пакетная запись (Pipeline) координат в Redis (TTL 120 сек).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"courier-service/internal/config"
	httpHandler "courier-service/internal/handler/http"
	"courier-service/internal/repository/cache"
	"courier-service/internal/repository/external"
	"courier-service/internal/service"
	"courier-service/internal/worker"
)

func main() {
	// =========================================================================
	// 1. Инициализация структурированного логирования (slog JSON)
	// =========================================================================
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Starting Courier Location Service...")

	// =========================================================================
	// 2. Загрузка типизированной конфигурации (ENV с безопасными дефолтами)
	// =========================================================================
	cfg := config.Load()
	slog.Info("Configuration loaded",
		slog.String("port", cfg.ServerPort),
		slog.String("redis_addr", cfg.RedisAddr),
		slog.Duration("sla_limit", cfg.SLALimit),
		slog.Duration("poll_interval", cfg.PollInterval),
		slog.Int("worker_concurrency", cfg.WorkerConcurrency),
		slog.Bool("pprof_enabled", cfg.PprofEnabled),
	)

	// =========================================================================
	// 3. Подключение к Redis (Основное хранилище кэша и распределенных замков)
	// =========================================================================
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	// Проверяем доступность Redis на старте с коротким таймаутом
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := redisClient.Ping(pingCtx).Err(); err != nil {
		slog.Warn("Could not reach Redis at startup (will retry on incoming calls)",
			slog.String("addr", cfg.RedisAddr),
			slog.String("error", err.Error()),
		)
	} else {
		slog.Info("Successfully connected to Redis", slog.String("addr", cfg.RedisAddr))
	}
	pingCancel()

	// Репозиторий кэша: обеспечивает O(1) чтение, пакетную запись (Pipeline) и распределенный замок
	cacheRepo := cache.NewRedisCacheRepository(redisClient)

	// =========================================================================
	// 4. Инициализация моков внешних сервисов с поддержкой хаос-тестирования
	// =========================================================================
	// OrderServiceMock: имитирует внешнюю систему заказов (латентность ~200 мс)
	orderServiceMock, err := external.NewOrderServiceMock(cfg.OrdersFilePath)
	if err != nil {
		slog.Error("Failed to initialize OrderServiceMock",
			slog.String("orders_file", cfg.OrdersFilePath),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
	orderServiceMock.SetChaosParams(cfg.OrderServiceErrorRate, cfg.ExternalLatencyJitter)
	slog.Info("OrderServiceMock initialized successfully with orders database")

	// CourierServiceMock: имитирует внешнюю систему трекинга курьеров (латентность ~500 мс)
	courierServiceMock := external.NewCourierServiceMock()
	courierServiceMock.SetChaosParams(cfg.CourierServiceTimeoutRate, cfg.ExternalLatencyJitter)
	slog.Info("CourierServiceMock initialized successfully")

	// Защитные автоматы Circuit Breaker:
	// При 5 последовательных ошибках изолируют вызовы к упавшему сервису,
	// предотвращая зависание горутин и отдавая мгновенный отказ (<0.1 мс).
	cbOrderService := external.NewCircuitBreakerOrderService(
		orderServiceMock,
		cfg.CircuitBreakerMaxFailures,
		cfg.CircuitBreakerTimeout,
	)
	cbCourierService := external.NewCircuitBreakerCourierService(
		courierServiceMock,
		cfg.CircuitBreakerMaxFailures,
		cfg.CircuitBreakerTimeout,
	)

	// =========================================================================
	// 5. Инициализация доменных сервисов бизнес-логики
	// =========================================================================
	// PollerService: оркестрирует дедупликацию курьеров, singleflight и опрос апстримов
	pollerService := service.NewPollerService(cacheRepo, cbOrderService, cbCourierService, cfg)

	// LocationService: обслуживает запросы чтения клиентов, проверяет кэш O(1) и ставит на трекинг
	locationService := service.NewLocationService(cacheRepo, pollerService, cfg)

	// =========================================================================
	// 6. Cache Pre-Warming (ADR-002)
	// Перед запуском воркера загружаем все заказы из orders.yml в кэш.
	// Используем флаг для координации между подами: первый запущенный под
	// выполнит предзагрузку, остальные увидят флаг и пропустят.
	// =========================================================================
	prewarmCtx, prewarmCancel := context.WithTimeout(context.Background(), 30*time.Second)
	prewarmed, err := cacheRepo.IsPreWarmed(prewarmCtx)
	if err != nil {
		slog.Warn("Failed to check pre-warmed flag, proceeding with pre-warm", slog.String("error", err.Error()))
		prewarmed = false
	}
	if !prewarmed {
		slog.Info("Cache not pre-warmed, starting pre-warm")
		if err := pollerService.PreWarmCache(prewarmCtx); err != nil {
			slog.Error("Cache pre-warm failed", slog.String("error", err.Error()))
		} else {
			slog.Info("Cache pre-warm completed successfully")
			if err := cacheRepo.SetPreWarmed(prewarmCtx); err != nil {
				slog.Warn("Failed to set pre-warmed flag", slog.String("error", err.Error()))
			}
		}
	} else {
		slog.Info("Cache already pre-warmed by another replica, skipping")
	}
	prewarmCancel()

	// =========================================================================
	// 7. Запуск фонового воркера (PollerWorker)
	// =========================================================================
	// Включает:
	// - Контур Distributed Leader Election (только 1 pod-лидер опрашивает апстримы)
	// - Обработчик локальной очереди срочных синхронизаций (urgentQueue) для всех подов
	workerCtx, workerCancel := context.WithCancel(context.Background())
	pollerWorker := worker.NewPollerWorker(pollerService, cacheRepo, cfg)
	pollerWorker.Start(workerCtx)

	// =========================================================================
	// 7. Настройка подсистемы профилирования Go (pprof runtime profiling)
	// =========================================================================
	if cfg.PprofEnabled {
		if cfg.BlockProfileRate > 0 {
			runtime.SetBlockProfileRate(cfg.BlockProfileRate)
		}
		if cfg.MutexProfileFraction > 0 {
			runtime.SetMutexProfileFraction(cfg.MutexProfileFraction)
		}
		slog.Info("pprof runtime profiling enabled",
			slog.Int("block_profile_rate", cfg.BlockProfileRate),
			slog.Int("mutex_profile_fraction", cfg.MutexProfileFraction),
		)
	}

	// =========================================================================
	// 8. Сборка HTTP-роутера, middleware и запуск сервера
	// =========================================================================
	locationHdl := httpHandler.NewLocationHandler(locationService)
	healthHdl := httpHandler.NewHealthHandler(cacheRepo)
	router := httpHandler.NewRouter(locationHdl, healthHdl, cfg.SLALimit, cfg.PprofEnabled)

	server := &http.Server{
		Addr:         ":" + cfg.ServerPort,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Запуск HTTP-сервера в фоновой горутине
	go func() {
		slog.Info("HTTP server listening", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	// =========================================================================
	// 9. Корректное завершение работы (Graceful Shutdown)
	// =========================================================================
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	slog.Info("Received termination signal, initiating graceful shutdown", slog.String("signal", sig.String()))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// 1) Остановка приема новых входящих HTTP-запросов
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server graceful shutdown failed", slog.String("error", err.Error()))
	} else {
		slog.Info("HTTP server stopped cleanly")
	}

	// 2) Остановка фонового воркера и освобождение за замка лидера
	pollerWorker.Stop()
	workerCancel()

	// 3) Закрытие пула соединений с Redis
	if err := redisClient.Close(); err != nil {
		slog.Warn("Error closing Redis connection", slog.String("error", err.Error()))
	} else {
		slog.Info("Redis connection closed")
	}

	slog.Info("Courier Location Service shutdown complete")
}
