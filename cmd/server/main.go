// Package main is the entrypoint for Courier Location Service.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
	// 1. Initialize structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Starting Courier Location Service...")

	// 2. Load configuration
	cfg := config.Load()
	slog.Info("Configuration loaded",
		slog.String("port", cfg.ServerPort),
		slog.String("redis_addr", cfg.RedisAddr),
		slog.Duration("sla_limit", cfg.SLALimit),
		slog.Duration("poll_interval", cfg.PollInterval),
		slog.Int("worker_concurrency", cfg.WorkerConcurrency),
	)

	// 3. Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

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

	cacheRepo := cache.NewRedisCacheRepository(redisClient)

	// 4. Initialize external service mocks
	orderServiceMock, err := external.NewOrderServiceMock(cfg.OrdersFilePath)
	if err != nil {
		slog.Error("Failed to initialize OrderServiceMock",
			slog.String("orders_file", cfg.OrdersFilePath),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
	slog.Info("OrderServiceMock initialized successfully with orders database")

	courierServiceMock := external.NewCourierServiceMock()
	slog.Info("CourierServiceMock initialized successfully")

	// 5. Initialize application domain services
	pollerService := service.NewPollerService(cacheRepo, orderServiceMock, courierServiceMock, cfg)
	locationService := service.NewLocationService(cacheRepo, pollerService, cfg)

	// 6. Start background poller worker
	workerCtx, workerCancel := context.WithCancel(context.Background())
	pollerWorker := worker.NewPollerWorker(pollerService, cfg)
	pollerWorker.Start(workerCtx)

	// 7. Setup HTTP router and server
	locationHdl := httpHandler.NewLocationHandler(locationService)
	healthHdl := httpHandler.NewHealthHandler(cacheRepo)
	router := httpHandler.NewRouter(locationHdl, healthHdl, cfg.SLALimit)

	server := &http.Server{
		Addr:         ":" + cfg.ServerPort,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 8. Launch HTTP server in background
	go func() {
		slog.Info("HTTP server listening", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	// 9. Await termination signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	slog.Info("Received termination signal, initiating graceful shutdown", slog.String("signal", sig.String()))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Stop HTTP server from accepting new connections
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server graceful shutdown failed", slog.String("error", err.Error()))
	} else {
		slog.Info("HTTP server stopped cleanly")
	}

	// Stop background poller worker
	pollerWorker.Stop()
	workerCancel()

	// Close Redis connections
	if err := redisClient.Close(); err != nil {
		slog.Warn("Error closing Redis connection", slog.String("error", err.Error()))
	} else {
		slog.Info("Redis connection closed")
	}

	slog.Info("Courier Location Service shutdown complete")
}
