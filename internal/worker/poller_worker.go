// Package worker implements background polling and asynchronous synchronization routines.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"courier-service/internal/config"
	"courier-service/internal/service"
)

// PollerWorker manages background periodic updates and urgent location synchronization.
type PollerWorker struct {
	pollerService service.PollerService
	cfg           *config.Config
	stopChan      chan struct{}
	wg            sync.WaitGroup

	inFlightMu sync.Mutex
	inFlight   map[int64]struct{}
}

// NewPollerWorker creates a new PollerWorker instance.
func NewPollerWorker(pollerService service.PollerService, cfg *config.Config) *PollerWorker {
	return &PollerWorker{
		pollerService: pollerService,
		cfg:           cfg,
		stopChan:      make(chan struct{}),
		inFlight:      make(map[int64]struct{}),
	}
}

// Start launches background periodic polling and urgent sync consumer routines.
func (w *PollerWorker) Start(ctx context.Context) {
	slog.Info("Starting PollerWorker",
		slog.Duration("poll_interval", w.cfg.PollInterval),
		slog.Int("concurrency", w.cfg.WorkerConcurrency),
	)

	// 1. Periodic background poller
	w.wg.Add(1)
	go w.runPeriodicSync(ctx)

	// 2. Urgent on-demand queue consumers
	w.wg.Add(1)
	go w.runUrgentQueueConsumer(ctx)
}

// Stop gracefully terminates worker routines and waits for active syncs to finish.
func (w *PollerWorker) Stop() {
	slog.Info("Stopping PollerWorker gracefully...")
	close(w.stopChan)
	w.wg.Wait()
	slog.Info("PollerWorker stopped.")
}

func (w *PollerWorker) runPeriodicSync(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	// Perform an initial sync on startup
	if err := w.pollerService.SyncActiveOrders(ctx); err != nil {
		slog.Warn("Initial periodic sync completed with warning", slog.String("error", err.Error()))
	}

	for {
		select {
		case <-w.stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.pollerService.SyncActiveOrders(ctx); err != nil {
				slog.Error("Periodic active orders sync error", slog.String("error", err.Error()))
			}
		}
	}
}

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

			// Deduplicate in-flight urgent syncs for the same orderID
			w.inFlightMu.Lock()
			if _, running := w.inFlight[orderID]; running {
				w.inFlightMu.Unlock()
				continue
			}
			w.inFlight[orderID] = struct{}{}
			w.inFlightMu.Unlock()

			// Acquire worker concurrency slot
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
