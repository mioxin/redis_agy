package worker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"courier-service/internal/config"
	"courier-service/internal/worker"
)

type mockPollerService struct {
	mu           sync.Mutex
	syncedOrders []int64
	urgentQueue  chan int64
}

func newMockPollerService() *mockPollerService {
	return &mockPollerService{
		urgentQueue: make(chan int64, 10),
	}
}

func (m *mockPollerService) TriggerUrgentSync(orderID int64) {
	m.urgentQueue <- orderID
}

func (m *mockPollerService) UrgentQueue() <-chan int64 {
	return m.urgentQueue
}

func (m *mockPollerService) SyncOrder(ctx context.Context, orderID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncedOrders = append(m.syncedOrders, orderID)
	return nil
}

func (m *mockPollerService) SyncActiveOrders(ctx context.Context) error {
	return nil
}

func TestPollerWorker_StartAndStop(t *testing.T) {
	cfg := &config.Config{
		PollInterval:      50 * time.Millisecond,
		WorkerConcurrency: 2,
		UrgentQueueSize:   10,
	}

	mockPoller := newMockPollerService()
	w := worker.NewPollerWorker(mockPoller, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w.Start(ctx)

	// Send an urgent order
	mockPoller.TriggerUrgentSync(123)

	// Wait briefly for worker to consume
	time.Sleep(30 * time.Millisecond)

	w.Stop()

	mockPoller.mu.Lock()
	defer mockPoller.mu.Unlock()
	if len(mockPoller.syncedOrders) != 1 || mockPoller.syncedOrders[0] != 123 {
		t.Fatalf("expected order 123 to be synced, got: %v", mockPoller.syncedOrders)
	}
}
