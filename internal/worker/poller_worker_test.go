package worker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"courier-service/internal/config"
	"courier-service/internal/repository/cache"
	"courier-service/internal/worker"
)

type mockPollerService struct {
	mu           sync.Mutex
	syncedOrders []int64
	activeSyncs  int
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeSyncs++
	return nil
}

func TestPollerWorker_StartAndStop(t *testing.T) {
	cfg := &config.Config{
		PollInterval:        50 * time.Millisecond,
		WorkerConcurrency:   2,
		UrgentQueueSize:     10,
		LeaderLockTTL:       1 * time.Second,
		LeaderRenewInterval: 200 * time.Millisecond,
	}

	memCache := cache.NewMemoryCacheRepository()
	mockPoller := newMockPollerService()
	w := worker.NewPollerWorker(mockPoller, memCache, cfg)

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

func TestPollerWorker_LeaderElectionMultiPod(t *testing.T) {
	cfg := &config.Config{
		PollInterval:        20 * time.Millisecond,
		WorkerConcurrency:   2,
		UrgentQueueSize:     10,
		LeaderLockTTL:       100 * time.Millisecond,
		LeaderRenewInterval: 30 * time.Millisecond,
	}

	memCache := cache.NewMemoryCacheRepository()
	poller1 := newMockPollerService()
	poller2 := newMockPollerService()

	w1 := worker.NewPollerWorker(poller1, memCache, cfg)
	w2 := worker.NewPollerWorker(poller2, memCache, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w1.Start(ctx)
	w2.Start(ctx)

	time.Sleep(50 * time.Millisecond)

	// One should be leader, one should be follower
	l1 := w1.IsLeader()
	l2 := w2.IsLeader()

	if !(l1 != l2) {
		t.Fatalf("expected exactly one leader among w1 and w2, got w1=%v, w2=%v", l1, l2)
	}

	// Stop the leader
	if l1 {
		w1.Stop()
	} else {
		w2.Stop()
	}

	// Allow follower to take over leadership
	time.Sleep(120 * time.Millisecond)

	if l1 {
		if !w2.IsLeader() {
			t.Fatalf("expected w2 to acquire leadership after w1 stopped")
		}
		w2.Stop()
	} else {
		if !w1.IsLeader() {
			t.Fatalf("expected w1 to acquire leadership after w2 stopped")
		}
		w1.Stop()
	}
}
