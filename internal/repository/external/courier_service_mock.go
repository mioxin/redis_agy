// Package external provides mock implementations of external services.
package external

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"courier-service/internal/domain"
)

// ErrCourierServiceSimulatedTimeout is returned when chaos injection simulates a timeout.
var ErrCourierServiceSimulatedTimeout = errors.New("simulated courier service timeout")

// CourierServiceMock emulates the external Courier Service with a 500ms delay and chaos injection.
// It generates realistic geographic coordinates for a given courier.
type CourierServiceMock struct {
	mu          sync.Mutex
	rng         *rand.Rand
	delay       time.Duration
	timeoutRate float64
	jitter      time.Duration
}

// NewCourierServiceMock creates a new CourierServiceMock with default 500ms delay.
func NewCourierServiceMock() *CourierServiceMock {
	return &CourierServiceMock{
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
		delay: 500 * time.Millisecond,
	}
}

// NewCourierServiceMockWithDelay creates a new CourierServiceMock with custom delay (for testing).
func NewCourierServiceMockWithDelay(delay time.Duration) *CourierServiceMock {
	return &CourierServiceMock{
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
		delay: delay,
	}
}

// SetChaosParams configures timeout simulation rate and latency jitter for resilience testing.
func (m *CourierServiceMock) SetChaosParams(timeoutRate float64, jitter time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timeoutRate = timeoutRate
	m.jitter = jitter
}

// GetCourierLocation retrieves coordinates for the requested courier, simulating latency and chaos injection.
func (m *CourierServiceMock) GetCourierLocation(ctx context.Context, courierID int64) (*domain.Coordinates, error) {
	m.mu.Lock()
	timeoutRate := m.timeoutRate
	jitter := m.jitter
	delay := m.delay
	var shouldTimeout bool
	if timeoutRate > 0 && m.rng.Float64() < timeoutRate {
		shouldTimeout = true
	}
	if jitter > 0 {
		jitterOffset := time.Duration((m.rng.Float64()*2 - 1) * float64(jitter))
		delay += jitterOffset
		if delay < 0 {
			delay = 0
		}
	}
	m.mu.Unlock()

	if shouldTimeout {
		// Simulate network timeout or hanging upstream
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %v", ErrCourierServiceSimulatedTimeout, ctx.Err())
		case <-time.After(2 * time.Second):
			return nil, ErrCourierServiceSimulatedTimeout
		}
	}

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("courier service context canceled: %w", ctx.Err())
		case <-timer.C:
		}
	}

	m.mu.Lock()
	// Base coordinates around Moscow center (lat: 55.7558, lon: 37.6173) with realistic random jitter (~+/- 0.05 degrees)
	latOffset := (m.rng.Float64() - 0.5) * 0.1
	lonOffset := (m.rng.Float64() - 0.5) * 0.1
	// Deterministic courier seed component so locations are somewhat consistent per courier
	courierBiasLat := float64(courierID%20) * 0.005
	courierBiasLon := float64(courierID%20) * 0.005

	latitude := 55.7558 + courierBiasLat + latOffset
	longitude := 37.6173 + courierBiasLon + lonOffset
	m.mu.Unlock()

	return &domain.Coordinates{
		Latitude:  latitude,
		Longitude: longitude,
	}, nil
}
