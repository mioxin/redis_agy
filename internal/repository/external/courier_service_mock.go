// Package external provides mock implementations of external services.
package external

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"courier-service/internal/domain"
)

// CourierServiceMock emulates the external Courier Service with a 500ms delay.
// It generates realistic geographic coordinates for a given courier.
type CourierServiceMock struct {
	mu    sync.Mutex
	rng   *rand.Rand
	delay time.Duration
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

// GetCourierLocation retrieves coordinates for the requested courier, simulating 500ms latency.
func (m *CourierServiceMock) GetCourierLocation(ctx context.Context, courierID int64) (*domain.Coordinates, error) {
	if m.delay > 0 {
		timer := time.NewTimer(m.delay)
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
