package external_test

import (
	"context"
	"testing"
	"time"

	"courier-service/internal/repository/external"
)

func TestCourierServiceMock_GetCourierLocation(t *testing.T) {
	mock := external.NewCourierServiceMockWithDelay(10 * time.Millisecond)
	ctx := context.Background()

	t.Run("Valid coordinates generated", func(t *testing.T) {
		start := time.Now()
		coords, err := mock.GetCourierLocation(ctx, 42)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if coords.Latitude == 0 || coords.Longitude == 0 {
			t.Fatalf("expected non-zero coordinates, got lat: %f, lon: %f", coords.Latitude, coords.Longitude)
		}
		if duration < 10*time.Millisecond {
			t.Fatalf("expected at least 10ms delay, got: %v", duration)
		}
	})

	t.Run("Context canceled", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := mock.GetCourierLocation(cancelCtx, 42)
		if err == nil {
			t.Fatal("expected error on canceled context, got nil")
		}
	})
}
