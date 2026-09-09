// Package http provides HTTP handlers and routing for the REST API.
package http

import (
	"encoding/json"
	"net/http"
	"time"

	"courier-service/internal/repository/cache"
)

// HealthHandler handles health check requests.
type HealthHandler struct {
	cacheRepo cache.LocationCacheRepository
	startTime time.Time
}

// NewHealthHandler creates a new HealthHandler.
func NewHealthHandler(cacheRepo cache.LocationCacheRepository) *HealthHandler {
	return &HealthHandler{
		cacheRepo: cacheRepo,
		startTime: time.Now(),
	}
}

// HealthCheckResponse represents health check status.
type HealthCheckResponse struct {
	Status        string  `json:"status"`
	Redis         string  `json:"redis"`
	LastSync      *string `json:"last_sync,omitempty"`
	UptimeSeconds float64 `json:"uptime_seconds"`
}

// HealthCheck handles GET /health.
func (h *HealthHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	redisStatus := "connected"
	if err := h.cacheRepo.Ping(r.Context()); err != nil {
		redisStatus = "unreachable"
	}

	var lastSyncStr *string
	if lastSync, err := h.cacheRepo.GetLastSync(r.Context()); err == nil && !lastSync.IsZero() {
		s := lastSync.Format(time.RFC3339)
		lastSyncStr = &s
	}

	resp := HealthCheckResponse{
		Status:        "ok",
		Redis:         redisStatus,
		LastSync:      lastSyncStr,
		UptimeSeconds: time.Since(h.startTime).Seconds(),
	}

	w.Header().Set("Content-Type", "application/json")
	if redisStatus == "unreachable" {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(resp)
}
