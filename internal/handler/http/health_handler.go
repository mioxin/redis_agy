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

// HealthCheck handles GET /health and GET /healthz (full status diagnostic).
// Returns 200 OK if Redis is connected, or 503 Service Unavailable if Redis is unreachable.
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

// LivenessCheck handles GET /health/live (Kubernetes liveness probe).
// Returns 200 OK as long as the application process is running and its HTTP loop is not deadlocked.
// If this probe fails consecutively, Kubernetes restarts the pod container.
func (h *HealthHandler) LivenessCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "alive",
		"uptime_seconds": time.Since(h.startTime).Seconds(),
	})
}

// ReadinessCheck handles GET /health/ready (Kubernetes readiness probe).
// Verifies connectivity to the required Redis cache dependency.
// If Redis is unreachable, returns 503 Service Unavailable, signaling Kubernetes Service / Ingress
// to temporarily remove this pod from traffic routing until connectivity recovers.
func (h *HealthHandler) ReadinessCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := h.cacheRepo.Ping(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "not ready",
			"error":  err.Error(),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ready",
	})
}

