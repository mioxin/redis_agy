// Package config manages application configuration loaded from environment variables with sensible defaults.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration parameters for the Courier Location Service.
type Config struct {
	// Server configuration
	ServerPort string
	SLALimit   time.Duration

	// Redis configuration
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Mock data configuration
	OrdersFilePath string

	// Background Polling Worker configuration
	PollInterval      time.Duration
	WorkerConcurrency int
	UrgentQueueSize   int

	// Cache TTLs
	LocationTTL            time.Duration
	OrderCourierMappingTTL time.Duration
	HeartbeatTTL           time.Duration
}

// Load loads configuration from environment variables or applies production-grade defaults.
func Load() *Config {
	return &Config{
		ServerPort:             getEnv("SERVER_PORT", "8080"),
		SLALimit:               getDurationEnv("SLA_LIMIT", 100*time.Millisecond),
		RedisAddr:              getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:          getEnv("REDIS_PASSWORD", ""),
		RedisDB:                getIntEnv("REDIS_DB", 0),
		OrdersFilePath:         getEnv("ORDERS_FILE_PATH", "orders.yml"),
		PollInterval:           getDurationEnv("POLL_INTERVAL", 30*time.Second),
		WorkerConcurrency:      getIntEnv("WORKER_CONCURRENCY", 10),
		UrgentQueueSize:        getIntEnv("URGENT_QUEUE_SIZE", 1000),
		LocationTTL:            getDurationEnv("LOCATION_TTL", 120*time.Second),
		OrderCourierMappingTTL: getDurationEnv("ORDER_COURIER_MAPPING_TTL", 1*time.Hour),
		HeartbeatTTL:           getDurationEnv("HEARTBEAT_TTL", 90*time.Second),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getIntEnv(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getDurationEnv(key string, defaultVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
