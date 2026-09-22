GO ?= go
APP ?= courier-service
BUILD_DIR ?= bin
COMPOSE ?= docker compose

.PHONY: format test integration race vet build run lrun up down

format:
	$(GO) fmt ./...

test:
	$(GO) test ./...

integration:
	$(GO) test -tags=integration ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

build:
	mkdir -p $(BUILD_DIR)
	$(GO) build -buildvcs=false -trimpath -o $(BUILD_DIR)/$(APP) ./cmd/server

run:
	$(GO) run ./cmd/server

lrun:
	if [ -f "$(BUILD_DIR)/$(APP)" ]; then \
		SERVER_PORT="8080" \
		REDIS_ADDR="redis:6379" \
		ORDERS_FILE_PATH="orders.yml" \
		POLL_INTERVAL="30s" \
		WORKER_CONCURRENCY="10" \
		SLA_LIMIT="100ms" \
		LOCATION_TTL="120s" \
		ORDER_COURIER_MAPPING_TTL="1h" \
		HEARTBEAT_TTL="90s" \
		LEADER_LOCK_TTL="25s" \
		LEADER_RENEW_INTERVAL="10s" \
		CIRCUIT_BREAKER_MAX_FAILURES="5" \
		CIRCUIT_BREAKER_TIMEOUT="15s" \
		PPROF_ENABLED="true" \
		BLOCK_PROFILE_RATE="10000" \
		MUTEX_PROFILE_FRACTION="5" \
		"$(BUILD_DIR)/$(APP)"; \
	else \
		echo "Server binary not found. Please run 'make build' first."; \
		exit 1; \
	fi


up:
	$(COMPOSE) up --build --detach

down:
	$(COMPOSE) down
