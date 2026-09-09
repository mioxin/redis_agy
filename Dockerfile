# Stage 1: Build static binary
FROM golang:1.24-alpine AS builder

WORKDIR /build

# Install security certificates & git if needed
RUN apk add --no-cache ca-certificates git

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /build/courier-service ./cmd/server

# Stage 2: Minimal runtime image
FROM alpine:3.21

WORKDIR /app

# Add ca-certificates for secure HTTPS calls
RUN apk add --no-cache ca-certificates tzdata

# Copy binary from builder
COPY --from=builder /build/courier-service /app/courier-service

# Copy dataset required by Order Service mock
COPY --from=builder /build/orders.yml /app/orders.yml

# Expose HTTP REST API port
EXPOSE 8080

USER nobody:nobody

ENTRYPOINT ["/app/courier-service"]
