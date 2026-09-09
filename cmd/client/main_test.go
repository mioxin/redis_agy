package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"courier-service/internal/domain"
)

func TestExecuteRequest_Success(t *testing.T) {
	expectedLocation := domain.CourierLocation{
		OrderID:   1,
		CourierID: 10,
		Latitude:  55.75,
		Longitude: 37.61,
		UpdatedAt: time.Now().UTC(),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Response-Time", "0.002ms")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(expectedLocation)
	}))
	defer server.Close()

	client := server.Client()
	job := RequestJob{SeqNum: 1, OrderID: 1}
	res := executeRequest(context.Background(), client, server.URL, 1, job, 100*time.Millisecond)

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got: %d", res.StatusCode)
	}
	if !res.IsSuccess {
		t.Fatalf("expected IsSuccess to be true")
	}
	if res.IsSLABreach {
		t.Fatalf("expected no SLA breach")
	}
	if res.ServerTime != "0.002ms" {
		t.Fatalf("expected server time 0.002ms, got: %s", res.ServerTime)
	}
}

func TestExecuteRequest_Pending(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.Header().Set("X-Response-Time", "0.001ms")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "pending",
			"message": "syncing",
		})
	}))
	defer server.Close()

	client := server.Client()
	job := RequestJob{SeqNum: 1, OrderID: 99}
	res := executeRequest(context.Background(), client, server.URL, 2, job, 100*time.Millisecond)

	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("expected status 202, got: %d", res.StatusCode)
	}
	if !res.IsPending {
		t.Fatalf("expected IsPending to be true")
	}
}

func TestMetricsTracker(t *testing.T) {
	tracker := &MetricsTracker{}

	tracker.Record(RequestResult{
		StatusCode:  http.StatusOK,
		Latency:     2 * time.Millisecond,
		IsSLABreach: false,
	})
	tracker.Record(RequestResult{
		StatusCode:  http.StatusAccepted,
		Latency:     1 * time.Millisecond,
		IsSLABreach: false,
	})
	tracker.Record(RequestResult{
		StatusCode:  http.StatusOK,
		Latency:     120 * time.Millisecond,
		IsSLABreach: true,
	})

	tracker.PrintSummary(100 * time.Millisecond)

	if tracker.total != 3 {
		t.Fatalf("expected total 3, got %d", tracker.total)
	}
	if tracker.success200 != 2 {
		t.Fatalf("expected 2 success, got %d", tracker.success200)
	}
	if tracker.pending202 != 1 {
		t.Fatalf("expected 1 pending, got %d", tracker.pending202)
	}
	if tracker.slaBreaches != 1 {
		t.Fatalf("expected 1 SLA breach, got %d", tracker.slaBreaches)
	}
}

func TestLoadOrderIDs(t *testing.T) {
	ids, err := loadOrderIDs("orders.yml")
	if err != nil {
		t.Fatalf("failed to load orders.yml: %v", err)
	}
	if len(ids) != 50 {
		t.Fatalf("expected 50 order IDs, got: %d", len(ids))
	}
	if ids[0] != 1 || ids[len(ids)-1] != 50 {
		t.Fatalf("expected order IDs from 1 to 50, got first=%d, last=%d", ids[0], ids[len(ids)-1])
	}
}
