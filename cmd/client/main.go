// Package main implements a CLI client for the Courier Location Service.
// It orchestrates requests based on orders.yml using workflow orchestration patterns
// (Fan-Out/Fan-In, Worker Pool, Rate-Limiting, and Metrics Aggregation).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// ANSI color codes for readable terminal output.
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

// ordersFile represents the structure of orders.yml.
type ordersFile struct {
	Orders []struct {
		ID        int64 `yaml:"id"`
		CourierID int64 `yaml:"courier_id"`
	} `yaml:"orders"`
}

// LocationResponse matches the HTTP 200 payload.
type LocationResponse struct {
	OrderID   int64     `json:"order_id"`
	CourierID int64     `json:"courier_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PendingResponse matches the HTTP 202 payload.
type PendingResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// RequestJob is a unit of work for a worker in the pool.
type RequestJob struct {
	SeqNum  int
	OrderID int64
}

// RequestResult holds measurement and payload details for an executed request.
type RequestResult struct {
	SeqNum       int
	WorkerID     int
	OrderID      int64
	StatusCode   int
	Latency      time.Duration
	ServerTime   string
	Data         string
	Error        error
	IsSLABreach  bool
	IsSuccess    bool
	IsPending    bool
}

// MetricsTracker aggregates latency and SLA metrics across all workers.
type MetricsTracker struct {
	mu           sync.Mutex
	latencies    []time.Duration
	total        int
	success200   int
	pending202   int
	clientErrors int
	serverErrors int
	slaBreaches  int
}

func (m *MetricsTracker) Record(r RequestResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.total++
	m.latencies = append(m.latencies, r.Latency)

	if r.IsSLABreach {
		m.slaBreaches++
	}

	switch {
	case r.StatusCode == http.StatusOK:
		m.success200++
	case r.StatusCode == http.StatusAccepted:
		m.pending202++
	case r.StatusCode >= 400 && r.StatusCode < 500:
		m.clientErrors++
	default:
		m.serverErrors++
	}
}

func (m *MetricsTracker) PrintSummary(slaLimit time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.latencies) == 0 {
		fmt.Println("\nNo requests were completed.")
		return
	}

	sort.Slice(m.latencies, func(i, j int) bool {
		return m.latencies[i] < m.latencies[j]
	})

	var sum time.Duration
	for _, l := range m.latencies {
		sum += l
	}
	avg := sum / time.Duration(len(m.latencies))
	min := m.latencies[0]
	max := m.latencies[len(m.latencies)-1]
	p50 := m.latencies[int(float64(len(m.latencies))*0.50)]
	p95 := m.latencies[int(float64(len(m.latencies))*0.95)]
	p99 := m.latencies[int(float64(len(m.latencies))*0.99)]

	slaPassCount := m.total - m.slaBreaches
	slaPercent := (float64(slaPassCount) / float64(m.total)) * 100.0

	fmt.Println("\n" + colorBold + "================================================================================" + colorReset)
	fmt.Printf("%s                  COURIER LOCATION SERVICE - TEST SUMMARY                     %s\n", colorBold, colorReset)
	fmt.Println(colorBold + "================================================================================" + colorReset)
	fmt.Printf(" Total Requests Sent : %s%d%s\n", colorBold, m.total, colorReset)
	fmt.Printf("   ├─ HTTP 200 OK    : %s%d%s (Cache Hit: coordinates returned)\n", colorGreen, m.success200, colorReset)
	fmt.Printf("   ├─ HTTP 202 Async : %s%d%s (Cache Miss: tracking started, non-blocking)\n", colorYellow, m.pending202, colorReset)
	fmt.Printf("   ├─ HTTP 4xx Error : %s%d%s\n", colorRed, m.clientErrors, colorReset)
	fmt.Printf("   └─ HTTP 5xx Error : %s%d%s\n", colorRed, m.serverErrors, colorReset)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(colorBold + " Latency Statistics (Client Round-Trip):" + colorReset)
	fmt.Printf("   Min Latency       : %s%v%s\n", colorCyan, min, colorReset)
	fmt.Printf("   Avg Latency       : %s%v%s\n", colorCyan, avg, colorReset)
	fmt.Printf("   Max Latency       : %s%v%s\n", colorCyan, max, colorReset)
	fmt.Printf("   Median (p50)      : %s%v%s\n", colorCyan, p50, colorReset)
	fmt.Printf("   95th Percentile   : %s%v%s\n", colorCyan, p95, colorReset)
	fmt.Printf("   99th Percentile   : %s%v%s\n", colorCyan, p99, colorReset)
	fmt.Println("--------------------------------------------------------------------------------")
	slaColor := colorGreen
	if slaPercent < 100.0 {
		slaColor = colorYellow
	}
	if slaPercent < 95.0 {
		slaColor = colorRed
	}
	fmt.Printf(" SLA Compliance (< %v) : %s%.2f%%%s (%d/%d requests passed)\n",
		slaLimit, slaColor, slaPercent, colorReset, slaPassCount, m.total)
	fmt.Printf(" SLA Violations (>= %v) : %s%d%s\n", slaLimit, colorRed, m.slaBreaches, colorReset)
	fmt.Println(colorBold + "================================================================================" + colorReset)
}

func main() {
	// 1. Command-line flags
	baseURL := flag.String("url", "http://localhost:8080", "Base URL of the Courier Location Service")
	ordersPath := flag.String("file", "orders.yml", "Path to orders.yml file")
	threads := flag.Int("threads", 5, "Number of concurrent worker threads (goroutines)")
	requests := flag.Int("requests", 50, "Total number of requests to execute (0 for unlimited loop)")
	interval := flag.Duration("interval", 30*time.Second, "Period between requests per order (per task.md, default: 30s)")
	requestTimeout := flag.Duration("timeout", 5*time.Second, "HTTP timeout per request")
	slaLimit := flag.Duration("sla", 100*time.Millisecond, "SLA threshold limit")
	flag.Parse()

	// 2. Load order IDs from orders.yml
	orders, err := loadOrderIDs(*ordersPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError reading %s: %v%s\n", colorRed, *ordersPath, err, colorReset)
		os.Exit(1)
	}
	if len(orders) == 0 {
		fmt.Fprintf(os.Stderr, "%sNo orders found in %s%s\n", colorRed, *ordersPath, colorReset)
		os.Exit(1)
	}

	fmt.Println(colorBold + "================================================================================" + colorReset)
	fmt.Printf("%s   Courier Location Service CLI Client       %s\n", colorCyan, colorReset)
	fmt.Println(colorBold + "================================================================================" + colorReset)
	fmt.Printf(" Target Endpoint     : %s%s%s\n", colorBold, *baseURL, colorReset)
	fmt.Printf(" Orders Source File  : %s (%d registered orders loaded)\n", *ordersPath, len(orders))
	fmt.Printf(" Concurrent Threads  : %s%d workers%s\n", colorBold, *threads, colorReset)
	if *requests > 0 {
		fmt.Printf(" Total Requests      : %s%d%s\n", colorBold, *requests, colorReset)
	} else {
		fmt.Printf(" Total Requests      : %sContinuous Loop%s (Press Ctrl+C to stop)\n", colorYellow, colorReset)
	}
	fmt.Printf(" Request Interval    : %s%v%s (per task.md: client queries every 30s)\n", colorBold, *interval, colorReset)
	fmt.Printf(" SLA Target Limit    : %s< %v%s\n", colorGreen, *slaLimit, colorReset)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("TIME         WORKER   ORDER     STATUS         LATENCY    SERVER-TIME  SLA-AUDIT  RESULT")
	fmt.Println("--------------------------------------------------------------------------------")

	// 3. Setup context with graceful interrupt handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Printf("\n%s[INTERRUPT] Stopping workers gracefully...%s\n", colorYellow, colorReset)
		cancel()
	}()

	// 4. Orchestration: Fan-Out / Fan-In Channels
	jobsChan := make(chan RequestJob, *threads*2)
	resultsChan := make(chan RequestResult, *threads*2)

	httpClient := &http.Client{
		Timeout: *requestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: *threads * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	tracker := &MetricsTracker{}

	// 5. Start Worker Pool (Activities / Runners)
	var workersWg sync.WaitGroup
	for w := 1; w <= *threads; w++ {
		workersWg.Add(1)
		go func(workerID int) {
			defer workersWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobsChan:
					if !ok {
						return
					}
					result := executeRequest(ctx, httpClient, *baseURL, workerID, job, *slaLimit)
					resultsChan <- result
				}
			}
		}(w)
	}

	// 6. Start Aggregator (Fan-In Collector & Real-time Printer)
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		for res := range resultsChan {
			tracker.Record(res)
			printResult(res)
		}
	}()

	// 7. Dispatcher (Workflow Producer)
	go func() {
		defer close(jobsChan)
		orderIndex := 0
		reqCount := 0

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if *requests > 0 && reqCount >= *requests {
				return
			}

			reqCount++
			orderID := orders[orderIndex%len(orders)]
			orderIndex++

			select {
			case <-ctx.Done():
				return
			case jobsChan <- RequestJob{SeqNum: reqCount, OrderID: orderID}:
			}

			// If interval > 0, throttle between dispatch cycles
			if *interval > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(*interval / time.Duration(*threads)):
					// Inter-request pacing to evenly distribute requests across the period
				}
			}
		}
	}()

	// 8. Await completion
	workersWg.Wait()
	close(resultsChan)
	collectorWg.Wait()

	// 9. Display aggregated SLA report
	tracker.PrintSummary(*slaLimit)
}

func executeRequest(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	workerID int,
	job RequestJob,
	slaLimit time.Duration,
) RequestResult {
	url := fmt.Sprintf("%s/orders/%d/courier-location", baseURL, job.OrderID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return RequestResult{
			SeqNum:     job.SeqNum,
			WorkerID:   workerID,
			OrderID:    job.OrderID,
			Error:      err,
			Data:       fmt.Sprintf("request init error: %v", err),
		}
	}

	start := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(start)

	if err != nil {
		return RequestResult{
			SeqNum:      job.SeqNum,
			WorkerID:    workerID,
			OrderID:     job.OrderID,
			Latency:     duration,
			Error:       err,
			IsSLABreach: duration >= slaLimit,
			Data:        fmt.Sprintf("network error: %v", err),
		}
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return RequestResult{
			SeqNum:      job.SeqNum,
			WorkerID:    workerID,
			OrderID:     job.OrderID,
			StatusCode:  resp.StatusCode,
			Latency:     duration,
			Error:       err,
			IsSLABreach: duration >= slaLimit,
			Data:        fmt.Sprintf("read error: %v", err),
		}
	}

	serverTime := resp.Header.Get("X-Response-Time")
	if serverTime == "" {
		serverTime = "-"
	}

	var dataStr string
	isSuccess := false
	isPending := false

	switch resp.StatusCode {
	case http.StatusOK:
		var loc LocationResponse
		if err := json.Unmarshal(bodyBytes, &loc); err == nil {
			dataStr = fmt.Sprintf("Courier #%-2d | Lat: %-8.4f Lon: %-8.4f", loc.CourierID, loc.Latitude, loc.Longitude)
		} else {
			dataStr = string(bodyBytes)
		}
		isSuccess = true

	case http.StatusAccepted:
		var pend PendingResponse
		retryAfter := resp.Header.Get("Retry-After")
		if err := json.Unmarshal(bodyBytes, &pend); err == nil {
			dataStr = fmt.Sprintf("Pending (%s) [Retry-After: %ss]", pend.Status, retryAfter)
		} else {
			dataStr = fmt.Sprintf("Pending [Retry-After: %ss]", retryAfter)
		}
		isPending = true

	default:
		dataStr = string(bodyBytes)
	}

	return RequestResult{
		SeqNum:      job.SeqNum,
		WorkerID:    workerID,
		OrderID:     job.OrderID,
		StatusCode:  resp.StatusCode,
		Latency:     duration,
		ServerTime:  serverTime,
		Data:        dataStr,
		IsSLABreach: duration >= slaLimit,
		IsSuccess:   isSuccess,
		IsPending:   isPending,
	}
}

func printResult(r RequestResult) {
	timestamp := time.Now().Format("15:04:05.000")
	workerTag := fmt.Sprintf("[W-%d]", r.WorkerID)
	orderTag := fmt.Sprintf("#%-4d", r.OrderID)

	var statusTag string
	switch r.StatusCode {
	case http.StatusOK:
		statusTag = colorGreen + "200 OK      " + colorReset
	case http.StatusAccepted:
		statusTag = colorYellow + "202 ACCEPTED" + colorReset
	case http.StatusBadRequest:
		statusTag = colorRed + "400 BAD REQ " + colorReset
	case 0:
		statusTag = colorRed + "CONN ERROR  " + colorReset
	default:
		statusTag = fmt.Sprintf("%s%-12d%s", colorRed, r.StatusCode, colorReset)
	}

	latencyStr := fmt.Sprintf("%.2fms", float64(r.Latency.Microseconds())/1000.0)

	slaTag := colorGreen + "PASS" + colorReset
	if r.IsSLABreach {
		slaTag = colorRed + "BREACH" + colorReset
	}

	fmt.Printf("%s  %-6s  %-6s  %-12s  %-9s  %-11s  %-9s  %s\n",
		timestamp,
		workerTag,
		orderTag,
		statusTag,
		latencyStr,
		r.ServerTime,
		slaTag,
		r.Data,
	)
}

func loadOrderIDs(filePath string) ([]int64, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		// Fallback for tests running from subdirectories
		altPath := "../../" + filePath
		if altData, altErr := os.ReadFile(altPath); altErr == nil {
			data = altData
		} else {
			return nil, fmt.Errorf("reading orders file %q: %w", filePath, err)
		}
	}

	var parsed ordersFile
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}

	ids := make([]int64, 0, len(parsed.Orders))
	for _, o := range parsed.Orders {
		ids = append(ids, o.ID)
	}
	return ids, nil
}
