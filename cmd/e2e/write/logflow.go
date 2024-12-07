package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"LogDb/internal/adapters/data/loader"
	"LogDb/internal/domain"
	"github.com/sirupsen/logrus"
)

var Address = "localhost:8080"
var Endpoint = "http://" + Address + "/api/v1/insert/record"

var log = logrus.New()

// Request payload for the API
type InsertRecordRequest struct {
	Record      domain.LogRecord `json:"record"`
	ShardingKey string           `json:"sharding_key"`
}

// WorkerPool controls the number of concurrent HTTP requests
type WorkerPool struct {
	workerCount      int
	jobs             chan *domain.LogRecord
	wg               sync.WaitGroup
	client           *http.Client
	counter          uint64
	success          uint64
	failures         uint64
	startTime        time.Time
	lastReportTime   time.Time
	expectedRequests uint64
}

// NewWorkerPool creates a new worker pool with the specified number of workers
func NewWorkerPool(workerCount int, expectedRequests uint64) *WorkerPool {
	return &WorkerPool{
		workerCount:      workerCount,
		jobs:             make(chan *domain.LogRecord, workerCount*10),
		client:           &http.Client{Timeout: 10 * time.Second},
		startTime:        time.Now(),
		lastReportTime:   time.Now(),
		expectedRequests: expectedRequests,
	}
}

// Start the worker pool
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
}

// Stop the worker pool and wait for all workers to finish
func (wp *WorkerPool) Stop() {
	close(wp.jobs)
	wp.wg.Wait()
}

// Submit a log record to the worker pool
func (wp *WorkerPool) Submit(record *domain.LogRecord) {
	wp.jobs <- record
}

// worker processes log records and sends HTTP requests
func (wp *WorkerPool) worker() {
	defer wp.wg.Done()
	for record := range wp.jobs {
		if err := sendLogRecord(wp.client, record); err != nil {
			atomic.AddUint64(&wp.failures, 1)
		} else {
			atomic.AddUint64(&wp.success, 1)
		}
		wp.incrementCounter()
	}
}

// incrementCounter updates the counter and logs progress every 1,000 records
func (wp *WorkerPool) incrementCounter() {
	count := atomic.AddUint64(&wp.counter, 1)
	if count%1000 == 0 {
		currentTime := time.Now()
		totalDuration := time.Since(wp.startTime)
		reportDuration := currentTime.Sub(wp.lastReportTime)
		successCount := atomic.LoadUint64(&wp.success)
		failureCount := atomic.LoadUint64(&wp.failures)
		total := successCount + failureCount
		rps := float64(1000) / reportDuration.Seconds()

		log.Infof("[%d/%d] Processed %d log records: Success=%d, Failures=%d, RPS=%.2f, Total Duration=%s, Since Last Report=%s",
			count, wp.expectedRequests, total, successCount, failureCount, rps, totalDuration, reportDuration)

		// Reset counters for the next batch
		atomic.StoreUint64(&wp.success, 0)
		atomic.StoreUint64(&wp.failures, 0)
		wp.lastReportTime = currentTime
	}
}

func main() {
	setupLogger()

	// Configure the worker pool size and total number of requests
	workerCount := 1_000 // Adjust this based on desired parallelism
	//totalRequests := 1_000_000
	totalRequests := 10_000_000

	log.Infof("Starting E2E test with %d workers and %d total requests...", workerCount, totalRequests)

	// Create and start the worker pool
	workerPool := NewWorkerPool(workerCount, uint64(totalRequests))
	workerPool.Start()

	// Generate logs in parallel using multiple goroutines
	logGenConcurrency := 50
	var genWg sync.WaitGroup
	genWg.Add(logGenConcurrency)

	for i := 0; i < logGenConcurrency; i++ {
		go func() {
			defer genWg.Done()
			loader.RandomLogsLoader(func(record *domain.LogRecord) error {
				workerPool.Submit(record)
				return nil
			}, totalRequests/logGenConcurrency)
		}()
	}

	// Wait for log generation to complete
	genWg.Wait()

	// Stop the worker pool and wait for all requests to complete
	workerPool.Stop()

	log.Info("E2E test completed.")
}

// sendLogRecord sends a random log record to the insert API endpoint
func sendLogRecord(client *http.Client, record *domain.LogRecord) error {
	// Prepare the request payload
	payload := InsertRecordRequest{
		Record:      *record,
		ShardingKey: "shard-01",
	}

	// Serialize the payload to JSON
	data, err := json.Marshal(payload)
	if err != nil {
		log.WithError(err).Debug("Failed to marshal log record")
		return fmt.Errorf("failed to marshal log record: %w", err)
	}

	// Create the HTTP POST request
	url := Endpoint
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		log.WithError(err).Debug("Failed to create HTTP request")
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Execute the HTTP request
	resp, err := client.Do(req)
	if err != nil {
		log.WithError(err).Debug("HTTP request failed")
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode != http.StatusOK {
		log.WithField("status_code", resp.StatusCode).Error("Received non-OK response")
		return fmt.Errorf("received non-OK status code: %d", resp.StatusCode)
	}

	log.Debug("Successfully inserted log record")
	return nil
}

// setupLogger configures the Logrus logger
func setupLogger() {
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "DEBUG" {
		log.SetLevel(logrus.DebugLevel)
	} else {
		log.SetLevel(logrus.InfoLevel)
	}
	log.Info("Logger initialized")
}
