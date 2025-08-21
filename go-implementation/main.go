package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Task represents a unit of work to be processed
type Task struct {
	ID        int       `json:"id"`
	Data      string    `json:"data"`
	Priority  int       `json:"priority"`
	Timestamp time.Time `json:"timestamp"`
	Retries   int       `json:"retries"`
}

// NewTask creates a new task with the given parameters
func NewTask(id int, data string, priority int) *Task {
	return &Task{
		ID:        id,
		Data:      data,
		Priority:  priority,
		Timestamp: time.Now(),
		Retries:   0,
	}
}

func (t *Task) String() string {
	return fmt.Sprintf("Task{ID: %d, Data: %s, Priority: %d, Retries: %d, Timestamp: %s}",
		t.ID, t.Data, t.Priority, t.Retries, t.Timestamp.Format("15:04:05.000"))
}

// ProcessingResult represents the outcome of processing a task
type ProcessingResult struct {
	TaskID         int           `json:"task_id"`
	OriginalData   string        `json:"original_data"`
	ProcessedData  string        `json:"processed_data"`
	ProcessingTime time.Duration `json:"processing_time"`
	WorkerName     string        `json:"worker_name"`
	Success        bool          `json:"success"`
	ErrorMessage   string        `json:"error_message,omitempty"`
	Timestamp      time.Time     `json:"timestamp"`
	Attempt        int           `json:"attempt"`
}

// NewSuccessResult creates a successful processing result
func NewSuccessResult(taskID int, originalData, processedData, workerName string, processingTime time.Duration, attempt int) *ProcessingResult {
	return &ProcessingResult{
		TaskID:         taskID,
		OriginalData:   originalData,
		ProcessedData:  processedData,
		ProcessingTime: processingTime,
		WorkerName:     workerName,
		Success:        true,
		Timestamp:      time.Now(),
		Attempt:        attempt,
	}
}

// NewErrorResult creates a failed processing result
func NewErrorResult(taskID int, originalData, workerName, errorMessage string, processingTime time.Duration, attempt int) *ProcessingResult {
	return &ProcessingResult{
		TaskID:         taskID,
		OriginalData:   originalData,
		ProcessingTime: processingTime,
		WorkerName:     workerName,
		Success:        false,
		ErrorMessage:   errorMessage,
		Timestamp:      time.Now(),
		Attempt:        attempt,
	}
}

func (r *ProcessingResult) String() string {
	if r.Success {
		return fmt.Sprintf("Result{TaskID: %d, Worker: %s, Attempt: %d, Time: %v, Data: %s}",
			r.TaskID, r.WorkerName, r.Attempt, r.ProcessingTime, r.ProcessedData)
	}
	return fmt.Sprintf("Result{TaskID: %d, Worker: %s, Attempt: %d, ERROR: %s}",
		r.TaskID, r.WorkerName, r.Attempt, r.ErrorMessage)
}

// EnhancedSharedResourceQueue manages tasks using channels with advanced features
type EnhancedSharedResourceQueue struct {
	taskChan     chan *Task
	retryQueue   chan *Task
	totalTasks   int64
	addedTasks   int64
	retriedTasks int64
	shutdown     int32
	shutdownOnce sync.Once
	mu           sync.RWMutex
	logger       *log.Logger
	maxRetries   int
}

// NewEnhancedSharedResourceQueue creates a new enhanced shared resource queue
func NewEnhancedSharedResourceQueue(bufferSize int, maxRetries int, logger *log.Logger) *EnhancedSharedResourceQueue {
	return &EnhancedSharedResourceQueue{
		taskChan:   make(chan *Task, bufferSize),
		retryQueue: make(chan *Task, bufferSize/2), // Smaller retry queue
		logger:     logger,
		maxRetries: maxRetries,
	}
}

// AddTask adds a task to the queue
func (q *EnhancedSharedResourceQueue) AddTask(task *Task) error {
	if atomic.LoadInt32(&q.shutdown) == 1 {
		return fmt.Errorf("queue is shutdown")
	}

	select {
	case q.taskChan <- task:
		atomic.AddInt64(&q.addedTasks, 1)
		q.logger.Printf("Task added to queue: %s", task)
		return nil
	default:
		return fmt.Errorf("queue is full")
	}
}

// RetryTask adds a task to the retry queue
func (q *EnhancedSharedResourceQueue) RetryTask(task *Task) error {
	if atomic.LoadInt32(&q.shutdown) == 1 {
		return fmt.Errorf("queue is shutdown")
	}

	if task.Retries >= q.maxRetries {
		return fmt.Errorf("task %d exceeded max retries (%d)", task.ID, q.maxRetries)
	}

	task.Retries++

	select {
	case q.retryQueue <- task:
		atomic.AddInt64(&q.retriedTasks, 1)
		q.logger.Printf("Task added to retry queue (attempt %d): %s", task.Retries+1, task)
		return nil
	default:
		q.logger.Printf("Retry queue full, dropping task %d", task.ID)
		return fmt.Errorf("retry queue is full")
	}
}

// GetTask retrieves a task from the queue with context for cancellation
// Prioritizes retry queue over main task queue
func (q *EnhancedSharedResourceQueue) GetTask(ctx context.Context) (*Task, error) {
	for {
		select {
		case task := <-q.retryQueue:
			q.logger.Printf("Task retrieved from retry queue: %s", task)
			return task, nil
		case task := <-q.taskChan:
			if task != nil {
				q.logger.Printf("Task retrieved from main queue: %s", task)
			}
			return task, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// Check if we should continue waiting
			if atomic.LoadInt32(&q.shutdown) == 1 && len(q.taskChan) == 0 && len(q.retryQueue) == 0 {
				return nil, nil
			}
			// Brief pause to prevent busy waiting
			select {
			case <-time.After(10 * time.Millisecond):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
}

// Shutdown gracefully shuts down the queue
func (q *EnhancedSharedResourceQueue) Shutdown() {
	q.shutdownOnce.Do(func() {
		atomic.StoreInt32(&q.shutdown, 1)
		close(q.taskChan)
		close(q.retryQueue)
		q.logger.Println("EnhancedSharedResourceQueue shutdown initiated")
	})
}

// GetStats returns queue statistics
func (q *EnhancedSharedResourceQueue) GetStats() (int64, int, int, bool) {
	return atomic.LoadInt64(&q.addedTasks), len(q.taskChan), len(q.retryQueue), atomic.LoadInt32(&q.shutdown) == 1
}

// GetRetryStats returns retry statistics
func (q *EnhancedSharedResourceQueue) GetRetryStats() int64 {
	return atomic.LoadInt64(&q.retriedTasks)
}

// EnhancedResultsManager manages processing results with enhanced features
type EnhancedResultsManager struct {
	results     []*ProcessingResult
	outputFile  *os.File
	mu          sync.RWMutex
	logger      *log.Logger
	successRate float64
	totalTime   time.Duration
	startTime   time.Time
}

// NewEnhancedResultsManager creates a new enhanced results manager
func NewEnhancedResultsManager(outputFileName string, logger *log.Logger) (*EnhancedResultsManager, error) {
	rm := &EnhancedResultsManager{
		results:   make([]*ProcessingResult, 0),
		logger:    logger,
		startTime: time.Now(),
	}

	if outputFileName != "" {
		file, err := os.OpenFile(outputFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open output file: %w", err)
		}
		rm.outputFile = file
		logger.Printf("EnhancedResultsManager initialized with output file: %s", outputFileName)
	} else {
		logger.Println("EnhancedResultsManager initialized (memory only)")
	}

	return rm, nil
}

// AddResult adds a processing result with enhanced tracking
func (rm *EnhancedResultsManager) AddResult(result *ProcessingResult) error {
	if result == nil {
		return fmt.Errorf("result cannot be nil")
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.results = append(rm.results, result)
	rm.updateStats()
	rm.logger.Printf("Result added: %s", result)

	// Write to file if configured
	if rm.outputFile != nil {
		resultJSON, err := json.Marshal(result)
		if err != nil {
			rm.logger.Printf("Error marshaling result to JSON: %v", err)
		} else {
			_, err := fmt.Fprintf(rm.outputFile, "[%s] %s | JSON: %s\n",
				result.Timestamp.Format("2006-01-02 15:04:05.000"), result.String(), string(resultJSON))
			if err != nil {
				rm.logger.Printf("Error writing result to file: %v", err)
				return err
			}
		}
	}

	return nil
}

// updateStats updates internal statistics (must be called with lock held)
func (rm *EnhancedResultsManager) updateStats() {
	if len(rm.results) == 0 {
		rm.successRate = 0
		rm.totalTime = 0
		return
	}

	var successCount int
	var totalProcessingTime time.Duration

	for _, result := range rm.results {
		if result.Success {
			successCount++
			totalProcessingTime += result.ProcessingTime
		}
	}

	rm.successRate = float64(successCount) / float64(len(rm.results)) * 100
	rm.totalTime = totalProcessingTime
}

// GetAllResults returns a copy of all results
func (rm *EnhancedResultsManager) GetAllResults() []*ProcessingResult {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	results := make([]*ProcessingResult, len(rm.results))
	copy(results, rm.results)
	return results
}

// GenerateEnhancedSummaryReport creates an enhanced summary of all processing results
func (rm *EnhancedResultsManager) GenerateEnhancedSummaryReport() string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	results := rm.results
	var successCount, failureCount int
	var totalProcessingTime time.Duration
	var minTime, maxTime time.Duration = time.Hour, 0
	retryCount := make(map[int]int)

	for _, result := range results {
		if result.Success {
			successCount++
			totalProcessingTime += result.ProcessingTime
			if result.ProcessingTime < minTime {
				minTime = result.ProcessingTime
			}
			if result.ProcessingTime > maxTime {
				maxTime = result.ProcessingTime
			}
		} else {
			failureCount++
		}
		retryCount[result.Attempt]++
	}

	total := len(results)
	var avgProcessingTime time.Duration
	if successCount > 0 {
		avgProcessingTime = totalProcessingTime / time.Duration(successCount)
	}

	var successRate float64
	if total > 0 {
		successRate = float64(successCount) / float64(total) * 100
	}

	throughput := float64(total) / time.Since(rm.startTime).Seconds()

	report := fmt.Sprintf(`=== ENHANCED PROCESSING SUMMARY REPORT ===
Total Results: %d
Successful: %d
Failed: %d
Success Rate: %.2f%%
Average Processing Time: %v
Min Processing Time: %v
Max Processing Time: %v
Total Execution Time: %v
Throughput: %.2f tasks/second

Retry Statistics:
`, total, successCount, failureCount, successRate, avgProcessingTime, minTime, maxTime, time.Since(rm.startTime), throughput)

	for attempt, count := range retryCount {
		report += fmt.Sprintf("  Attempt %d: %d tasks\n", attempt, count)
	}

	return report
}

// Close closes the results manager and any open files
func (rm *EnhancedResultsManager) Close() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.outputFile != nil {
		err := rm.outputFile.Close()
		rm.outputFile = nil
		return err
	}
	return nil
}

// EnhancedWorkerPool manages a pool of enhanced worker goroutines
type EnhancedWorkerPool struct {
	workerCount       int
	taskQueue         *EnhancedSharedResourceQueue
	resultsManager    *EnhancedResultsManager
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	logger            *log.Logger
	minProcessingTime time.Duration
	maxProcessingTime time.Duration
	errorRate         float64
	maxRetries        int
	stats             *WorkerStats
}

// WorkerStats tracks worker pool statistics
type WorkerStats struct {
	TasksProcessed   int64
	TasksRetried     int64
	TasksFailed      int64
	TotalProcessTime int64
	WorkersActive    int32
}

// NewEnhancedWorkerPool creates a new enhanced worker pool
func NewEnhancedWorkerPool(workerCount int, taskQueue *EnhancedSharedResourceQueue, resultsManager *EnhancedResultsManager, logger *log.Logger) *EnhancedWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &EnhancedWorkerPool{
		workerCount:       workerCount,
		taskQueue:         taskQueue,
		resultsManager:    resultsManager,
		ctx:               ctx,
		cancel:            cancel,
		logger:            logger,
		minProcessingTime: 50 * time.Millisecond,
		maxProcessingTime: 500 * time.Millisecond,
		errorRate:         0.05,
		maxRetries:        3,
		stats:             &WorkerStats{},
	}
}

// Start starts all enhanced worker goroutines
func (wp *EnhancedWorkerPool) Start() {
	wp.logger.Printf("Starting %d enhanced worker goroutines", wp.workerCount)

	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.enhancedWorker(fmt.Sprintf("EnhancedWorker-%d", i+1))
	}

	wp.logger.Println("All enhanced workers started")
}

// Stop gracefully stops all enhanced workers
func (wp *EnhancedWorkerPool) Stop() {
	wp.logger.Println("Stopping enhanced worker pool")
	wp.cancel()
	wp.wg.Wait()
	wp.logger.Println("All enhanced workers stopped")
}

// enhancedWorker is the main enhanced worker goroutine function
func (wp *EnhancedWorkerPool) enhancedWorker(workerName string) {
	defer wp.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			wp.logger.Printf("Enhanced worker %s recovered from panic: %v", workerName, r)
		}
		atomic.AddInt32(&wp.stats.WorkersActive, -1)
	}()

	atomic.AddInt32(&wp.stats.WorkersActive, 1)
	wp.logger.Printf("Enhanced worker %s started", workerName)

	tasksProcessed := 0
	errorsEncountered := 0
	consecutiveErrors := 0
	maxConsecutiveErrors := 5

	for {
		select {
		case <-wp.ctx.Done():
			wp.logger.Printf("Enhanced worker %s received stop signal", workerName)
			wp.logger.Printf("Enhanced worker %s finished. Tasks processed: %d, Errors: %d",
				workerName, tasksProcessed, errorsEncountered)
			return

		default:
			// Try to get a task with timeout
			taskCtx, taskCancel := context.WithTimeout(wp.ctx, 2*time.Second)
			task, err := wp.taskQueue.GetTask(taskCtx)
			taskCancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					// Check if we should continue waiting
					_, pending, retryPending, shutdown := wp.taskQueue.GetStats()
					if shutdown && pending == 0 && retryPending == 0 {
						wp.logger.Printf("Enhanced worker %s: No more tasks available, exiting", workerName)
						break
					}
					continue
				}
				wp.logger.Printf("Enhanced worker %s: Error getting task: %v", workerName, err)
				continue
			}

			if task == nil {
				// Channel was closed
				wp.logger.Printf("Enhanced worker %s: Task channel closed, exiting", workerName)
				break
			}

			// Process the task with enhanced error handling and retry logic
			result := wp.processTaskWithRetry(task, workerName)

			// Add result to results manager
			if err := wp.resultsManager.AddResult(result); err != nil {
				wp.logger.Printf("Enhanced worker %s: Error adding result: %v", workerName, err)
			}

			tasksProcessed++
			atomic.AddInt64(&wp.stats.TasksProcessed, 1)

			if result.Success {
				consecutiveErrors = 0
			} else {
				errorsEncountered++
				consecutiveErrors++
				atomic.AddInt64(&wp.stats.TasksFailed, 1)

				// Try to retry the task if it hasn't exceeded max retries
				if task.Retries < wp.maxRetries {
					if err := wp.taskQueue.RetryTask(task); err != nil {
						wp.logger.Printf("Enhanced worker %s: Failed to retry task %d: %v", workerName, task.ID, err)
					} else {
						atomic.AddInt64(&wp.stats.TasksRetried, 1)
					}
				}

				if consecutiveErrors >= maxConsecutiveErrors {
					wp.logger.Printf("Enhanced worker %s: Too many consecutive errors (%d), stopping",
						workerName, consecutiveErrors)
					break
				}
			}

			// Brief pause between tasks to prevent overwhelming the system
			select {
			case <-time.After(time.Duration(rand.Intn(10)) * time.Millisecond):
			case <-wp.ctx.Done():
				return
			}
		}
	}

	wp.logger.Printf("Enhanced worker %s finished. Tasks processed: %d, Errors: %d",
		workerName, tasksProcessed, errorsEncountered)
}

// processTaskWithRetry processes a task with enhanced error handling
func (wp *EnhancedWorkerPool) processTaskWithRetry(task *Task, workerName string) *ProcessingResult {
	startTime := time.Now()
	attempt := task.Retries + 1

	wp.logger.Printf("Enhanced worker %s processing task (attempt %d): %s", workerName, attempt, task)

	// Simulate random processing errors
	if rand.Float64() < wp.errorRate {
		errorTypes := []string{
			"Network timeout error",
			"Invalid data format",
			"Resource temporarily unavailable",
			"Authentication failed",
			"Rate limit exceeded",
		}
		errorMsg := errorTypes[rand.Intn(len(errorTypes))]
		wp.logger.Printf("Enhanced worker %s: %s for task %d (attempt %d)", workerName, errorMsg, task.ID, attempt)
		return NewErrorResult(task.ID, task.Data, workerName, errorMsg, time.Since(startTime), attempt)
	}

	// Simulate processing time with some variance
	processingTime := wp.minProcessingTime +
		time.Duration(rand.Float64()*float64(wp.maxProcessingTime-wp.minProcessingTime))

	select {
	case <-time.After(processingTime):
		// Normal completion
	case <-wp.ctx.Done():
		// Context cancelled during processing
		errorMsg := "Task processing cancelled due to context cancellation"
		wp.logger.Printf("Enhanced worker %s: %s for task %d (attempt %d)", workerName, errorMsg, task.ID, attempt)
		return NewErrorResult(task.ID, task.Data, workerName, errorMsg, time.Since(startTime), attempt)
	}

	// Process the data with enhanced transformation
	processedData := wp.enhancedProcessData(task.Data, workerName, attempt)
	totalTime := time.Since(startTime)

	atomic.AddInt64(&wp.stats.TotalProcessTime, int64(totalTime))

	wp.logger.Printf("Enhanced worker %s completed task %d (attempt %d) in %v", workerName, task.ID, attempt, totalTime)

	return NewSuccessResult(task.ID, task.Data, processedData, workerName, totalTime, attempt)
}

// enhancedProcessData simulates enhanced data processing transformation
func (wp *EnhancedWorkerPool) enhancedProcessData(data, workerName string, attempt int) string {
	if data == "" {
		return "[EMPTY_DATA]"
	}

	// Enhanced data transformation with metadata
	processed := fmt.Sprintf("ENHANCED_PROCESSED_%s_HASH_%d_WORKER_%s_ATTEMPT_%d_TIMESTAMP_%d",
		strings.ToUpper(strings.ReplaceAll(data, " ", "_")),
		len(data)*7919+attempt*13, // Enhanced hash with attempt factor
		workerName,
		attempt,
		time.Now().Unix())

	return processed
}

// GetStats returns current worker pool statistics
func (wp *EnhancedWorkerPool) GetStats() *WorkerStats {
	return &WorkerStats{
		TasksProcessed:   atomic.LoadInt64(&wp.stats.TasksProcessed),
		TasksRetried:     atomic.LoadInt64(&wp.stats.TasksRetried),
		TasksFailed:      atomic.LoadInt64(&wp.stats.TasksFailed),
		TotalProcessTime: atomic.LoadInt64(&wp.stats.TotalProcessTime),
		WorkersActive:    atomic.LoadInt32(&wp.stats.WorkersActive),
	}
}

// EnhancedDataProcessingSystem orchestrates the entire enhanced data processing workflow
type EnhancedDataProcessingSystem struct {
	taskQueue      *EnhancedSharedResourceQueue
	resultsManager *EnhancedResultsManager
	workerPool     *EnhancedWorkerPool
	logger         *log.Logger
	config         *EnhancedConfig
}

// EnhancedConfig holds enhanced system configuration
type EnhancedConfig struct {
	WorkerCount     int
	QueueSize       int
	TaskCount       int
	OutputFile      string
	LogFile         string
	ShutdownTimeout time.Duration
	MaxRetries      int
	EnableMetrics   bool
	MetricsInterval time.Duration
}

// DefaultEnhancedConfig returns a default enhanced configuration
func DefaultEnhancedConfig() *EnhancedConfig {
	return &EnhancedConfig{
		WorkerCount:     runtime.NumCPU(),
		QueueSize:       200,
		TaskCount:       50,
		OutputFile:      "enhanced_processing_results.txt",
		LogFile:         "",
		ShutdownTimeout: 30 * time.Second,
		MaxRetries:      3,
		EnableMetrics:   true,
		MetricsInterval: 5 * time.Second,
	}
}

// NewEnhancedDataProcessingSystem creates a new enhanced data processing system
func NewEnhancedDataProcessingSystem(config *EnhancedConfig) (*EnhancedDataProcessingSystem, error) {
	// Setup enhanced logging
	var logger *log.Logger
	if config.LogFile != "" {
		logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}
		logger = log.New(logFile, "", log.LstdFlags|log.Lshortfile)
	} else {
		logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
	}

	// Create enhanced components
	taskQueue := NewEnhancedSharedResourceQueue(config.QueueSize, config.MaxRetries, logger)

	resultsManager, err := NewEnhancedResultsManager(config.OutputFile, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create enhanced results manager: %w", err)
	}

	workerPool := NewEnhancedWorkerPool(config.WorkerCount, taskQueue, resultsManager, logger)

	system := &EnhancedDataProcessingSystem{
		taskQueue:      taskQueue,
		resultsManager: resultsManager,
		workerPool:     workerPool,
		logger:         logger,
		config:         config,
	}

	logger.Printf("EnhancedDataProcessingSystem initialized with config: %+v", config)
	return system, nil
}

// AddTasks adds the specified number of enhanced tasks to the queue
func (edps *EnhancedDataProcessingSystem) AddTasks(numberOfTasks int) error {
	edps.logger.Printf("Adding %d enhanced tasks to the queue", numberOfTasks)

	taskTemplates := []string{
		"Process customer financial data",
		"Analyze real-time sales metrics",
		"Transform user behavioral input",
		"Compute advanced statistical models",
		"Calculate comprehensive financial reports",
		"Validate critical data integrity",
		"Encrypt highly sensitive information",
		"Compress large file archives",
		"Parse complex configuration files",
		"Aggregate distributed system logs",
		"Generate detailed business intelligence reports",
		"Optimize complex database queries",
		"Process machine learning datasets",
		"Analyze network traffic patterns",
		"Transform multimedia content",
	}

	for i := 1; i <= numberOfTasks; i++ {
		template := taskTemplates[(i-1)%len(taskTemplates)]
		data := fmt.Sprintf("%s (Customer: %d, Batch: %d, Priority: %d)",
			template, 1000+i, (i-1)/20+1, i%5)
		priority := i % 5 // Vary priorities (0-4)

		task := NewTask(i, data, priority)

		if err := edps.taskQueue.AddTask(task); err != nil {
			edps.logger.Printf("Failed to add enhanced task %d: %v", i, err)
			return fmt.Errorf("failed to add enhanced task %d: %w", i, err)
		}
	}

	edps.logger.Printf("Finished adding %d enhanced tasks to queue", numberOfTasks)
	return nil
}

// Run executes the complete enhanced data processing workflow
func (edps *EnhancedDataProcessingSystem) Run() error {
	defer func() {
		if err := edps.resultsManager.Close(); err != nil {
			edps.logger.Printf("Error closing enhanced results manager: %v", err)
		}
	}()

	edps.logger.Println("Starting enhanced data processing workflow")

	// Start metrics collection if enabled
	if edps.config.EnableMetrics {
		go edps.collectMetrics()
	}

	// Start enhanced workers
	edps.workerPool.Start()

	// Add enhanced tasks
	if err := edps.AddTasks(edps.config.TaskCount); err != nil {
		return fmt.Errorf("failed to add enhanced tasks: %w", err)
	}

	// Wait for all enhanced tasks to be processed
	edps.waitForEnhancedCompletion()

	// Shutdown the enhanced task queue
	edps.taskQueue.Shutdown()

	// Stop enhanced workers with timeout
	done := make(chan struct{})
	go func() {
		edps.workerPool.Stop()
		close(done)
	}()

	select {
	case <-done:
		edps.logger.Println("Enhanced workers stopped gracefully")
	case <-time.After(edps.config.ShutdownTimeout):
		edps.logger.Println("Timeout reached during enhanced worker shutdown")
	}

	// Print final enhanced statistics
	edps.printEnhancedStatistics()

	edps.logger.Println("Enhanced data processing workflow completed")
	return nil
}

// collectMetrics periodically collects and logs system metrics
func (edps *EnhancedDataProcessingSystem) collectMetrics() {
	ticker := time.NewTicker(edps.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stats := edps.workerPool.GetStats()
			added, pending, retryPending, shutdown := edps.taskQueue.GetStats()
			retried := edps.taskQueue.GetRetryStats()

			edps.logger.Printf("METRICS: Added=%d, Pending=%d, Retry=%d, Processed=%d, Failed=%d, Retried=%d, Active=%d, Shutdown=%v",
				added, pending, retryPending, stats.TasksProcessed, stats.TasksFailed, retried, stats.WorkersActive, shutdown)

		case <-edps.workerPool.ctx.Done():
			return
		}
	}
}

// waitForEnhancedCompletion waits for all enhanced tasks to be processed
func (edps *EnhancedDataProcessingSystem) waitForEnhancedCompletion() {
	edps.logger.Println("Waiting for all enhanced tasks to be completed")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		added, pending, retryPending, shutdown := edps.taskQueue.GetStats()
		results := len(edps.resultsManager.GetAllResults())

		edps.logger.Printf("Enhanced progress: %d tasks added, %d pending, %d in retry, %d results, shutdown: %v",
			added, pending, retryPending, results, shutdown)

		if pending == 0 && retryPending == 0 && int64(results) >= added {
			edps.logger.Println("All enhanced tasks have been completed")
			break
		}

		<-ticker.C
	}
}

// printEnhancedStatistics prints enhanced system statistics
func (edps *EnhancedDataProcessingSystem) printEnhancedStatistics() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("ENHANCED SYSTEM STATISTICS")
	fmt.Println(strings.Repeat("=", 80))

	// Enhanced queue statistics
	added, pending, retryPending, shutdown := edps.taskQueue.GetStats()
	retried := edps.taskQueue.GetRetryStats()
	fmt.Printf("Enhanced Queue Status: Added: %d, Pending: %d, Retry: %d, Total Retried: %d, Shutdown: %v\n",
		added, pending, retryPending, retried, shutdown)

	// Enhanced worker statistics
	stats := edps.workerPool.GetStats()
	fmt.Printf("Enhanced Worker Stats: Processed: %d, Failed: %d, Retried: %d, Active: %d\n",
		stats.TasksProcessed, stats.TasksFailed, stats.TasksRetried, stats.WorkersActive)

	// Enhanced results statistics
	results := edps.resultsManager.GetAllResults()
	fmt.Printf("Enhanced Results: Total: %d\n", len(results))

	// Generate and print enhanced summary report
	fmt.Println(edps.resultsManager.GenerateEnhancedSummaryReport())

	fmt.Println(strings.Repeat("=", 80))
}

// setupEnhancedGracefulShutdown sets up enhanced signal handling for graceful shutdown
func setupEnhancedGracefulShutdown(system *EnhancedDataProcessingSystem) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		system.logger.Println("Received enhanced shutdown signal")
		system.taskQueue.Shutdown()
		system.workerPool.Stop()
		os.Exit(0)
	}()
}

func main() {
	fmt.Println("Enhanced Multi-threaded Data Processing System - Go Implementation")
	fmt.Println("Features: Advanced Error Handling, Retry Logic, Comprehensive Metrics")
	fmt.Printf("Starting at: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	// Parse command line arguments
	config := DefaultEnhancedConfig()

	if len(os.Args) > 1 {
		if workers, err := strconv.Atoi(os.Args[1]); err == nil {
			config.WorkerCount = workers
		}
	}
	if len(os.Args) > 2 {
		if tasks, err := strconv.Atoi(os.Args[2]); err == nil {
			config.TaskCount = tasks
		}
	}
	if len(os.Args) > 3 {
		config.OutputFile = os.Args[3]
	}

	// Set enhanced log file with timestamp
	config.LogFile = fmt.Sprintf("enhanced_data_processing_%s.log",
		time.Now().Format("20060102_150405"))

	fmt.Printf("Enhanced Configuration: %d workers, %d tasks, %d max retries\n",
		config.WorkerCount, config.TaskCount, config.MaxRetries)

	// Create and run the enhanced system
	system, err := NewEnhancedDataProcessingSystem(config)
	if err != nil {
		log.Fatalf("Failed to create enhanced data processing system: %v", err)
	}

	// Setup enhanced graceful shutdown
	setupEnhancedGracefulShutdown(system)

	// Run the enhanced system
	if err := system.Run(); err != nil {
		log.Fatalf("Error running enhanced data processing system: %v", err)
	}

	fmt.Printf("Enhanced system completed at: %s\n", time.Now().Format("2006-01-02 15:04:05"))
}
