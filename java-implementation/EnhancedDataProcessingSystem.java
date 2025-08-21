import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.ArrayList;

/**
 * Enhanced data processing system using Executor framework for better thread management
 * Includes circuit breaker pattern, retry logic, and comprehensive monitoring
 */
public class EnhancedDataProcessingSystem {
    private static final Logger LOGGER = Logger.getLogger(EnhancedDataProcessingSystem.class.getName());
    
    private final ExecutorService executorService;
    private final CompletionService<ProcessingResult> completionService;
    private final SharedResourceQueue taskQueue;
    private final ResultsManager resultsManager;
    private final CircuitBreaker circuitBreaker;
    private final int numberOfWorkers;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    private final AtomicInteger retriedTasks = new AtomicInteger(0);

    public EnhancedDataProcessingSystem(int numberOfWorkers, boolean usePriorityQueue, String outputFile) {
        this.numberOfWorkers = numberOfWorkers;
        this.taskQueue = new SharedResourceQueue(usePriorityQueue);
        this.resultsManager = new ResultsManager(outputFile);
        
        // Initialize circuit breaker
        this.circuitBreaker = new CircuitBreaker("TaskProcessing", 5, 10000, 3);
        
        // Create thread pool with custom thread factory
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "EnhancedWorker-" + threadNumber.getAndIncrement());
                t.setDaemon(false);
                t.setUncaughtExceptionHandler((thread, ex) -> {
                    LOGGER.log(Level.SEVERE, "Uncaught exception in thread " + thread.getName(), ex);
                });
                return t;
            }
        };
        
        // Use a cached thread pool that can grow as needed
        this.executorService = new ThreadPoolExecutor(
            numberOfWorkers, // core pool size
            numberOfWorkers * 2, // maximum pool size
            60L, TimeUnit.SECONDS, // keep alive time
            new LinkedBlockingQueue<>(), // work queue
            threadFactory,
            new ThreadPoolExecutor.CallerRunsPolicy() // rejection policy
        );
        
        this.completionService = new ExecutorCompletionService<>(executorService);
        
        setupLogging();
        LOGGER.info(String.format("EnhancedDataProcessingSystem initialized with %d workers using Executor framework", 
                                  numberOfWorkers));
    }

    /**
     * Sets up enhanced logging configuration
     */
    private void setupLogging() {
        try {
            String logFileName = "enhanced_processing_" + 
                               LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + 
                               ".log";
            FileHandler fileHandler = new FileHandler(logFileName, true);
            fileHandler.setFormatter(new SimpleFormatter());
            
            Logger rootLogger = Logger.getLogger("");
            rootLogger.setLevel(Level.INFO);
            rootLogger.addHandler(fileHandler);
            
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.INFO);
            rootLogger.addHandler(consoleHandler);
            
            LOGGER.info("Enhanced logging configured. Log file: " + logFileName);
        } catch (Exception e) {
            System.err.println("Failed to setup enhanced file logging: " + e.getMessage());
        }
    }

    /**
     * Enhanced worker task using Callable interface with circuit breaker and retry logic
     */
    private class EnhancedWorkerTask implements Callable<ProcessingResult> {
        private final String workerName;
        private final long minProcessingTime;
        private final long maxProcessingTime;
        private final double errorRate;
        private final int maxRetries;

        public EnhancedWorkerTask(String workerName, long minProcessingTime, 
                                 long maxProcessingTime, double errorRate, int maxRetries) {
            this.workerName = workerName;
            this.minProcessingTime = minProcessingTime;
            this.maxProcessingTime = maxProcessingTime;
            this.errorRate = errorRate;
            this.maxRetries = maxRetries;
        }

        @Override
        public ProcessingResult call() throws Exception {
            try {
                // Get task from queue with timeout
                Task task = null;
                try {
                    task = taskQueue.getTask();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while getting task", e);
                }
                
                if (task == null) {
                    return null; // No more tasks
                }

                activeTasks.incrementAndGet();
                LOGGER.fine(String.format("%s processing task: %s", workerName, task));

                // Process task through circuit breaker with retry logic
                ProcessingResult result = processTaskWithCircuitBreaker(task);
                
                // Mark task as completed
                taskQueue.markTaskCompleted();
                
                return result;

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error in enhanced worker task: " + workerName, e);
                throw e;
            } finally {
                activeTasks.decrementAndGet();
            }
        }

        private ProcessingResult processTaskWithCircuitBreaker(Task task) {
            return processTaskWithRetry(task, maxRetries);
        }

        private ProcessingResult processTaskWithRetry(Task task, int maxRetries) {
            Exception lastException = null;
            
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    // Execute through circuit breaker
                    return circuitBreaker.execute(() -> processTask(task));
                    
                } catch (CircuitBreaker.CircuitBreakerOpenException e) {
                    // Circuit breaker is open, don't retry immediately
                    String errorMsg = String.format("Circuit breaker open for task %d", task.getId());
                    LOGGER.warning(String.format("%s: %s", workerName, errorMsg));
                    return new ProcessingResult(task.getId(), task.getData(), null,
                                              0, workerName, false, errorMsg);
                    
                } catch (Exception e) {
                    lastException = e;
                    LOGGER.warning(String.format("%s: Attempt %d/%d failed for task %d: %s", 
                                                workerName, attempt, maxRetries, task.getId(), e.getMessage()));
                    
                    if (attempt < maxRetries) {
                        retriedTasks.incrementAndGet();
                        try {
                            // Exponential backoff with jitter
                            long backoffTime = (long) (Math.pow(2, attempt - 1) * 100 + Math.random() * 100);
                            Thread.sleep(backoffTime);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
            
            // All retries failed
            String errorMsg = String.format("All %d attempts failed. Last error: %s", 
                                          maxRetries, lastException != null ? lastException.getMessage() : "Unknown error");
            return new ProcessingResult(task.getId(), task.getData(), null,
                                      0, workerName, false, errorMsg);
        }

        private ProcessingResult processTask(Task task) throws Exception {
            long startTime = System.currentTimeMillis();
            
            // Simulate random errors based on error rate
            if (Math.random() < errorRate) {
                throw new RuntimeException("Simulated processing error for task " + task.getId());
            }
            
            // Simulate processing time
            long processingTime = minProcessingTime + 
                                (long)(Math.random() * (maxProcessingTime - minProcessingTime));
            
            // Check for interruption during processing
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Processing interrupted");
            }
            
            Thread.sleep(processingTime);
            
            // Process data with enhanced transformation
            String processedData = enhancedProcessData(task.getData(), workerName);
            
            long totalTime = System.currentTimeMillis() - startTime;
            totalProcessingTime.addAndGet(totalTime);
            
            LOGGER.fine(String.format("%s completed task %d in %d ms", 
                                     workerName, task.getId(), totalTime));
            
            return new ProcessingResult(task.getId(), task.getData(), processedData,
                                      totalTime, workerName);
        }

        private String enhancedProcessData(String data, String workerName) {
            if (data == null || data.trim().isEmpty()) {
                return "[EMPTY_DATA]";
            }
            
            // Enhanced data transformation
            StringBuilder processed = new StringBuilder();
            
            // Clean and transform
            String cleaned = data.toUpperCase()
                               .replaceAll("\\s+", "_")
                               .replaceAll("[^A-Z0-9_]", "");
            processed.append("PROCESSED_").append(cleaned);
            
            // Add metadata
            processed.append("_HASH_").append(Math.abs(data.hashCode()));
            processed.append("_LENGTH_").append(data.length());
            processed.append("_WORKER_").append(workerName);
            processed.append("_TIMESTAMP_").append(System.currentTimeMillis());
            
            return processed.toString();
        }
    }

    /**
     * Runs the enhanced processing workflow
     */
    public void runProcessing(int numberOfTasks) {
        try {
            LOGGER.info("Starting enhanced data processing workflow with Executor framework");
            
            // Add tasks to queue
            addTasks(numberOfTasks);
            
            // Submit worker tasks to executor
            submitWorkerTasks(numberOfTasks);
            
            // Collect results with timeout
            collectResults(numberOfTasks);
            
            // Shutdown executor
            shutdownExecutor();
            
            // Print comprehensive statistics
            printEnhancedStatistics();
            
            // Generate detailed report
            generateDetailedReport();
            
            LOGGER.info("Enhanced data processing workflow completed successfully");
            
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in enhanced processing workflow", e);
        }
    }

    private void addTasks(int numberOfTasks) {
        LOGGER.info("Adding " + numberOfTasks + " tasks to enhanced queue");
        
        String[] taskPrefixes = {
            "Process customer data", "Analyze sales metrics", "Transform user input",
            "Compute statistical models", "Calculate financial reports", "Validate data integrity",
            "Encrypt sensitive information", "Compress file archives", "Parse configuration files",
            "Aggregate system logs", "Generate business reports", "Optimize database queries"
        };
        
        for (int i = 1; i <= numberOfTasks; i++) {
            String prefix = taskPrefixes[(i-1) % taskPrefixes.length];
            String data = String.format("%s for customer %d (Priority: %d, Batch: %d)", 
                                       prefix, i, i % 3, (i-1) / 10 + 1);
            Task task = new Task(i, data, i % 3);
            
            if (!taskQueue.addTask(task)) {
                LOGGER.warning("Failed to add task " + i + " to queue");
            }
        }
        
        taskQueue.shutdown(); // Signal no more tasks will be added
        LOGGER.info("Finished adding " + numberOfTasks + " tasks to enhanced queue");
    }

    private void submitWorkerTasks(int numberOfTasks) {
        LOGGER.info("Submitting enhanced worker tasks to executor");
        
        // Calculate optimal number of worker task submissions
        int tasksPerWorker = Math.max(1, numberOfTasks / numberOfWorkers);
        int totalSubmissions = numberOfTasks + (numberOfWorkers * 2); // Extra buffer
        
        for (int i = 0; i < numberOfWorkers; i++) {
            String workerName = "EnhancedWorker-" + (i + 1);
            long minTime = 50 + (i * 20);
            long maxTime = 300 + (i * 50);
            double errorRate = 0.02 + (i * 0.005); // Gradually increase error rates
            int maxRetries = 3;
            
            EnhancedWorkerTask workerTask = new EnhancedWorkerTask(workerName, minTime, maxTime, errorRate, maxRetries);
            
            // Submit multiple instances for each worker
            for (int j = 0; j < tasksPerWorker + 2; j++) {
                completionService.submit(workerTask);
            }
        }
        
        LOGGER.info(String.format("Submitted %d worker task instances for %d workers", 
                                totalSubmissions, numberOfWorkers));
    }

    private void collectResults(int expectedTasks) throws InterruptedException {
        LOGGER.info("Collecting results from enhanced completion service");
        
        int collectedResults = 0;
        int completedTasks = 0;
        int nullResults = 0;
        long startTime = System.currentTimeMillis();
        
        while (completedTasks < expectedTasks) {
            try {
                // Use timeout to avoid infinite waiting
                Future<ProcessingResult> future = completionService.poll(5, TimeUnit.SECONDS);
                
                if (future != null) {
                    ProcessingResult result = future.get(1, TimeUnit.SECONDS);
                    
                    if (result != null) {
                        resultsManager.addResult(result);
                        completedTasks++;
                        LOGGER.fine("Collected result for task " + result.getTaskId());
                    } else {
                        nullResults++;
                    }
                    
                    collectedResults++;
                } else {
                    // Timeout occurred, check if we should continue waiting
                    long elapsed = System.currentTimeMillis() - startTime;
                    if (elapsed > 60000) { // 60 seconds timeout
                        LOGGER.warning("Timeout waiting for results. Completed: " + completedTasks + "/" + expectedTasks);
                        break;
                    }
                }
                
                // Log progress every 10 tasks
                if (completedTasks > 0 && completedTasks % 10 == 0) {
                    LOGGER.info(String.format("Progress: %d/%d tasks completed, %d null results, Circuit Breaker: %s", 
                                            completedTasks, expectedTasks, nullResults, circuitBreaker.getState()));
                }
                
            } catch (ExecutionException e) {
                LOGGER.log(Level.WARNING, "Enhanced worker task execution failed", e.getCause());
                collectedResults++;
            } catch (TimeoutException e) {
                LOGGER.warning("Timeout getting result from future");
                collectedResults++;
            }
        }
        
        LOGGER.info(String.format("Enhanced collection completed: %d results collected, %d tasks completed, %d null results", 
                                collectedResults, completedTasks, nullResults));
    }

    private void shutdownExecutor() {
        LOGGER.info("Shutting down enhanced executor service");
        
        executorService.shutdown();
        
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                LOGGER.warning("Enhanced executor did not terminate gracefully, forcing shutdown");
                List<Runnable> pendingTasks = executorService.shutdownNow();
                LOGGER.info("Cancelled " + pendingTasks.size() + " pending tasks");
                
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOGGER.severe("Enhanced executor did not terminate after forced shutdown");
                }
            } else {
                LOGGER.info("Enhanced executor terminated gracefully");
            }
        } catch (InterruptedException e) {
            LOGGER.warning("Enhanced shutdown interrupted");
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void printEnhancedStatistics() {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("ENHANCED SYSTEM STATISTICS (Executor Framework + Circuit Breaker)");
        System.out.println("=".repeat(70));
        
        // Queue statistics
        System.out.println("Queue Status: " + taskQueue);
        
        // Executor statistics
        if (executorService instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) executorService;
            System.out.printf("Thread Pool: Core=%d, Max=%d, Active=%d, Completed=%d%n",
                            tpe.getCorePoolSize(), tpe.getMaximumPoolSize(), 
                            tpe.getActiveCount(), tpe.getCompletedTaskCount());
        }
        
        // Circuit breaker statistics
        System.out.println("Circuit Breaker: " + circuitBreaker.getStatistics());
        
        // Processing statistics
        System.out.println("Active Tasks: " + activeTasks.get());
        System.out.println("Retried Tasks: " + retriedTasks.get());
        System.out.println("Total Processing Time: " + totalProcessingTime.get() + " ms");
        
        // Results statistics
        System.out.println("\nResults Manager: " + resultsManager);
        System.out.println(resultsManager.generateSummaryReport());
        
        System.out.println("=".repeat(70));
    }

    private void generateDetailedReport() {
        try {
            String reportFileName = "enhanced_detailed_report_" + 
                                  LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".txt";
            
            StringBuilder report = new StringBuilder();
            report.append("ENHANCED DATA PROCESSING SYSTEM - DETAILED REPORT\n");
            report.append("=".repeat(60)).append("\n");
            report.append("Generated: ").append(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)).append("\n\n");
            
            // System configuration
            report.append("SYSTEM CONFIGURATION:\n");
            report.append("- Workers: ").append(numberOfWorkers).append("\n");
            report.append("- Queue Type: ").append(taskQueue.isPriorityQueue() ? "Priority" : "FIFO").append("\n");
            report.append("- Circuit Breaker: ").append(circuitBreaker.getName()).append("\n\n");
            
            // Performance metrics
            report.append("PERFORMANCE METRICS:\n");
            report.append("- Total Processing Time: ").append(totalProcessingTime.get()).append(" ms\n");
            report.append("- Retried Tasks: ").append(retriedTasks.get()).append("\n");
            report.append("- Circuit Breaker State: ").append(circuitBreaker.getState()).append("\n");
            report.append("- Circuit Breaker Failures: ").append(circuitBreaker.getFailureCount()).append("\n\n");
            
            // Results summary
            report.append(resultsManager.generateSummaryReport());
            
            // Write report to file
            resultsManager.writeSummaryToFile(reportFileName);
            
            LOGGER.info("Detailed report generated: " + reportFileName);
            
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to generate detailed report", e);
        }
    }

    /**
     * Main method to run the enhanced data processing system
     */
    public static void main(String[] args) {
        System.out.println("Enhanced Multi-threaded Data Processing System - Java Executor Framework");
        System.out.println("Features: Circuit Breaker, Retry Logic, Enhanced Monitoring");
        System.out.println("Starting at: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        
        try {
            // Parse command line arguments
            int workers = args.length > 0 ? Integer.parseInt(args[0]) : 4;
            int tasks = args.length > 1 ? Integer.parseInt(args[1]) : 20;
            boolean usePriority = args.length > 2 ? Boolean.parseBoolean(args[2]) : false;
            String outputFile = args.length > 3 ? args[3] : "enhanced_processing_results.txt";
            
            System.out.printf("Configuration: %d workers, %d tasks, priority queue: %s%n", 
                            workers, tasks, usePriority);
            
            // Create and run the enhanced system
            EnhancedDataProcessingSystem system = new EnhancedDataProcessingSystem(workers, usePriority, outputFile);
            system.runProcessing(tasks);
            
        } catch (NumberFormatException e) {
            System.err.println("Error: Invalid number format in arguments");
            System.err.println("Usage: java EnhancedDataProcessingSystem [workers] [tasks] [usePriority] [outputFile]");
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Fatal error in enhanced system: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        
        System.out.println("Enhanced system completed at: " + 
                          LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }
}
