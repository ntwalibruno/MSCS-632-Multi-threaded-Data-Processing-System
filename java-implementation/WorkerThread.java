import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Worker thread that processes tasks from the shared queue
 */
public class WorkerThread extends Thread {
    private static final Logger LOGGER = Logger.getLogger(WorkerThread.class.getName());
    private static final AtomicInteger WORKER_COUNTER = new AtomicInteger(0);
    
    private final String workerName;
    private final SharedResourceQueue taskQueue;
    private final ResultsManager resultsManager;
    private final AtomicBoolean shouldStop = new AtomicBoolean(false);
    private final Random random = new Random();
    private final ReentrantLock processingLock = new ReentrantLock();
    
    // Configuration parameters
    private final long minProcessingTime;
    private final long maxProcessingTime;
    private final double errorRate;
    private final int maxConsecutiveErrors;
    
    // Statistics
    private final AtomicInteger tasksProcessed = new AtomicInteger(0);
    private final AtomicInteger errorsEncountered = new AtomicInteger(0);
    private int consecutiveErrors = 0;

    public WorkerThread(SharedResourceQueue taskQueue, ResultsManager resultsManager,
                       long minProcessingTime, long maxProcessingTime, double errorRate) {
        this.workerName = "Worker-" + WORKER_COUNTER.incrementAndGet();
        this.taskQueue = taskQueue;
        this.resultsManager = resultsManager;
        this.minProcessingTime = minProcessingTime;
        this.maxProcessingTime = maxProcessingTime;
        this.errorRate = Math.max(0.0, Math.min(1.0, errorRate)); // Clamp between 0 and 1
        this.maxConsecutiveErrors = 5;
        
        setName(workerName);
        setDaemon(false); // Ensure JVM waits for worker threads
        
        LOGGER.info(String.format("Worker thread created: %s (processing time: %d-%d ms, error rate: %.2f%%)", 
                                  workerName, minProcessingTime, maxProcessingTime, errorRate * 100));
    }

    public WorkerThread(SharedResourceQueue taskQueue, ResultsManager resultsManager) {
        this(taskQueue, resultsManager, 100, 1000, 0.05); // Default: 100-1000ms, 5% error rate
    }

    @Override
    public void run() {
        LOGGER.info(workerName + " started processing tasks");
        
        try {
            while (!shouldStop.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    // Get task from queue with timeout for better responsiveness
                    Task task = null;
                    try {
                        task = taskQueue.getTask();
                    } catch (InterruptedException e) {
                        LOGGER.info(workerName + " interrupted while getting task");
                        Thread.currentThread().interrupt();
                        break;
                    }
                    
                    if (task == null) {
                        // No more tasks available
                        if (taskQueue.isShutdown()) {
                            LOGGER.info(workerName + " detected shutdown signal - exiting");
                            break;
                        }
                        continue;
                    }

                    // Process the task with ReentrantLock for enhanced synchronization
                    ProcessingResult result = processTaskSafely(task);
                    
                    // Add result to results manager
                    resultsManager.addResult(result);
                    
                    // Mark task as completed in the queue
                    taskQueue.markTaskCompleted();
                    
                    // Update statistics
                    tasksProcessed.incrementAndGet();
                    
                    if (result.isSuccess()) {
                        consecutiveErrors = 0; // Reset consecutive error counter
                    } else {
                        errorsEncountered.incrementAndGet();
                        consecutiveErrors++;
                        
                        // Check if we should stop due to too many consecutive errors
                        if (consecutiveErrors >= maxConsecutiveErrors) {
                            LOGGER.severe(String.format("%s encountered %d consecutive errors - stopping", 
                                                       workerName, consecutiveErrors));
                            break;
                        }
                    }

                } catch (Exception e) {
                    errorsEncountered.incrementAndGet();
                    consecutiveErrors++;
                    LOGGER.log(Level.SEVERE, workerName + " encountered unexpected error", e);
                    
                    if (consecutiveErrors >= maxConsecutiveErrors) {
                        LOGGER.severe(String.format("%s stopping due to %d consecutive errors", 
                                                   workerName, consecutiveErrors));
                        break;
                    }
                    
                    // Brief pause before retrying after error
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            // Ensure processing lock is released
            if (processingLock.isHeldByCurrentThread()) {
                processingLock.unlock();
            }
            LOGGER.info(String.format("%s finished. Tasks processed: %d, Errors: %d", 
                                     workerName, tasksProcessed.get(), errorsEncountered.get()));
        }
    }

    /**
     * Safely processes a task using ReentrantLock for enhanced synchronization
     */
    private ProcessingResult processTaskSafely(Task task) {
        try {
            // Try to acquire lock with timeout to prevent deadlocks
            if (processingLock.tryLock(5, TimeUnit.SECONDS)) {
                try {
                    return processTask(task);
                } finally {
                    processingLock.unlock();
                }
            } else {
                String errorMsg = "Failed to acquire processing lock within timeout";
                LOGGER.warning(String.format("%s: %s for task %d", workerName, errorMsg, task.getId()));
                return new ProcessingResult(task.getId(), task.getData(), null,
                                          0, workerName, false, errorMsg);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            String errorMsg = "Interrupted while acquiring processing lock";
            LOGGER.warning(String.format("%s: %s for task %d", workerName, errorMsg, task.getId()));
            return new ProcessingResult(task.getId(), task.getData(), null,
                                      0, workerName, false, errorMsg);
        }
    }

    /**
     * Processes a single task with simulated work and potential errors
     * Enhanced with better exception handling and resource management
     */
    private ProcessingResult processTask(Task task) {
        long startTime = System.currentTimeMillis();
        
        try {
            LOGGER.fine(String.format("%s processing task: %s", workerName, task));
            
            // Enhanced error simulation with specific exception types
            double errorChance = random.nextDouble();
            if (errorChance < errorRate) {
                if (errorChance < errorRate * 0.3) {
                    throw new IllegalArgumentException("Invalid task data format");
                } else if (errorChance < errorRate * 0.6) {
                    throw new RuntimeException("Resource temporarily unavailable");
                } else {
                    throw new InterruptedException("Processing interrupted by system");
                }
            }
            
            // Simulate processing time with potential for interruption
            long processingTime = minProcessingTime + 
                                (long)(random.nextDouble() * (maxProcessingTime - minProcessingTime));
            
            // Check for interruption before starting processing
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Thread interrupted before processing");
            }
            
            Thread.sleep(processingTime);
            
            // Check for interruption after processing
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Thread interrupted after processing");
            }
            
            // Simulate data processing (transform the data)
            String processedData = processData(task.getData());
            
            long totalTime = System.currentTimeMillis() - startTime;
            
            LOGGER.fine(String.format("%s completed task %d in %d ms", 
                                     workerName, task.getId(), totalTime));
            
            return new ProcessingResult(task.getId(), task.getData(), processedData, 
                                      totalTime, workerName);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            String errorMsg = "Task processing interrupted";
            LOGGER.warning(String.format("%s: %s for task %d", workerName, errorMsg, task.getId()));
            return new ProcessingResult(task.getId(), task.getData(), null,
                                      System.currentTimeMillis() - startTime,
                                      workerName, false, errorMsg);
        } catch (IllegalArgumentException e) {
            String errorMsg = "Invalid task data: " + e.getMessage();
            LOGGER.log(Level.WARNING, String.format("%s: %s for task %d", workerName, errorMsg, task.getId()), e);
            return new ProcessingResult(task.getId(), task.getData(), null,
                                      System.currentTimeMillis() - startTime,
                                      workerName, false, errorMsg);
        } catch (RuntimeException e) {
            String errorMsg = "Runtime error during processing: " + e.getMessage();
            LOGGER.log(Level.WARNING, String.format("%s: %s for task %d", workerName, errorMsg, task.getId()), e);
            return new ProcessingResult(task.getId(), task.getData(), null,
                                      System.currentTimeMillis() - startTime,
                                      workerName, false, errorMsg);
        } catch (Exception e) {
            String errorMsg = "Unexpected error during task processing: " + e.getMessage();
            LOGGER.log(Level.WARNING, String.format("%s: %s for task %d", workerName, errorMsg, task.getId()), e);
            return new ProcessingResult(task.getId(), task.getData(), null,
                                      System.currentTimeMillis() - startTime,
                                      workerName, false, errorMsg);
        }
    }

    /**
     * Simulates data processing transformation
     */
    private String processData(String data) {
        if (data == null || data.trim().isEmpty()) {
            return "[EMPTY]";
        }
        
        // Various data transformations
        String processed = data.toUpperCase()
                              .replaceAll("\\s+", "_")
                              .replaceAll("[^A-Z0-9_]", "");
        
        // Add some computation result
        int hash = data.hashCode();
        processed += "_HASH_" + Math.abs(hash);
        
        // Add worker signature
        processed += "_BY_" + workerName;
        
        return processed;
    }

    /**
     * Gracefully stops the worker thread
     */
    public void stopWorker() {
        shouldStop.set(true);
        this.interrupt();
        LOGGER.info("Stop signal sent to " + workerName);
    }

    // Getters for statistics
    public String getWorkerName() { return workerName; }
    public int getTasksProcessed() { return tasksProcessed.get(); }
    public int getErrorsEncountered() { return errorsEncountered.get(); }
    public boolean isStopping() { return shouldStop.get(); }
    
    @Override
    public String toString() {
        return String.format("WorkerThread{name='%s', tasksProcessed=%d, errors=%d, active=%s}", 
                           workerName, tasksProcessed.get(), errorsEncountered.get(), isAlive());
    }
}
