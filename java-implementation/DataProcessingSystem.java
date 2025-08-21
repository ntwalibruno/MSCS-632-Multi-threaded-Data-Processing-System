import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Main class that orchestrates the multi-threaded data processing system
 */
public class DataProcessingSystem {
    private static final Logger LOGGER = Logger.getLogger(DataProcessingSystem.class.getName());
    
    private final SharedResourceQueue taskQueue;
    private final ResultsManager resultsManager;
    private final List<WorkerThread> workers;
    private final int numberOfWorkers;
    
    // Configuration
    private static final int DEFAULT_WORKERS = 4;
    private static final int DEFAULT_TASKS = 20;
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

    public DataProcessingSystem(int numberOfWorkers, boolean usePriorityQueue, String outputFile) {
        this.numberOfWorkers = numberOfWorkers;
        this.taskQueue = new SharedResourceQueue(usePriorityQueue);
        this.resultsManager = new ResultsManager(outputFile);
        this.workers = new ArrayList<>();
        
        setupLogging();
        LOGGER.info(String.format("DataProcessingSystem initialized with %d workers, priority queue: %s", 
                                  numberOfWorkers, usePriorityQueue));
    }

    public DataProcessingSystem(int numberOfWorkers) {
        this(numberOfWorkers, false, null);
    }

    public DataProcessingSystem() {
        this(DEFAULT_WORKERS);
    }

    /**
     * Sets up logging configuration
     */
    private void setupLogging() {
        try {
            // Create file handler for logging
            String logFileName = "data_processing_" + 
                               LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + 
                               ".log";
            FileHandler fileHandler = new FileHandler(logFileName, true);
            fileHandler.setFormatter(new SimpleFormatter());
            
            // Configure root logger
            Logger rootLogger = Logger.getLogger("");
            rootLogger.setLevel(Level.INFO);
            rootLogger.addHandler(fileHandler);
            
            // Ensure console output
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.INFO);
            rootLogger.addHandler(consoleHandler);
            
            LOGGER.info("Logging configured. Log file: " + logFileName);
        } catch (Exception e) {
            System.err.println("Failed to setup file logging: " + e.getMessage());
        }
    }

    /**
     * Creates and starts worker threads
     */
    public void startWorkers() {
        LOGGER.info("Starting " + numberOfWorkers + " worker threads");
        
        for (int i = 0; i < numberOfWorkers; i++) {
            // Create workers with slightly different configurations for variety
            long minTime = 50 + (i * 25);  // Stagger processing times
            long maxTime = 500 + (i * 100);
            double errorRate = 0.02 + (i * 0.01); // Gradually increase error rates
            
            WorkerThread worker = new WorkerThread(taskQueue, resultsManager, minTime, maxTime, errorRate);
            workers.add(worker);
            worker.start();
        }
        
        LOGGER.info("All worker threads started");
    }

    /**
     * Adds tasks to the processing queue
     */
    public void addTasks(int numberOfTasks) {
        LOGGER.info("Adding " + numberOfTasks + " tasks to the queue");
        
        for (int i = 1; i <= numberOfTasks; i++) {
            // Create diverse task data
            String data = generateTaskData(i);
            int priority = (i % 3); // Vary priorities (0, 1, 2)
            
            Task task = new Task(i, data, priority);
            boolean added = taskQueue.addTask(task);
            
            if (!added) {
                LOGGER.warning("Failed to add task " + i);
            }
        }
        
        LOGGER.info("Finished adding tasks to queue");
    }

    /**
     * Generates sample task data
     */
    private String generateTaskData(int taskId) {
        String[] prefixes = {"Process", "Analyze", "Transform", "Compute", "Calculate"};
        String[] subjects = {"Data Set", "User Input", "File Content", "Database Record", "API Response"};
        String[] operations = {"validation", "encryption", "compression", "parsing", "aggregation"};
        
        String prefix = prefixes[taskId % prefixes.length];
        String subject = subjects[taskId % subjects.length];
        String operation = operations[taskId % operations.length];
        
        return String.format("%s %s for %s (ID: %d)", prefix, subject, operation, taskId);
    }

    /**
     * Initiates shutdown sequence and waits for completion
     */
    public void shutdown() {
        LOGGER.info("Initiating system shutdown");
        
        try {
            // Signal shutdown to the task queue
            taskQueue.shutdown();
            
            // Wait for all workers to complete their current tasks
            LOGGER.info("Waiting for workers to complete...");
            
            long startTime = System.currentTimeMillis();
            boolean allCompleted = false;
            
            while (!allCompleted && 
                   (System.currentTimeMillis() - startTime) < (SHUTDOWN_TIMEOUT_SECONDS * 1000)) {
                
                allCompleted = true;
                for (WorkerThread worker : workers) {
                    if (worker.isAlive()) {
                        allCompleted = false;
                        break;
                    }
                }
                
                if (!allCompleted) {
                    Thread.sleep(100); // Wait a bit before checking again
                }
            }
            
            // Force stop any remaining workers
            if (!allCompleted) {
                LOGGER.warning("Timeout reached - forcefully stopping remaining workers");
                for (WorkerThread worker : workers) {
                    if (worker.isAlive()) {
                        worker.stopWorker();
                        worker.join(1000); // Wait up to 1 second for graceful shutdown
                        if (worker.isAlive()) {
                            LOGGER.warning("Worker " + worker.getWorkerName() + " did not stop gracefully");
                        }
                    }
                }
            }
            
            LOGGER.info("All workers have stopped");
            
        } catch (InterruptedException e) {
            LOGGER.warning("Shutdown interrupted");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error during shutdown", e);
        }
    }

    /**
     * Prints system statistics
     */
    public void printStatistics() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("SYSTEM STATISTICS");
        System.out.println("=".repeat(60));
        
        // Queue statistics
        System.out.println("Queue Status: " + taskQueue);
        
        // Worker statistics
        System.out.println("\nWorker Statistics:");
        for (WorkerThread worker : workers) {
            System.out.println("  " + worker);
        }
        
        // Results statistics
        System.out.println("\nResults: " + resultsManager);
        System.out.println(resultsManager.generateSummaryReport());
        
        System.out.println("=".repeat(60));
    }

    /**
     * Runs the complete data processing workflow
     */
    public void runProcessing(int numberOfTasks) {
        try {
            LOGGER.info("Starting data processing workflow");
            
            // Start workers
            startWorkers();
            
            // Add tasks
            addTasks(numberOfTasks);
            
            // Wait for all tasks to be processed
            waitForCompletion();
            
            // Shutdown system
            shutdown();
            
            // Print final statistics
            printStatistics();
            
            // Generate summary report
            resultsManager.writeSummaryToFile("processing_summary_" + 
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".txt");
            
            LOGGER.info("Data processing workflow completed successfully");
            
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in processing workflow", e);
        }
    }

    /**
     * Waits for all tasks to be completed
     */
    private void waitForCompletion() throws InterruptedException {
        LOGGER.info("Waiting for all tasks to be completed");
        
        while (!taskQueue.isAllTasksCompleted()) {
            Thread.sleep(500);
            
            // Log progress periodically
            if (taskQueue.getCompletedTasks() % 5 == 0 && taskQueue.getPendingTasks() > 0) {
                LOGGER.info(String.format("Progress: %d/%d tasks completed", 
                                        taskQueue.getCompletedTasks(), taskQueue.getTotalTasks()));
            }
        }
        
        LOGGER.info("All tasks have been completed");
    }

    /**
     * Main method to run the data processing system
     */
    public static void main(String[] args) {
        System.out.println("Multi-threaded Data Processing System - Java Implementation");
        System.out.println("Starting at: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        
        try {
            // Parse command line arguments
            int workers = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_WORKERS;
            int tasks = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_TASKS;
            boolean usePriority = args.length > 2 ? Boolean.parseBoolean(args[2]) : false;
            String outputFile = args.length > 3 ? args[3] : "processing_results.txt";
            
            System.out.printf("Configuration: %d workers, %d tasks, priority queue: %s%n", 
                            workers, tasks, usePriority);
            
            // Create and run the system
            DataProcessingSystem system = new DataProcessingSystem(workers, usePriority, outputFile);
            system.runProcessing(tasks);
            
        } catch (NumberFormatException e) {
            System.err.println("Error: Invalid number format in arguments");
            System.err.println("Usage: java DataProcessingSystem [workers] [tasks] [usePriority] [outputFile]");
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        
        System.out.println("System completed at: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }
}
