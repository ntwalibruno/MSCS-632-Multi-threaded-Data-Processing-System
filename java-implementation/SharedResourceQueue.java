import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Comparator;

/**
 * Thread-safe shared resource queue that manages tasks for worker threads
 */
public class SharedResourceQueue {
    private static final Logger LOGGER = Logger.getLogger(SharedResourceQueue.class.getName());
    
    private final BlockingQueue<Task> taskQueue;
    private final AtomicInteger totalTasks = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final boolean priorityQueue;

    public SharedResourceQueue(boolean usePriority) {
        this.priorityQueue = usePriority;
        if (usePriority) {
            // Priority queue - higher priority values processed first
            this.taskQueue = new PriorityBlockingQueue<>(100, 
                Comparator.comparingInt(Task::getPriority).reversed()
                          .thenComparingLong(Task::getTimestamp));
        } else {
            this.taskQueue = new LinkedBlockingQueue<>();
        }
        LOGGER.info("SharedResourceQueue initialized with " + 
                   (usePriority ? "priority" : "FIFO") + " ordering");
    }

    public SharedResourceQueue() {
        this(false); // Default to FIFO
    }

    /**
     * Adds a task to the queue
     */
    public boolean addTask(Task task) {
        if (isShutdown.get()) {
            LOGGER.warning("Cannot add task - queue is shutdown: " + task);
            return false;
        }

        try {
            boolean added = taskQueue.offer(task);
            if (added) {
                totalTasks.incrementAndGet();
                LOGGER.fine("Task added to queue: " + task);
            } else {
                LOGGER.warning("Failed to add task to queue: " + task);
            }
            return added;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error adding task to queue: " + task, e);
            return false;
        }
    }

    /**
     * Retrieves a task from the queue, blocking if necessary
     */
    public Task getTask() throws InterruptedException {
        if (isShutdown.get() && taskQueue.isEmpty()) {
            return null; // No more tasks available
        }

        try {
            Task task = taskQueue.take(); // Blocks until task is available
            LOGGER.fine("Task retrieved from queue: " + task);
            return task;
        } catch (InterruptedException e) {
            LOGGER.info("Thread interrupted while waiting for task");
            Thread.currentThread().interrupt(); // Restore interrupt status
            throw e;
        }
    }

    /**
     * Attempts to retrieve a task without blocking
     */
    public Task tryGetTask() {
        try {
            Task task = taskQueue.poll();
            if (task != null) {
                LOGGER.fine("Task retrieved from queue (non-blocking): " + task);
            }
            return task;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error retrieving task from queue", e);
            return null;
        }
    }

    /**
     * Marks a task as completed
     */
    public void markTaskCompleted() {
        int completed = completedTasks.incrementAndGet();
        LOGGER.fine("Task marked as completed. Total completed: " + completed);
    }

    /**
     * Initiates shutdown of the queue
     */
    public void shutdown() {
        isShutdown.set(true);
        LOGGER.info("SharedResourceQueue shutdown initiated");
    }

    /**
     * Checks if all tasks have been completed
     */
    public boolean isAllTasksCompleted() {
        return isShutdown.get() && 
               taskQueue.isEmpty() && 
               completedTasks.get() >= totalTasks.get();
    }

    // Statistics methods
    public int getTotalTasks() { return totalTasks.get(); }
    public int getCompletedTasks() { return completedTasks.get(); }
    public int getPendingTasks() { return taskQueue.size(); }
    public boolean isShutdown() { return isShutdown.get(); }
    public boolean isPriorityQueue() { return priorityQueue; }

    @Override
    public String toString() {
        return String.format("SharedResourceQueue{total=%d, completed=%d, pending=%d, shutdown=%s}", 
                           totalTasks.get(), completedTasks.get(), taskQueue.size(), isShutdown.get());
    }
}
