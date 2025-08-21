/**
 * Represents the result of processing a task
 */
public class ProcessingResult {
    private final int taskId;
    private final String originalData;
    private final String processedData;
    private final long processingTime;
    private final String workerName;
    private final boolean success;
    private final String errorMessage;

    public ProcessingResult(int taskId, String originalData, String processedData, 
                          long processingTime, String workerName, boolean success, String errorMessage) {
        this.taskId = taskId;
        this.originalData = originalData;
        this.processedData = processedData;
        this.processingTime = processingTime;
        this.workerName = workerName;
        this.success = success;
        this.errorMessage = errorMessage;
    }

    public ProcessingResult(int taskId, String originalData, String processedData, 
                          long processingTime, String workerName) {
        this(taskId, originalData, processedData, processingTime, workerName, true, null);
    }

    // Getters
    public int getTaskId() { return taskId; }
    public String getOriginalData() { return originalData; }
    public String getProcessedData() { return processedData; }
    public long getProcessingTime() { return processingTime; }
    public String getWorkerName() { return workerName; }
    public boolean isSuccess() { return success; }
    public String getErrorMessage() { return errorMessage; }

    @Override
    public String toString() {
        if (success) {
            return String.format("Result{taskId=%d, worker='%s', processingTime=%dms, processedData='%s'}", 
                               taskId, workerName, processingTime, processedData);
        } else {
            return String.format("Result{taskId=%d, worker='%s', ERROR='%s'}", 
                               taskId, workerName, errorMessage);
        }
    }
}
