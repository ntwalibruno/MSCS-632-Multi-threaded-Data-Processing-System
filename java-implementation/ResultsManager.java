import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.List;
import java.util.ArrayList;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Thread-safe manager for processing results with file output capabilities
 */
public class ResultsManager {
    private static final Logger LOGGER = Logger.getLogger(ResultsManager.class.getName());
    
    private final ConcurrentLinkedQueue<ProcessingResult> results = new ConcurrentLinkedQueue<>();
    private final ReadWriteLock fileLock = new ReentrantReadWriteLock();
    private final AtomicInteger totalResults = new AtomicInteger(0);
    private final String outputFileName;
    private final boolean writeToFile;

    public ResultsManager(String outputFileName) {
        this.outputFileName = outputFileName;
        this.writeToFile = outputFileName != null && !outputFileName.trim().isEmpty();
        LOGGER.info("ResultsManager initialized" + 
                   (writeToFile ? " with output file: " + outputFileName : " (memory only)"));
    }

    public ResultsManager() {
        this(null); // Memory only
    }

    /**
     * Adds a processing result to the shared results collection
     */
    public void addResult(ProcessingResult result) {
        if (result == null) {
            LOGGER.warning("Attempted to add null result");
            return;
        }

        try {
            results.offer(result);
            totalResults.incrementAndGet();
            
            LOGGER.fine("Result added: " + result);

            // Write to file if configured
            if (writeToFile) {
                writeResultToFile(result);
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error adding result: " + result, e);
        }
    }

    /**
     * Writes a single result to the output file
     */
    private void writeResultToFile(ProcessingResult result) {
        fileLock.writeLock().lock();
        try (FileWriter writer = new FileWriter(outputFileName, true)) {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            writer.write(String.format("[%s] %s%n", timestamp, result.toString()));
            writer.flush();
            LOGGER.fine("Result written to file: " + result.getTaskId());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error writing result to file: " + outputFileName, e);
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    /**
     * Retrieves all results as a list (snapshot)
     */
    public List<ProcessingResult> getAllResults() {
        return new ArrayList<>(results);
    }

    /**
     * Retrieves only successful results
     */
    public List<ProcessingResult> getSuccessfulResults() {
        return results.stream()
                     .filter(ProcessingResult::isSuccess)
                     .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    /**
     * Retrieves only failed results
     */
    public List<ProcessingResult> getFailedResults() {
        return results.stream()
                     .filter(result -> !result.isSuccess())
                     .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    /**
     * Generates a summary report of all results
     */
    public String generateSummaryReport() {
        List<ProcessingResult> allResults = getAllResults();
        long successCount = allResults.stream().mapToLong(r -> r.isSuccess() ? 1 : 0).sum();
        long failureCount = allResults.size() - successCount;
        
        double avgProcessingTime = allResults.stream()
                                           .filter(ProcessingResult::isSuccess)
                                           .mapToLong(ProcessingResult::getProcessingTime)
                                           .average()
                                           .orElse(0.0);

        StringBuilder report = new StringBuilder();
        report.append("=== PROCESSING SUMMARY REPORT ===\n");
        report.append(String.format("Total Results: %d\n", allResults.size()));
        report.append(String.format("Successful: %d\n", successCount));
        report.append(String.format("Failed: %d\n", failureCount));
        report.append(String.format("Success Rate: %.2f%%\n", 
                                   allResults.isEmpty() ? 0.0 : (successCount * 100.0 / allResults.size())));
        report.append(String.format("Average Processing Time: %.2f ms\n", avgProcessingTime));
        
        if (writeToFile) {
            report.append(String.format("Output File: %s\n", outputFileName));
        }
        
        return report.toString();
    }

    /**
     * Writes the summary report to a file
     */
    public void writeSummaryToFile(String summaryFileName) {
        fileLock.writeLock().lock();
        try (FileWriter writer = new FileWriter(summaryFileName)) {
            writer.write(generateSummaryReport());
            writer.flush();
            LOGGER.info("Summary report written to: " + summaryFileName);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error writing summary to file: " + summaryFileName, e);
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    /**
     * Clears all results (use with caution in multi-threaded environment)
     */
    public void clearResults() {
        results.clear();
        totalResults.set(0);
        LOGGER.info("All results cleared");
    }

    // Statistics methods
    public int getTotalResults() { return totalResults.get(); }
    public int getCurrentResultCount() { return results.size(); }
    public boolean isWritingToFile() { return writeToFile; }
    public String getOutputFileName() { return outputFileName; }

    @Override
    public String toString() {
        return String.format("ResultsManager{totalResults=%d, currentCount=%d, outputFile='%s'}", 
                           totalResults.get(), results.size(), outputFileName);
    }
}
