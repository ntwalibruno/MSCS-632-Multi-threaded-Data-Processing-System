import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Circuit breaker pattern implementation for enhanced error handling
 * Provides automatic failure detection and recovery mechanisms
 */
public class CircuitBreaker {
    private static final Logger LOGGER = Logger.getLogger(CircuitBreaker.class.getName());
    
    public enum State { CLOSED, OPEN, HALF_OPEN }
    
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    
    private final int failureThreshold;
    private final long timeout;
    private final int successThreshold;
    private final String name;

    /**
     * Creates a new Circuit Breaker
     * @param name Name for logging purposes
     * @param failureThreshold Number of failures before opening circuit
     * @param timeoutMs Time to wait before trying half-open state
     * @param successThreshold Number of successes needed to close circuit from half-open
     */
    public CircuitBreaker(String name, int failureThreshold, long timeoutMs, int successThreshold) {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.timeout = timeoutMs;
        this.successThreshold = successThreshold;
        
        LOGGER.info(String.format("Circuit breaker '%s' initialized: failureThreshold=%d, timeout=%dms, successThreshold=%d",
                                  name, failureThreshold, timeoutMs, successThreshold));
    }

    /**
     * Executes a task through the circuit breaker
     */
    public <T> T execute(CircuitBreakerTask<T> task) throws Exception {
        State currentState = state.get();
        
        if (currentState == State.OPEN) {
            if (System.currentTimeMillis() - lastFailureTime.get() < timeout) {
                throw new CircuitBreakerOpenException("Circuit breaker '" + name + "' is OPEN");
            } else {
                // Try to move to half-open
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    successCount.set(0);
                    LOGGER.info("Circuit breaker '" + name + "' moved to HALF_OPEN state");
                }
            }
        }

        try {
            T result = task.execute();
            onSuccess();
            return result;
        } catch (Exception e) {
            onFailure();
            throw e;
        }
    }

    private void onSuccess() {
        State currentState = state.get();
        
        if (currentState == State.HALF_OPEN) {
            int successes = successCount.incrementAndGet();
            if (successes >= successThreshold) {
                if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                    failureCount.set(0);
                    successCount.set(0);
                    LOGGER.info("Circuit breaker '" + name + "' moved to CLOSED state after " + successes + " successes");
                }
            }
        } else if (currentState == State.CLOSED) {
            // Reset failure count on success
            failureCount.set(0);
        }
    }

    private void onFailure() {
        lastFailureTime.set(System.currentTimeMillis());
        int failures = failureCount.incrementAndGet();
        
        State currentState = state.get();
        if ((currentState == State.CLOSED || currentState == State.HALF_OPEN) && failures >= failureThreshold) {
            if (state.compareAndSet(currentState, State.OPEN)) {
                LOGGER.warning("Circuit breaker '" + name + "' moved to OPEN state after " + failures + " failures");
            }
        }
    }

    // Getters for monitoring
    public State getState() { return state.get(); }
    public int getFailureCount() { return failureCount.get(); }
    public int getSuccessCount() { return successCount.get(); }
    public String getName() { return name; }
    public long getLastFailureTime() { return lastFailureTime.get(); }

    /**
     * Manually reset the circuit breaker to closed state
     */
    public void reset() {
        state.set(State.CLOSED);
        failureCount.set(0);
        successCount.set(0);
        lastFailureTime.set(0);
        LOGGER.info("Circuit breaker '" + name + "' manually reset to CLOSED state");
    }

    /**
     * Get circuit breaker statistics
     */
    public String getStatistics() {
        return String.format("CircuitBreaker{name='%s', state=%s, failures=%d, successes=%d, lastFailure=%d}",
                           name, state.get(), failureCount.get(), successCount.get(), lastFailureTime.get());
    }

    @Override
    public String toString() {
        return getStatistics();
    }

    /**
     * Functional interface for tasks that can be executed through circuit breaker
     */
    @FunctionalInterface
    public interface CircuitBreakerTask<T> {
        T execute() throws Exception;
    }

    /**
     * Exception thrown when circuit breaker is in OPEN state
     */
    public static class CircuitBreakerOpenException extends Exception {
        public CircuitBreakerOpenException(String message) {
            super(message);
        }
    }
}
