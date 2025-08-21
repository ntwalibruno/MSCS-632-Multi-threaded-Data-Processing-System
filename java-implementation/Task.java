/**
 * Represents a data processing task with unique ID and data payload
 */
public class Task {
    private final int id;
    private final String data;
    private final long timestamp;
    private final int priority;

    public Task(int id, String data, int priority) {
        this.id = id;
        this.data = data;
        this.priority = priority;
        this.timestamp = System.currentTimeMillis();
    }

    public Task(int id, String data) {
        this(id, data, 0); // Default priority
    }

    public int getId() {
        return id;
    }

    public String getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getPriority() {
        return priority;
    }

    @Override
    public String toString() {
        return String.format("Task{id=%d, data='%s', priority=%d, timestamp=%d}", 
                           id, data, priority, timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Task task = (Task) obj;
        return id == task.id;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }
}
