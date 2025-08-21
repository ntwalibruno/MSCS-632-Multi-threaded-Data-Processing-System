# MSCS-632-Multi-threaded-Data-Processing-System

This project implements a comprehensive multi-threaded data processing system in both **Java** and **Go**, demonstrating different concurrency models and error-handling mechanisms. The system simulates multiple worker threads processing data in parallel while managing shared resources efficiently.

## System Architecture

### Core Components

1. **Shared Resource Queue**: Thread-safe task queue with synchronization
2. **Worker Threads/Goroutines**: Parallel task processors
3. **Results Manager**: Thread-safe results collection and file output
4. **Concurrency Management**: Deadlock prevention and safe termination
5. **Exception Handling**: Comprehensive error management
6. **Logging System**: Detailed execution tracking

## 📁 Project Structure

```
Multi-threaded Data Processing System/
├── java-implementation/
│   ├── Task.java                         # Task representation
│   ├── ProcessingResult.java             # Result representation
│   ├── SharedResourceQueue.java          # Thread-safe task queue
│   ├── ResultsManager.java               # Results collection and file I/O
│   ├── WorkerThread.java                 # Worker thread implementation (enhanced with ReentrantLock)
│   ├── DataProcessingSystem.java         # Main orchestrator
│   ├── CircuitBreaker.java              # Circuit breaker pattern implementation
│   └── DataProcessingSystem.java # Executor framework implementation
├── go-implementation/
│   │─  main.go                 # Enhanced Go implementation with advanced features
│   └── go.mod                           # Go module definition
└── README.md                           # This file
```

## Features

### Java Implementation

- **Synchronization**: Uses `BlockingQueue`, `ConcurrentLinkedQueue`, `ReadWriteLock`, and `ReentrantLock`
- **Thread Management**: Custom `WorkerThread` classes and Executor framework
- **Error Handling**: Try-catch blocks with detailed exception logging and Circuit Breaker pattern
- **Priority Queue**: Optional priority-based task processing
- **Statistics**: Real-time processing statistics and reporting
- **File I/O**: Thread-safe file writing with proper resource management
- **Circuit Breaker**: Automatic failure detection and recovery
- **Retry Logic**: Exponential backoff retry mechanisms

### Go Implementation

- **Channels**: Goroutine communication via buffered channels with retry queues
- **Context**: Graceful cancellation and timeout handling
- **Error Handling**: Explicit error returns with `defer` statements and panic recovery
- **Worker Pool**: Coordinated goroutine management with enhanced monitoring
- **Signal Handling**: Graceful shutdown on system signals
- **Memory Safety**: Race-condition prevention with mutexes
- **Metrics**: Real-time metrics collection and JSON logging
- **Retry Mechanisms**: Advanced retry queues with priority handling

### Prerequisites

**For Java:**
- Java Development Kit (JDK) 8 or higher
- PATH configured for `java` and `javac`

**For Go:**
- Go 1.19 or higher
- PATH configured for `go`

### Manual Execution

#### Java
```cmd
cd java-implementation
javac *.java

# Original implementation
java DataProcessingSystem [workers] [tasks] [usePriority] [outputFile]

# Enhanced implementation with Executor framework and Circuit Breaker
java EnhancedDataProcessingSystem [workers] [tasks] [usePriority] [outputFile]
```

#### Go
```cmd
cd go-implementation

# Original implementation
go build -o data-processing-system.exe main.go
data-processing-system.exe [workers] [tasks] [outputFile]

### Parameters

- **workers**: Number of worker threads/goroutines (default: 4)
- **tasks**: Number of tasks to process (default: 20)
- **usePriority**: Use priority queue in Java (default: false)
- **outputFile**: Output file for results (default: processing_results.txt)

## Concurrency & Safety Features

### Deadlock Prevention

**Java:**
- Uses `BlockingQueue.take()` for safe blocking
- Timeout-based operations where appropriate
- Proper lock ordering in `ReadWriteLock`
- Atomic operations for counters

**Go:**
- Channel-based communication (CSP model)
- Context cancellation for timeouts
- Select statements for non-blocking operations
- Mutex protection for shared state

### Error Handling

**Java:**
- Comprehensive try-catch blocks
- Checked exception propagation
- Resource cleanup in finally blocks
- Logging at multiple severity levels

**Go:**
- Explicit error returns
- `defer` statements for cleanup
- Panic recovery in goroutines
- Context-aware error handling

### Safe Termination

**Java:**
- Graceful shutdown signals
- Worker thread interruption
- Timeout-based forced termination
- Resource cleanup verification

**Go:**
- Context cancellation propagation
- Channel closure signals
- WaitGroup synchronization
- Signal handling for graceful shutdown

## Output Examples

### Console Output
```
Multi-threaded Data Processing System - Java Implementation
Starting at: 2025-08-20T14:30:15.123
Configuration: 4 workers, 20 tasks, priority queue: false

## Concurrency Concepts Demonstrated

### Java Specific
- `BlockingQueue` for producer-consumer patterns
- `ConcurrentLinkedQueue` for lock-free operations
- `ReadWriteLock` for reader-writer scenarios
- `AtomicInteger` for thread-safe counters
- Thread interruption and graceful shutdown

### Go Specific
- Channel-based communication (CSP model)
- Goroutine lifecycle management
- Context-based cancellation
- Select statements for multiplexing
- WaitGroup for synchronization
- Mutex for critical sections

This implementation serves as a comprehensive example of multi-threaded programming best practices in both Java and Go, showcasing the unique strengths and approaches of each language's concurrency model.
