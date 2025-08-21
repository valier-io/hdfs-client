# HdfsTransferManager Integration Tests

This document describes the integration tests for the HdfsTransferManager, which demonstrate its parallel directory download capabilities with a real HDFS cluster running in Docker containers.

## Overview

The integration tests showcase the following HdfsTransferManager features:

### Core Capabilities Tested

1. **Single Thread Downloads** (`threadPoolSize = 1`)
   - Uses single thread pool for sequential file processing
   - Demonstrates precise timing and error handling

2. **Parallel Downloads** (`threadPoolSize = 4`)
   - Uses thread pool for concurrent file transfers
   - Shows performance improvements for bulk operations

3. **High Concurrency** (`threadPoolSize = 10`)
   - Tests scalability with many parallel transfers
   - Measures throughput and performance metrics

4. **Builder Pattern Validation**
   - Tests parameter validation and error handling
   - Ensures proper configuration requirements

5. **Asynchronous Completion**
   - Demonstrates `CompletableFuture` usage
   - Shows non-blocking completion handling

6. **Error Handling**
   - Tests resilience to network failures
   - Validates error reporting and recovery

### Test Data Structure

The integration tests use a variety of test files:

```
/test-data/
├── small-file-1.txt        (< 1KB text files)
├── small-file-2.txt
├── ...
├── medium-file-1.txt       (~10KB files)
├── medium-file-2.txt
├── large-file.txt          (~100KB file)
├── binary-data.bin         (50KB binary file)
└── nested/                 (subdirectory)
    ├── nested-1.txt
    ├── nested-2.txt
    └── nested-3.txt
```

## Prerequisites

- Docker and Docker Compose installed
- Java 11+
- Maven 3.6+
- Available ports: 9000 (NameNode), 9866 (DataNode), 9870 (Web UI), 9864 (DataNode HTTP)

## Running the Tests

### Quick Start

```bash
cd valier-hdfs-file-manager
./run-integration-tests.sh
```

This script will:
1. Start HDFS Docker containers (NameNode + DataNode)
2. Set up test data in HDFS
3. Run all integration tests
4. Clean up containers afterwards

### Skipping Integration Tests

Integration tests can be resource-intensive and require Docker infrastructure. During normal development, you can skip them:

```bash
# Skip integration tests during build
mvn install -DskipITs

# Skip integration tests during verify phase
mvn verify -DskipITs
```

Integration tests will run by default during `mvn verify` or `mvn integration-test` unless explicitly skipped.

### Manual Execution

If you prefer to run the tests manually:

```bash
# 1. Start HDFS cluster
docker-compose -f src/test/resources/docker-compose-test.yml up -d

# 2. Wait for services to be ready (30+ seconds)
sleep 30

# 3. Set up test data
docker run --rm \
    --network hdfs-filemanager-test \
    -v "$(pwd)/src/test/resources/setup-test-data.sh:/setup-test-data.sh:ro" \
    apache/hadoop:3.3.6 \
    /setup-test-data.sh

# 4. Run integration tests
mvn verify -Dit.test=HdfsTransferManagerIntegrationTest

# 5. Clean up
docker-compose -f src/test/resources/docker-compose-test.yml down -v
```

## Test Details

### TestSingleThreadDirectoryDownload

Tests single-threaded execution using a single thread pool:

```java
HdfsTransferManager transferManager = HdfsTransferManager.builder()
    .hdfsClient(hdfsClient)
    .threadPoolSize(1)  // Single thread pool
    .build();

CompletedDirectoryDownload download = transferManager.downloadDirectory(
    "/test-data", localPath);

download.waitForCompletion();
```

**Verifies:**
- Sequential file processing
- Timing accuracy
- File integrity
- Resource management

### TestParallelDirectoryDownload

Tests multi-threaded execution with moderate concurrency:

```java
HdfsTransferManager transferManager = HdfsTransferManager.builder()
    .hdfsClient(hdfsClient)
    .threadPoolSize(4)  // 4 parallel threads
    .build();
```

**Verifies:**
- Parallel processing efficiency
- Individual file metrics
- Concurrent resource access
- Thread safety

### TestHighConcurrencyDownload

Tests scalability with high thread count:

```java
HdfsTransferManager transferManager = HdfsTransferManager.builder()
    .hdfsClient(hdfsClient)
    .threadPoolSize(10)  // 10 parallel threads
    .build();
```

**Verifies:**
- High concurrency handling
- Throughput calculations
- Performance metrics
- System resource utilization

### TestTransferManagerBuilder

Tests configuration validation:

```java
// Test negative thread pool size validation
try {
    HdfsTransferManager.builder()
        .hdfsClient(hdfsClient)
        .threadPoolSize(-1)
        .build();
    fail("Should reject negative thread pool size");
} catch (IllegalArgumentException e) {
    // Expected
}
```

**Verifies:**
- Parameter validation
- Required field checks
- Error message clarity
- Configuration flexibility

### TestErrorHandling

Tests resilience and error recovery:

```java
CompletedDirectoryDownload download = transferManager.downloadDirectory(
    "/non-existent-directory", localPath);
```

**Verifies:**
- Exception handling
- Error propagation
- Failure reporting
- Resource cleanup

### TestAsyncCompletion

Tests asynchronous completion patterns:

```java
CompletedDirectoryDownload download = transferManager.downloadDirectory(
    "/test-data", localPath);

download.getCompletionFuture().thenRun(() -> {
    System.out.println("Download completed!");
});

download.waitForCompletion();
```

**Verifies:**
- Non-blocking operations
- Callback execution
- Future composition
- Status monitoring

## Expected Output

When running successfully, you'll see output like:

```
=== Testing Single Thread Directory Download (threadPoolSize=1) ===
Single Thread Download Results:
  Total files: 11
  Successful: 11
  Failed: 0
  Total time: 1250 ms

=== Testing Parallel Directory Download (threadPoolSize=4) ===
Parallel Download Results:
  Total files: 11
  Successful: 11
  Failed: 0
  Total time: 850 ms
Individual File Download Times:
  small-file-1.txt: 87 bytes in 45 ms
  medium-file-1.txt: 10051 bytes in 120 ms
  large-file.txt: 100025 bytes in 280 ms
  ...

=== Testing High Concurrency Download (threadPoolSize=10) ===
High Concurrency Download Results:
  Total files: 11
  Total bytes: 156789
  Total time: 650 ms
  Throughput: 0.24 MB/s
```

## Performance Benchmarks

The tests provide performance insights:

- **Single Thread**: Baseline single-threaded performance
- **Parallel (4 threads)**: Typically 20-40% faster for mixed file sizes
- **High Concurrency (10 threads)**: Best for many small files, may show diminishing returns

Actual performance depends on:
- Network latency to HDFS
- File sizes and count
- System resources (CPU, memory, network)
- HDFS cluster configuration

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 9000, 9866, 9870, 9864 are available
2. **Docker issues**: Check `docker ps` and container logs
3. **Network timeouts**: Increase wait times for slow systems
4. **Permission errors**: Ensure Docker has file system access

### Debug Commands

```bash
# Check container status
docker ps | grep hdfs

# View container logs
docker logs hdfs-namenode-filemanager-test
docker logs hdfs-datanode-filemanager-test

# Check HDFS status
docker exec hdfs-namenode-filemanager-test hdfs dfsadmin -report

# List test data
docker exec hdfs-namenode-filemanager-test hdfs dfs -ls -R /test-data
```

### Environment Variables

You can customize the test behavior:

```bash
# Change HDFS connection settings
export NAMENODE_URI="hdfs://localhost:9000"
export DATANODE_PORT=9866

# Adjust test timeouts
export TEST_TIMEOUT_SECONDS=120
```

## Architecture Notes

The integration tests demonstrate the complete HdfsTransferManager architecture:

1. **NameNodeClient**: Queries HDFS metadata and directory listings
2. **DataNodeClient**: Transfers block data using HDFS Data Transfer Protocol
3. **HdfsClient**: Combines NameNode and DataNode operations
4. **HdfsTransferManager**: Orchestrates parallel transfers with thread pools
5. **CompletedDirectoryDownload**: Provides completion tracking and metrics

This layered architecture allows for:
- **Separation of concerns**: Each layer has specific responsibilities
- **Testability**: Individual components can be tested in isolation
- **Flexibility**: Different transport and execution strategies
- **Observability**: Comprehensive metrics and logging at each level

## Integration with CI/CD

The tests can be integrated into CI/CD pipelines:

```yaml
# GitHub Actions example
- name: Run HDFS Integration Tests
  run: |
    cd valier-hdfs-file-manager
    ./run-integration-tests.sh
```

The tests are designed to be:
- **Self-contained**: No external HDFS cluster required
- **Repeatable**: Clean setup and teardown
- **Fast**: Complete in under 5 minutes
- **Reliable**: Robust error handling and cleanup
