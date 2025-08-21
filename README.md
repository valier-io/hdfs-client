# Valier HDFS Client Libraries

A comprehensive set of Java client libraries for Apache HDFS, providing low-level protocol implementations and high-level transfer utilities. This library addresses the core weaknesses of Hadoop's native client libraries by offering a modern, lightweight alternative with intuitive APIs aligned with JDK standards.

## Why Valier HDFS?

Traditional Hadoop client libraries suffer from several architectural problems:

- **Large disk footprint**: Full Hadoop distributions require hundreds of megabytes of JARs with complex dependency trees
- **Highly coupled**: Hadoop clients are tightly bound to specific Hadoop versions and configurations
- **Complicated APIs**: Legacy APIs are verbose, inconsistent, and don't follow modern Java conventions

Valier HDFS solves these problems by:

- **Minimal dependencies**: Lightweight libraries with only essential dependencies (protobuf)
- **Version independent**: Direct protocol implementation works across HDFS versions
- **Modern APIs**: Clean interfaces following JDK `Files` API patterns with proper exception handling
- **Modular design**: Use only what you need - NameNode client, DataNode client, or high-level transfer utilities

## Modules

- **valier-hdfs-crt**: Common runtime components and utilities
- **valier-hdfs-nn**: NameNode client for metadata operations
- **valier-hdfs-dn**: DataNode client for block data operations
- **valier-hdfs-client**: Combined HDFS client (NameNode + DataNode)
- **valier-hdfs-file-manager**: High-level file transfer utilities

## Building

### Prerequisites

This project uses the Hadoop source code as a git submodule to access protobuf definitions. Before building, initialize the submodule:

```bash
# Initialize and update the Hadoop submodule
git submodule update --init --recursive
```

### Build Commands

```bash
# Build without any integration tests
mvn clean install -DskipITs

# Build with integration tests (requires Docker)
mvn clean verify
```

## Integration Tests

Integration tests require Docker infrastructure and can be resource-intensive. They are automatically included in the `verify` phase but can be skipped:

```bash
# Skip integration tests during any Maven phase
mvn install -DskipITs
mvn verify -DskipITs

# Run only integration tests
mvn integration-test
```

### Manual Integration Testing

Each module with integration tests provides a dedicated script:

```bash
# NameNode client tests
cd valier-hdfs-nn
./run-integration-tests.sh

# Combined client tests
./run-integration-tests.sh

# File manager tests
cd valier-hdfs-file-manager
./run-integration-tests.sh
```

## Quick Start

### Basic HDFS Client Usage

```java
// Create HDFS client with modern builder pattern
HdfsClient client = HdfsClient.builder()
    .nameNodeUri("hdfs://namenode:9000")
    .userInformationProvider(() -> SimpleUserInformation.currentUser())
    .build();

// List files using JDK-style APIs
List<HdfsFileSummary> files = client.list("/user/data")
    .collect(Collectors.toList());

// Create directory (similar to Files.createDirectories)
HdfsFileSummary dir = client.createDirectories("/user/data/output");

// Read file attributes (similar to Files.readAttributes)
HdfsFileSummary fileInfo = client.readAttributes("/user/data/input.txt");

// Delete files (similar to Files.delete)
client.delete("/user/data/temp.txt");
boolean deleted = client.deleteIfExists("/user/data/maybe-exists.txt");
```

## Requirements

- Java 11+
- Maven 3.6+
- Docker (for integration tests only)

**Protocol Compatibility**: Built and tested against Hadoop 3.4.0, but should work with other HDFS versions due to direct protocol implementation.

## Current Limitations

This library is a best-effort replication of the HDFS DFS client functionality with some current limitations:

- **Security**: No support for encryption or strong authentication (Kerberos). Only simple authentication is supported.
- **Feature Coverage**: Not all HDFS features are implemented. Missing features compared to the full Hadoop client may include advanced file operations, extended attributes, and some administrative functions.
- **Robustness**: While functional, this implementation may not have the same level of robustness and edge case handling as the mature Hadoop reference client.
- **Directory Listing**: Limited to first 1000 entries per directory due to current paging implementation.
- **Testing**: Integration tests exist and may be helpful for manual testing, but they are not yet mature enough to be considered part of the normal testing lifecycle of the library.

These limitations will be addressed in future releases as the library matures.

## File Manager - High-Level Transfer Utilities

For bulk operations and advanced transfer scenarios, use the File Manager:

### Basic File Operations

```java
// Create transfer manager
HdfsTransferManager transferManager = HdfsTransferManager.builder()
    .hdfsClient(client)
    .build();

// Upload single file with progress tracking
UploadRequest uploadRequest = UploadRequest.builder()
    .source(Paths.get("/local/file.txt"))
    .destination("/hdfs/remote/file.txt")
    .build();

CompletedFileUpload result = transferManager.upload(uploadRequest);
result.waitForCompletion();
System.out.println("Uploaded " + result.getFileUploadResult().getFileSizeBytes() + " bytes");

// Download single file with logging
DownloadRequest downloadRequest = DownloadRequest.builder()
    .source("/hdfs/remote/file.txt")
    .destination(Paths.get("/local/downloaded-file.txt"))
    .downloadListener(LoggingDownloadListener.defaultInstance())
    .build();

CompletedFileDownload download = transferManager.download(downloadRequest);
download.waitForCompletion();
System.out.println("Downloaded to: " + download.getFileDownloadResult().getLocalFilePath());
```

### Directory Operations

```java
// Upload entire directory with custom listeners
UploadDirectoryRequest dirUpload = UploadDirectoryRequest.builder()
    .source(Paths.get("/local/directory"))
    .destination("/hdfs/remote/directory")
    .uploadListener(new LoggingUploadListener())
    .build();

CompletedDirectoryUpload result = transferManager.uploadDirectory(dirUpload);
result.waitForCompletion();
System.out.println("Uploaded " + result.getSuccessfulUploadCount() + " files");

// Download directory with progress tracking
DownloadDirectoryRequest dirDownload = DownloadDirectoryRequest.builder()
    .source("/hdfs/remote/directory")
    .destination(Paths.get("/local/download"))
    .downloadListener(LoggingDownloadListener.defaultInstance())
    .build();

CompletedDirectoryDownload download = transferManager.downloadDirectory(dirDownload);
download.waitForCompletion();
```

### Progress Monitoring

```java
// Custom upload listener for detailed monitoring
UploadListener customListener = new UploadListener() {
    @Override
    public void uploadStarted(UploadRequest request) {
        System.out.println("Starting upload: " + request.destination());
    }

    @Override
    public void bytesUploaded(UploadRequest request, long bytesUploaded, long totalBytes) {
        double percent = (bytesUploaded * 100.0) / totalBytes;
        System.out.printf("Upload progress: %.1f%% (%d/%d bytes)%n",
            percent, bytesUploaded, totalBytes);
    }

    @Override
    public void uploadCompleted(CompletedFileUpload result) {
        System.out.println("Upload completed: " + result.destination());
    }
};

UploadRequest request = UploadRequest.builder()
    .source(localFile)
    .destination(hdfsPath)
    .uploadListener(customListener)
    .build();
```

## Maven Lifecycle

- **Unit Tests**: Run automatically during `mvn test` and `mvn install`
- **Integration Tests**: Run automatically during `mvn verify` and `mvn integration-test` (unless skipped with `-DskipITs`)
- **Manual Scripts**: Run integration tests independently with full Docker lifecycle management
