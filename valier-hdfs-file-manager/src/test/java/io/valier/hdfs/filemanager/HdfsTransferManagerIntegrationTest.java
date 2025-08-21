package io.valier.hdfs.filemanager;

import static org.junit.Assert.*;

import io.valier.hdfs.client.DefaultHdfsClient;
import io.valier.hdfs.client.HdfsClient;
import io.valier.hdfs.crt.HdfsPaths;
import io.valier.hdfs.nn.DefaultNameNodeClient;
import io.valier.hdfs.nn.NameNodeClient;
import io.valier.hdfs.nn.auth.SimpleUserInformation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test for HdfsTransferManager that demonstrates parallel directory downloads from an
 * HDFS cluster running in Docker containers.
 *
 * <p>This test showcases: - Parallel file downloads using configurable thread pools - Thread pool
 * execution with different concurrency levels - Transfer metrics and error handling - Proper
 * resource management
 *
 * <p>Prerequisites: - Docker containers running HDFS cluster (NameNode + DataNode) - Test data
 * populated in HDFS directories - Network access to HDFS services on localhost
 */
public class HdfsTransferManagerIntegrationTest {

  private static final String NAMENODE_URI = "hdfs://localhost:9000";
  private static final String TEST_HDFS_DIR = "/test-data";
  private static final Path LOCAL_TEST_DIR = Paths.get("target/test-downloads");

  private HdfsClient hdfsClient;
  private Path tempDownloadDir;

  @Before
  public void setUp() throws IOException {
    // Create NameNode client
    NameNodeClient nameNodeClient =
        DefaultNameNodeClient.builder()
            .userInformationProvider(SimpleUserInformation::hdfsUser)
            .nameNodeUri(NAMENODE_URI)
            .build();

    // Create HDFS client with local mode for containerized testing
    hdfsClient =
        DefaultHdfsClient.builder()
            .nameNodeClient(nameNodeClient)
            .localMode(true) // Enable local mode for Docker integration tests
            .replicationFactor(1) // Use replication factor of 1 for testing
            .build();

    // Create temporary local directory for downloads
    tempDownloadDir = Files.createTempDirectory("hdfs-transfer-test");

    // Clean up any existing test downloads
    if (Files.exists(LOCAL_TEST_DIR)) {
      deleteDirectory(LOCAL_TEST_DIR);
    }
    Files.createDirectories(LOCAL_TEST_DIR);
  }

  @After
  public void tearDown() throws IOException {
    // Clean up temporary directories
    if (tempDownloadDir != null && Files.exists(tempDownloadDir)) {
      deleteDirectory(tempDownloadDir);
    }

    if (Files.exists(LOCAL_TEST_DIR)) {
      deleteDirectory(LOCAL_TEST_DIR);
    }
  }

  @Test
  public void testSingleThreadDirectoryDownload() throws IOException {
    System.out.println("=== Testing Single Thread Directory Download (threadPoolSize=1) ===");

    // Create transfer manager with single thread execution
    try (HdfsTransferManager transferManager =
        DefaultHdfsTransferManager.builder()
            .hdfsClient(hdfsClient)
            .threadPoolSize(1) // Use single thread for sequential execution
            .build()) {

      Path localDir = LOCAL_TEST_DIR.resolve("single-thread-download");

      // Create download request
      DownloadDirectoryRequest request =
          DownloadDirectoryRequest.builder()
              .hdfsDirectoryPath(TEST_HDFS_DIR)
              .localDirectoryPath(localDir)
              .build();

      // Start download
      long startTime = System.nanoTime();
      CompletedDirectoryDownload download = transferManager.downloadDirectory(request);

      // Wait for completion
      download.waitForCompletion();
      long endTime = System.nanoTime();

      // Verify results
      assertTrue("Download should be completed", download.isDone());
      assertTrue("Should have downloaded files", download.getTotalFileCount() > 0);
      assertEquals(
          "All files should succeed",
          download.getTotalFileCount(),
          download.getSuccessfulDownloadCount());
      assertEquals("No files should fail", 0, download.getFailedDownloadCount());

      // Print metrics
      long totalTimeMs = (endTime - startTime) / 1_000_000;
      System.out.println("Single Thread Download Results:");
      System.out.println("  Total files: " + download.getTotalFileCount());
      System.out.println("  Successful: " + download.getSuccessfulDownloadCount());
      System.out.println("  Failed: " + download.getFailedDownloadCount());
      System.out.println("  Total time: " + totalTimeMs + " ms");

      // Verify files were downloaded
      assertTrue("Local directory should exist", Files.exists(localDir));
      verifyDownloadedFiles(download, localDir);
    }
  }

  @Test
  public void testParallelDirectoryDownload() throws IOException {
    System.out.println("\n=== Testing Parallel Directory Download (threadPoolSize=4) ===");

    // Create transfer manager with parallel execution
    try (HdfsTransferManager transferManager =
        DefaultHdfsTransferManager.builder()
            .hdfsClient(hdfsClient)
            .threadPoolSize(4) // Use 4 parallel threads
            .build()) {

      Path localDir = LOCAL_TEST_DIR.resolve("parallel-download");

      // Create download request
      DownloadDirectoryRequest request =
          DownloadDirectoryRequest.builder()
              .hdfsDirectoryPath(TEST_HDFS_DIR)
              .localDirectoryPath(localDir)
              .build();

      // Start download
      long startTime = System.nanoTime();
      CompletedDirectoryDownload download = transferManager.downloadDirectory(request);

      // Wait for completion
      download.waitForCompletion();
      long endTime = System.nanoTime();

      // Verify results
      assertTrue("Download should be completed", download.isDone());
      assertTrue("Should have downloaded files", download.getTotalFileCount() > 0);
      assertEquals(
          "All files should succeed",
          download.getTotalFileCount(),
          download.getSuccessfulDownloadCount());
      assertEquals("No files should fail", 0, download.getFailedDownloadCount());

      // Print metrics
      long totalTimeMs = (endTime - startTime) / 1_000_000;
      System.out.println("Parallel Download Results:");
      System.out.println("  Total files: " + download.getTotalFileCount());
      System.out.println("  Successful: " + download.getSuccessfulDownloadCount());
      System.out.println("  Failed: " + download.getFailedDownloadCount());
      System.out.println("  Total time: " + totalTimeMs + " ms");

      // Verify files were downloaded
      assertTrue("Local directory should exist", Files.exists(localDir));
      verifyDownloadedFiles(download, localDir);

      // Print individual file metrics
      System.out.println("Individual File Download Times:");
      for (CompletedDirectoryDownload.FileDownloadResult result :
          download.getFileDownloadResults()) {
        if (result.isSuccess()) {
          System.out.printf(
              "  %s: %d bytes in %d ms%n",
              result.getLocalFilePath().getFileName(),
              result.getFileSizeBytes(),
              result.getDownloadTimeMs());
        }
      }
    }
  }

  @Test
  public void testHighConcurrencyDownload() throws IOException {
    System.out.println("\n=== Testing High Concurrency Download (threadPoolSize=10) ===");

    // Create transfer manager with high concurrency
    try (HdfsTransferManager transferManager =
        DefaultHdfsTransferManager.builder()
            .hdfsClient(hdfsClient)
            .threadPoolSize(10) // Use 10 parallel threads
            .build()) {

      Path localDir = LOCAL_TEST_DIR.resolve("high-concurrency-download");

      // Create download request
      DownloadDirectoryRequest request =
          DownloadDirectoryRequest.builder()
              .hdfsDirectoryPath(TEST_HDFS_DIR)
              .localDirectoryPath(localDir)
              .build();

      // Start download
      long startTime = System.nanoTime();
      CompletedDirectoryDownload download = transferManager.downloadDirectory(request);

      // Wait for completion
      download.waitForCompletion();
      long endTime = System.nanoTime();

      // Verify results
      assertTrue("Download should be completed", download.isDone());
      assertEquals(
          "All files should succeed",
          download.getTotalFileCount(),
          download.getSuccessfulDownloadCount());

      // Print performance metrics
      long totalTimeMs = (endTime - startTime) / 1_000_000;
      long totalBytes =
          download.getFileDownloadResults().stream()
              .mapToLong(CompletedDirectoryDownload.FileDownloadResult::getFileSizeBytes)
              .sum();

      System.out.println("High Concurrency Download Results:");
      System.out.println("  Total files: " + download.getTotalFileCount());
      System.out.println("  Total bytes: " + totalBytes);
      System.out.println("  Total time: " + totalTimeMs + " ms");
      System.out.println(
          "  Throughput: " + (totalBytes / 1024.0 / 1024.0 / (totalTimeMs / 1000.0)) + " MB/s");

      verifyDownloadedFiles(download, localDir);
    }
  }

  @Test
  public void testTransferManagerBuilder() {
    System.out.println("\n=== Testing Transfer Manager Builder Validation ===");

    // Test invalid thread pool size validation
    try {
      DefaultHdfsTransferManager.builder().hdfsClient(hdfsClient).threadPoolSize(0).build();
      fail("Should throw IllegalArgumentException for thread pool size < 1");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "Should mention minimum thread pool size",
          e.getMessage().contains("Thread pool size must be at least 1"));
      System.out.println("✓ Correctly rejected thread pool size < 1: " + e.getMessage());
    }

    // Test negative thread pool size validation
    try {
      DefaultHdfsTransferManager.builder().hdfsClient(hdfsClient).threadPoolSize(-1).build();
      fail("Should throw IllegalArgumentException for negative thread pool size");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "Should mention minimum thread pool size",
          e.getMessage().contains("Thread pool size must be at least 1"));
      System.out.println("✓ Correctly rejected negative thread pool size: " + e.getMessage());
    }

    // Test missing HDFS client
    try {
      DefaultHdfsTransferManager.builder().threadPoolSize(1).build();
      fail("Should require HDFS client");
    } catch (Exception e) {
      System.out.println("✓ Correctly required HDFS client: " + e.getMessage());
    }

    // Test valid configurations
    try (HdfsTransferManager manager1 =
            DefaultHdfsTransferManager.builder().hdfsClient(hdfsClient).threadPoolSize(1).build();
        HdfsTransferManager manager2 =
            DefaultHdfsTransferManager.builder().hdfsClient(hdfsClient).threadPoolSize(5).build()) {

      System.out.println("✓ Successfully created transfer managers with valid configurations");
    } catch (IOException e) {
      fail("Valid configurations should not throw exceptions: " + e.getMessage());
    }
  }

  @Test
  public void testAsyncCompletion() throws IOException {
    System.out.println("\n=== Testing Asynchronous Completion Handling ===");

    try (HdfsTransferManager transferManager =
        DefaultHdfsTransferManager.builder().hdfsClient(hdfsClient).threadPoolSize(3).build()) {

      Path localDir = LOCAL_TEST_DIR.resolve("async-test");

      // Create download request
      DownloadDirectoryRequest request =
          DownloadDirectoryRequest.builder()
              .hdfsDirectoryPath(TEST_HDFS_DIR)
              .localDirectoryPath(localDir)
              .build();

      // Start download
      CompletedDirectoryDownload download = transferManager.downloadDirectory(request);

      // Test isDone() before completion
      System.out.println("Download started, isDone(): " + download.isDone());

      // Use completionFuture for async handling
      download
          .getCompletionFuture()
          .thenRun(
              () -> {
                System.out.println("✓ Async completion callback executed");
                System.out.println(
                    "  Final status - Success: "
                        + download.getSuccessfulDownloadCount()
                        + ", Failed: "
                        + download.getFailedDownloadCount());
              });

      // Wait for completion
      download.waitForCompletion();

      assertTrue("Download should be completed", download.isDone());
      System.out.println("✓ Download completed successfully");
    }
  }

  @Test
  public void testDirectoryUpload() throws IOException, NoSuchAlgorithmException {
    System.out.println("\n=== Testing Directory Upload ===");

    try (HdfsTransferManager transferManager =
        DefaultHdfsTransferManager.builder().hdfsClient(hdfsClient).threadPoolSize(1).build()) {

      // Create temporary local directory with test files
      Path localTestDir = tempDownloadDir.resolve("upload-test");
      Files.createDirectories(localTestDir);

      // Create multiple test files with different sizes
      Path smallFile = localTestDir.resolve("small-file.txt");
      Path mediumFile = localTestDir.resolve("medium-file.bin");
      Path largeFile = localTestDir.resolve("large-file.data");

      // Create test files with predictable content
      Files.write(smallFile, "Hello HDFS Upload Test!".getBytes());

      Random random = new Random(54321L); // Fixed seed
      byte[] mediumData = new byte[64 * 1024]; // 64KB
      random.nextBytes(mediumData);
      Files.write(mediumFile, mediumData);

      byte[] largeData = new byte[512 * 1024 * 2 * 516]; // 512KB
      random.nextBytes(largeData);
      Files.write(largeFile, largeData);

      System.out.println("Created test files:");
      System.out.println("  Small file: " + Files.size(smallFile) + " bytes");
      System.out.println("  Medium file: " + Files.size(mediumFile) + " bytes");
      System.out.println("  Large file: " + Files.size(largeFile) + " bytes");

      // Calculate checksums for verification
      String smallChecksum = calculateMD5(smallFile);
      String mediumChecksum = calculateMD5(mediumFile);
      String largeChecksum = calculateMD5(largeFile);

      // Upload directory to HDFS
      String hdfsUploadDir =
          "/test-data/test-upload-dir/" + ThreadLocalRandom.current().nextInt(10000);

      System.out.println("  Local source directory: " + localTestDir);
      System.out.println("  HDFS upload directory: " + hdfsUploadDir);

      UploadDirectoryRequest uploadRequest =
          UploadDirectoryRequest.builder()
              .localDirectoryPath(localTestDir)
              .hdfsDirectoryPath(hdfsUploadDir)
              .build();

      long uploadStart = System.nanoTime();
      CompletedDirectoryUpload upload = transferManager.uploadDirectory(uploadRequest);

      // Wait for upload completion
      upload.waitForCompletion();
      long uploadEnd = System.nanoTime();

      // Verify upload results
      assertTrue("Upload should be completed", upload.isDone());
      assertEquals("Should upload 3 files", 3, upload.getTotalFileCount());
      assertEquals("All files should succeed", 3, upload.getSuccessfulUploadCount());
      assertEquals("No files should fail", 0, upload.getFailedUploadCount());

      long uploadTimeMs = (uploadEnd - uploadStart) / 1_000_000;
      long totalBytes =
          upload.getFileUploadResults().stream()
              .mapToLong(CompletedDirectoryUpload.FileUploadResult::getFileSizeBytes)
              .sum();

      System.out.println("Directory Upload Results:");
      System.out.println("  Total files: " + upload.getTotalFileCount());
      System.out.println("  Successful: " + upload.getSuccessfulUploadCount());
      System.out.println("  Failed: " + upload.getFailedUploadCount());
      System.out.println("  Total bytes: " + totalBytes);
      System.out.println("  Total time: " + uploadTimeMs + " ms");
      System.out.println(
          "  Throughput: " + (totalBytes / 1024.0 / 1024.0 / (uploadTimeMs / 1000.0)) + " MB/s");

      // Print individual file metrics
      System.out.println("Individual File Upload Times:");
      for (CompletedDirectoryUpload.FileUploadResult result : upload.getFileUploadResults()) {
        if (result.isSuccess()) {
          System.out.printf(
              "  %s: %d bytes in %d ms%n",
              result.getLocalFilePath().getFileName(),
              result.getFileSizeBytes(),
              result.getUploadTimeMs());
        }
      }

      // Verify files exist in HDFS by downloading them back
      Path downloadDir = tempDownloadDir.resolve("verify-download");

      System.out.println("  Local verify directory: " + downloadDir);

      DownloadDirectoryRequest downloadRequest =
          DownloadDirectoryRequest.builder()
              .hdfsDirectoryPath(hdfsUploadDir)
              .localDirectoryPath(downloadDir)
              .build();

      CompletedDirectoryDownload download = transferManager.downloadDirectory(downloadRequest);
      download.waitForCompletion();

      // Verify download results
      assertTrue("Download should be completed", download.isDone());
      assertEquals(
          "Should download same number of files",
          upload.getTotalFileCount(),
          download.getTotalFileCount());
      assertEquals(
          "All downloads should succeed",
          download.getTotalFileCount(),
          download.getSuccessfulDownloadCount());

      // Verify checksums match original files
      Path downloadedSmall = downloadDir.resolve("small-file.txt");
      Path downloadedMedium = downloadDir.resolve("medium-file.bin");
      Path downloadedLarge = downloadDir.resolve("large-file.data");

      assertTrue("Small file should be downloaded", Files.exists(downloadedSmall));
      assertTrue("Medium file should be downloaded", Files.exists(downloadedMedium));
      assertTrue("Large file should be downloaded", Files.exists(downloadedLarge));

      assertEquals(
          "Small file checksum should match", smallChecksum, calculateMD5(downloadedSmall));
      assertEquals(
          "Medium file checksum should match", mediumChecksum, calculateMD5(downloadedMedium));
      assertEquals(
          "Large file checksum should match", largeChecksum, calculateMD5(downloadedLarge));

      // Clean up HDFS test files using the HDFS client
      try {
        // Delete each uploaded file individually
        for (CompletedDirectoryUpload.FileUploadResult result : upload.getFileUploadResults()) {
          if (result.isSuccess()) {
            String hdfsFilePath =
                HdfsPaths.get(hdfsUploadDir, result.getLocalFilePath().getFileName().toString());
            hdfsClient.deleteIfExists(hdfsFilePath);
            System.out.println("✓ Deleted HDFS file: " + hdfsFilePath);
          }
        }

        // Delete the empty directory last
        hdfsClient.deleteIfExists(hdfsUploadDir);
        System.out.println("✓ Cleaned up HDFS test directory: " + hdfsUploadDir);
      } catch (Exception e) {
        System.err.println("Warning: Failed to clean up HDFS test directory: " + e.getMessage());
      }
    }
    System.out.println("✓ Directory upload successful - all files verified");
  }

  @Test
  public void testUploadDownloadRoundTrip() throws IOException, NoSuchAlgorithmException {
    System.out.println("\n=== Testing Upload/Download Round Trip ===");

    try (HdfsTransferManager transferManager =
        DefaultHdfsTransferManager.builder().hdfsClient(hdfsClient).threadPoolSize(2).build()) {

      // Create temporary file with 1MB of predictably random data
      Path originalFile = tempDownloadDir.resolve("test-data-1mb.bin");
      Path downloadedFile = tempDownloadDir.resolve("test-data-1mb-downloaded.bin");
      String hdfsFilePath =
          String.format(
              "/test-data/test-data-1mb-%d.bin", ThreadLocalRandom.current().nextInt(10000));

      // Generate 1MB of predictable random data using fixed seed
      Random random = new Random(12345L); // Fixed seed for predictable data
      byte[] data = new byte[128 * 1024 * 1024 * 2]; // 1MB
      random.nextBytes(data);
      Files.write(originalFile, data);

      System.out.println(
          "Created test file: " + originalFile + " (" + Files.size(originalFile) + " bytes)");
      System.out.println("Target test file: " + hdfsFilePath);

      // Calculate MD5 checksum of original file
      String originalChecksum = calculateMD5(originalFile);
      System.out.println("Original file MD5: " + originalChecksum);

      // Upload the file to HDFS
      UploadRequest uploadRequest =
          UploadRequest.builder().localFilePath(originalFile).hdfsFilePath(hdfsFilePath).build();

      long uploadStart = System.nanoTime();
      CompletedFileUpload upload = transferManager.upload(uploadRequest);

      // Wait for upload completion
      CompletedFileUpload.FileUploadResult uploadResult = upload.waitForCompletion();
      long uploadEnd = System.nanoTime();

      // Verify upload success
      assertTrue("Upload should succeed", uploadResult.isSuccess());
      assertNull("Upload should have no exception", uploadResult.getException());
      assertEquals(
          "Upload size should match original",
          Files.size(originalFile),
          uploadResult.getFileSizeBytes());

      long uploadTimeMs = (uploadEnd - uploadStart) / 1_000_000;
      System.out.println("Upload completed successfully:");
      System.out.println("  File size: " + uploadResult.getFileSizeBytes() + " bytes");
      System.out.println("  Upload time: " + uploadTimeMs + " ms");
      System.out.println(
          "  Throughput: "
              + (uploadResult.getFileSizeBytes() / 1024.0 / 1024.0 / (uploadTimeMs / 1000.0))
              + " MB/s");

      // Download the file back from HDFS
      DownloadRequest downloadRequest =
          DownloadRequest.builder()
              .hdfsFilePath(hdfsFilePath)
              .localFilePath(downloadedFile)
              .build();

      long downloadStart = System.nanoTime();
      CompletedFileDownload download = transferManager.download(downloadRequest);

      // Wait for download completion
      CompletedFileDownload.FileDownloadResult downloadResult = download.waitForCompletion();
      long downloadEnd = System.nanoTime();

      // Verify download success
      assertTrue("Download should succeed", downloadResult.isSuccess());
      assertNull("Download should have no exception", downloadResult.getException());
      assertTrue("Downloaded file should exist", Files.exists(downloadedFile));

      long downloadTimeMs = (downloadEnd - downloadStart) / 1_000_000;
      System.out.println("Download completed successfully:");
      System.out.println("  File size: " + downloadResult.getFileSizeBytes() + " bytes");
      System.out.println("  Download time: " + downloadTimeMs + " ms");
      System.out.println(
          "  Throughput: "
              + (downloadResult.getFileSizeBytes() / 1024.0 / 1024.0 / (downloadTimeMs / 1000.0))
              + " MB/s");

      // Verify file sizes match
      long originalSize = Files.size(originalFile);
      long downloadedSize = Files.size(downloadedFile);
      assertEquals("File sizes should match", originalSize, downloadedSize);
      assertEquals(
          "Upload result size should match", originalSize, uploadResult.getFileSizeBytes());
      assertEquals(
          "Download result size should match", originalSize, downloadResult.getFileSizeBytes());

      // Verify checksums match
      String downloadedChecksum = calculateMD5(downloadedFile);
      System.out.println("Downloaded file MD5: " + downloadedChecksum);
      assertEquals("File checksums should match", originalChecksum, downloadedChecksum);

      System.out.println("✓ Upload/Download round trip successful - files are identical");
      System.out.println("  Original file:  " + originalFile + " (" + originalSize + " bytes)");
      System.out.println(
          "  Downloaded file: " + downloadedFile + " (" + downloadedSize + " bytes)");
      System.out.println("  Total round trip time: " + (uploadTimeMs + downloadTimeMs) + " ms");
    }
  }

  /** Verifies that downloaded files match expectations. */
  private void verifyDownloadedFiles(CompletedDirectoryDownload download, Path localDir)
      throws IOException {
    // Verify local directory exists and contains files
    assertTrue("Local download directory should exist", Files.exists(localDir));

    // Verify each successful download result
    for (CompletedDirectoryDownload.FileDownloadResult result : download.getFileDownloadResults()) {
      if (result.isSuccess()) {
        assertTrue(
            "Downloaded file should exist: " + result.getLocalFilePath(),
            Files.exists(result.getLocalFilePath()));
        assertTrue(
            "Downloaded file should have content: " + result.getLocalFilePath(),
            Files.size(result.getLocalFilePath()) > 0);
        assertEquals(
            "File size should match reported size",
            Files.size(result.getLocalFilePath()),
            result.getFileSizeBytes());
      } else {
        System.err.println(
            "Failed download: "
                + result.getHdfsFilePath()
                + " - "
                + result.getException().getMessage());
      }
    }

    System.out.println("✓ All downloaded files verified successfully");
  }

  /** Calculates the MD5 checksum of a file. */
  private String calculateMD5(Path filePath) throws IOException, NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] fileBytes = Files.readAllBytes(filePath);
    byte[] hashBytes = md.digest(fileBytes);

    // Convert hash bytes to hex string
    StringBuilder sb = new StringBuilder();
    for (byte b : hashBytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  /** Recursively deletes a directory and all its contents. */
  private void deleteDirectory(Path directory) throws IOException {
    if (Files.exists(directory)) {
      Files.walk(directory)
          .sorted(Comparator.reverseOrder()) // Delete files before directories
          .forEach(
              path -> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  System.err.println("Failed to delete: " + path + " - " + e.getMessage());
                }
              });
    }
  }
}
