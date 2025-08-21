package io.valier.hdfs.filemanager;

import io.valier.hdfs.client.HdfsClient;
import io.valier.hdfs.filemanager.listener.DownloadListener;
import io.valier.hdfs.filemanager.listener.UploadListener;
import io.valier.hdfs.nn.HdfsFileSummary;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation of HdfsTransferManager that provides parallel directory and file transfer
 * operations.
 *
 * <p>The DefaultHdfsTransferManager uses a configurable thread pool to perform parallel file
 * transfers, improving performance for bulk operations.
 *
 * <p>Example usage:
 *
 * <pre>
 * HdfsTransferManager transferManager = DefaultHdfsTransferManager.builder()
 *     .hdfsClient(hdfsClient)
 *     .threadPoolSize(2)
 *     .build();
 *
 * CompletedDirectoryDownload download = transferManager.downloadDirectory(
 *     "/hdfs/path", Paths.get("/local/path"));
 *
 * download.waitForCompletion();
 * </pre>
 */
@Value
@Builder
@Slf4j
public class DefaultHdfsTransferManager implements HdfsTransferManager {

  /** HDFS client used for all file operations. */
  @NonNull HdfsClient hdfsClient;

  /** Number of threads in the internal thread pool for parallel transfers. */
  @Builder.Default int threadPoolSize = 1;

  /**
   * Thread pool executor for parallel file transfers. Created during build() based on
   * threadPoolSize configuration.
   */
  ExecutorService executorService;

  /**
   * Downloads an entire directory from HDFS to a local path.
   *
   * <p>This method lists all files in the specified HDFS directory and downloads them in parallel
   * using the configured thread pool. Only regular files are downloaded - subdirectories are
   * created but not recursively processed.
   *
   * @param request The download directory request containing source and destination paths
   * @return CompletedDirectoryDownload with a future that can be used to wait for completion
   * @throws IOException if there's an error accessing the HDFS directory or creating local
   *     directories
   */
  public CompletedDirectoryDownload downloadDirectory(DownloadDirectoryRequest request)
      throws IOException {

    String hdfsDirectoryPath = request.getHdfsDirectoryPath();
    Path localDirectoryPath = request.getLocalDirectoryPath();

    log.debug(
        "Starting directory download from HDFS path: {} to local path: {}",
        hdfsDirectoryPath,
        localDirectoryPath);

    // Ensure local directory exists
    Files.createDirectories(localDirectoryPath);

    // List files in the HDFS directory
    List<HdfsFileSummary> filesToDownload = new ArrayList<>();
    try (Stream<HdfsFileSummary> files = hdfsClient.list(hdfsDirectoryPath)) {
      files.filter(HdfsFileSummary::isFile).forEach(filesToDownload::add);
    }

    log.debug(
        "Found {} files to download from directory: {}", filesToDownload.size(), hdfsDirectoryPath);

    // Create download tasks for each file
    List<CompletedDirectoryDownload.FileDownloadResult> results =
        new ArrayList<>(filesToDownload.size());
    List<CompletableFuture<CompletedDirectoryDownload.FileDownloadResult>> downloadFutures =
        new ArrayList<>(filesToDownload.size());

    // Submit download tasks for each file
    for (HdfsFileSummary file : filesToDownload) {
      String hdfsFilePath = hdfsDirectoryPath + "/" + file.getName();
      Path localFilePath = localDirectoryPath.resolve(file.getName());

      CompletableFuture<CompletedDirectoryDownload.FileDownloadResult> downloadFuture =
          CompletableFuture.supplyAsync(
              () -> downloadSingleFile(hdfsFilePath, localFilePath), executorService);

      downloadFutures.add(downloadFuture);
    }

    // Create a future that completes when all downloads are done
    CompletableFuture<Void> allDownloadsFuture =
        CompletableFuture.allOf(downloadFutures.toArray(new CompletableFuture[0]));

    // Populate the results list immediately with placeholders, then update as downloads complete
    for (CompletableFuture<CompletedDirectoryDownload.FileDownloadResult> future :
        downloadFutures) {
      future.thenAccept(results::add);
    }

    return CompletedDirectoryDownload.builder()
        .hdfsDirectoryPath(hdfsDirectoryPath)
        .localDirectoryPath(localDirectoryPath)
        .fileDownloadResults(results)
        .completionFuture(allDownloadsFuture)
        .totalFileCount(filesToDownload.size())
        .build();
  }

  /**
   * Downloads a single file from HDFS to a local path with optional progress tracking.
   *
   * <p>This method downloads a single file from HDFS and provides progress notifications through
   * the DownloadListener included in the request (if provided). The listener is notified when the
   * download starts, progresses (on each write operation), completes successfully, or fails.
   *
   * @param request The download request containing source and destination file paths, and optional
   *     listener
   * @return CompletedFileDownload with a future that can be used to wait for completion
   */
  public CompletedFileDownload download(DownloadRequest request) {

    // Create a CompletableFuture that will complete when the download is done
    CompletableFuture<CompletedFileDownload.FileDownloadResult> completionFuture =
        CompletableFuture.supplyAsync(
            () -> downloadSingleFileWithProgress(request), executorService);

    return CompletedFileDownload.builder()
        .downloadRequest(request)
        .completionFuture(completionFuture)
        .build();
  }

  /**
   * Uploads an entire directory from the local filesystem to HDFS.
   *
   * <p>This method lists all files in the specified local directory and uploads them in parallel
   * using the configured thread pool. Only regular files are uploaded - subdirectories are created
   * in HDFS but not recursively processed.
   *
   * @param request The upload directory request containing source and destination paths
   * @return CompletedDirectoryUpload with a future that can be used to wait for completion
   * @throws IOException if there's an error accessing the local directory or creating HDFS
   *     directories
   */
  public CompletedDirectoryUpload uploadDirectory(UploadDirectoryRequest request)
      throws IOException {

    Path localDirectoryPath = request.getLocalDirectoryPath();
    String hdfsDirectoryPath = request.getHdfsDirectoryPath();

    log.debug(
        "Starting directory upload from local path: {} to HDFS path: {}",
        localDirectoryPath,
        hdfsDirectoryPath);

    hdfsClient.createDirectories(hdfsDirectoryPath);

    // List files in the local directory
    List<Path> filesToUpload = new ArrayList<>();
    try (Stream<Path> files = Files.list(localDirectoryPath)) {
      files.filter(Files::isRegularFile).forEach(filesToUpload::add);
    }

    log.debug(
        "Found {} files to upload from directory: {}", filesToUpload.size(), localDirectoryPath);

    // Create upload tasks for each file
    List<CompletedDirectoryUpload.FileUploadResult> results = new ArrayList<>(filesToUpload.size());
    List<CompletableFuture<CompletedDirectoryUpload.FileUploadResult>> uploadFutures =
        new ArrayList<>(filesToUpload.size());

    // Submit upload tasks for each file
    for (Path localFile : filesToUpload) {
      String fileName = localFile.getFileName().toString();
      String hdfsFilePath = hdfsDirectoryPath + "/" + fileName;

      CompletableFuture<CompletedDirectoryUpload.FileUploadResult> uploadFuture =
          CompletableFuture.supplyAsync(
              () -> uploadSingleFile(localFile, hdfsFilePath), executorService);

      uploadFutures.add(uploadFuture);
    }

    // Create a future that completes when all uploads are done
    CompletableFuture<Void> allUploadsFuture =
        CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0]));

    // Populate the results list immediately with placeholders, then update as uploads complete
    for (CompletableFuture<CompletedDirectoryUpload.FileUploadResult> future : uploadFutures) {
      future.thenAccept(results::add);
    }

    return CompletedDirectoryUpload.builder()
        .localDirectoryPath(localDirectoryPath)
        .hdfsDirectoryPath(hdfsDirectoryPath)
        .fileUploadResults(results)
        .completionFuture(allUploadsFuture)
        .totalFileCount(filesToUpload.size())
        .build();
  }

  /**
   * Uploads a single file from a local path to HDFS with optional progress tracking.
   *
   * <p>This method uploads a single file to HDFS and provides progress notifications through the
   * UploadListener included in the request (if provided). The listener is notified when the upload
   * starts, progresses (on each read operation), completes successfully, or fails.
   *
   * @param request The upload request containing source and destination file paths, and optional
   *     listener
   * @return CompletedFileUpload with a future that can be used to wait for completion
   */
  public CompletedFileUpload upload(UploadRequest request) {

    // Create a CompletableFuture that will complete when the upload is done
    CompletableFuture<CompletedFileUpload.FileUploadResult> completionFuture =
        CompletableFuture.supplyAsync(() -> uploadSingleFileWithProgress(request), executorService);

    return CompletedFileUpload.builder()
        .uploadRequest(request)
        .completionFuture(completionFuture)
        .build();
  }

  /**
   * Uploads a single file from a local path to HDFS.
   *
   * @param localFilePath The local file path to upload
   * @param hdfsFilePath The HDFS file path where the file should be saved
   * @return FileUploadResult with success status and metrics
   */
  private CompletedDirectoryUpload.FileUploadResult uploadSingleFile(
      Path localFilePath, String hdfsFilePath) {
    long startTime = System.nanoTime();

    try {
      log.debug("Uploading file: {} to {}", localFilePath, hdfsFilePath);

      // Check if local file exists
      if (!Files.exists(localFilePath)) {
        throw new IOException("Local file does not exist: " + localFilePath);
      }

      // Upload the file using the HDFS client with InputStream
      try (InputStream in = Files.newInputStream(localFilePath)) {
        hdfsClient.copy(hdfsFilePath, in);
      }

      long endTime = System.nanoTime();
      long fileSize = Files.size(localFilePath);
      long uploadTimeNs = endTime - startTime;
      long uploadTimeMs = TimeUnit.NANOSECONDS.toMillis(uploadTimeNs);

      log.debug(
          "Successfully uploaded file: {} ({} bytes in {} ms)",
          localFilePath,
          fileSize,
          uploadTimeMs);

      return CompletedDirectoryUpload.FileUploadResult.builder()
          .localFilePath(localFilePath)
          .hdfsFilePath(hdfsFilePath)
          .success(true)
          .exception(null)
          .fileSizeBytes(fileSize)
          .uploadTimeMs(uploadTimeMs)
          .build();

    } catch (Exception e) {
      long endTime = System.nanoTime();
      long uploadTimeNs = endTime - startTime;
      long uploadTimeMs = TimeUnit.NANOSECONDS.toMillis(uploadTimeNs);

      log.debug("Failed to upload file: {} to {}", localFilePath, hdfsFilePath, e);

      return CompletedDirectoryUpload.FileUploadResult.builder()
          .localFilePath(localFilePath)
          .hdfsFilePath(hdfsFilePath)
          .success(false)
          .exception(e)
          .fileSizeBytes(0)
          .uploadTimeMs(uploadTimeMs)
          .build();
    }
  }

  /**
   * Downloads a single file from HDFS to a local path.
   *
   * @param hdfsFilePath The HDFS file path to download
   * @param localFilePath The local file path where the file should be saved
   * @return FileDownloadResult with success status and metrics
   */
  private CompletedDirectoryDownload.FileDownloadResult downloadSingleFile(
      String hdfsFilePath, Path localFilePath) {
    long startTime = System.nanoTime();

    try {
      log.debug("Downloading file: {} to {}", hdfsFilePath, localFilePath);

      // Ensure parent directory exists
      Files.createDirectories(localFilePath.getParent());

      // Download the file using the HDFS client with OutputStream
      try (OutputStream out = Files.newOutputStream(localFilePath)) {
        hdfsClient.copy(hdfsFilePath, out);
      }

      long endTime = System.nanoTime();
      long fileSize = Files.size(localFilePath);
      long downloadTimeNs = endTime - startTime;
      long downloadTimeMs = TimeUnit.NANOSECONDS.toMillis(downloadTimeNs);

      log.debug(
          "Successfully downloaded file: {} ({} bytes in {} ms)",
          hdfsFilePath,
          fileSize,
          downloadTimeMs);

      return CompletedDirectoryDownload.FileDownloadResult.builder()
          .hdfsFilePath(hdfsFilePath)
          .localFilePath(localFilePath)
          .success(true)
          .exception(null)
          .fileSizeBytes(fileSize)
          .downloadTimeMs(downloadTimeMs)
          .build();

    } catch (Exception e) {
      long endTime = System.nanoTime();
      long downloadTimeNs = endTime - startTime;
      long downloadTimeMs = TimeUnit.NANOSECONDS.toMillis(downloadTimeNs);

      log.debug("Failed to download file: {} to {}", hdfsFilePath, localFilePath, e);

      return CompletedDirectoryDownload.FileDownloadResult.builder()
          .hdfsFilePath(hdfsFilePath)
          .localFilePath(localFilePath)
          .success(false)
          .exception(e)
          .fileSizeBytes(0)
          .downloadTimeMs(downloadTimeMs)
          .build();
    }
  }

  /**
   * Downloads a single file from HDFS to a local path with progress tracking.
   *
   * @param request The download request containing source and destination paths and optional
   *     listener
   * @return CompletedFileDownload.FileDownloadResult with success status and metrics
   */
  private CompletedFileDownload.FileDownloadResult downloadSingleFileWithProgress(
      DownloadRequest request) {
    long startTime = System.nanoTime();
    String hdfsFilePath = request.getHdfsFilePath();
    Path localFilePath = request.getLocalFilePath();
    DownloadListener downloadListener = request.getDownloadListener();

    // Notify listener that download is starting
    if (downloadListener != null) {
      downloadListener.onDownloadStarted(request);
    }

    try {
      log.debug("Downloading file: {} to {}", hdfsFilePath, localFilePath);

      // Ensure parent directory exists
      Files.createDirectories(localFilePath.getParent());

      // Download the file using the HDFS client with OutputStream
      try (OutputStream baseOut = Files.newOutputStream(localFilePath)) {
        OutputStream out = baseOut;

        // Wrap with progress tracking if listener is provided
        if (downloadListener != null) {
          out = new ProgressTrackingOutputStream(baseOut, request, downloadListener);
        }

        hdfsClient.copy(hdfsFilePath, out);
      }

      long endTime = System.nanoTime();
      long fileSize = Files.size(localFilePath);
      long downloadTimeNs = endTime - startTime;
      long downloadTimeMs = TimeUnit.NANOSECONDS.toMillis(downloadTimeNs);

      log.debug(
          "Successfully downloaded file: {} ({} bytes in {} ms)",
          hdfsFilePath,
          fileSize,
          downloadTimeMs);

      // Notify listener that download completed successfully
      if (downloadListener != null) {
        downloadListener.onDownloadCompleted(request, fileSize, downloadTimeMs);
      }

      return CompletedFileDownload.FileDownloadResult.builder()
          .downloadRequest(request)
          .success(true)
          .exception(null)
          .fileSizeBytes(fileSize)
          .downloadTimeMs(downloadTimeMs)
          .build();

    } catch (Exception e) {
      long endTime = System.nanoTime();
      long downloadTimeNs = endTime - startTime;
      long downloadTimeMs = TimeUnit.NANOSECONDS.toMillis(downloadTimeNs);

      log.debug("Failed to download file: {} to {}", hdfsFilePath, localFilePath, e);

      // Notify listener that download failed
      if (downloadListener != null) {
        downloadListener.onDownloadFailed(request, e);
      }

      return CompletedFileDownload.FileDownloadResult.builder()
          .downloadRequest(request)
          .success(false)
          .exception(e)
          .fileSizeBytes(0)
          .downloadTimeMs(downloadTimeMs)
          .build();
    }
  }

  /**
   * Uploads a single file from a local path to HDFS with progress tracking.
   *
   * @param request The upload request containing source and destination paths and optional listener
   * @return CompletedFileUpload.FileUploadResult with success status and metrics
   */
  private CompletedFileUpload.FileUploadResult uploadSingleFileWithProgress(UploadRequest request) {
    Path localFilePath = request.getLocalFilePath();
    String hdfsFilePath = request.getHdfsFilePath();
    UploadListener uploadListener = request.getUploadListener();
    long startTime = System.nanoTime();

    // Notify listener that upload is starting
    if (uploadListener != null) {
      uploadListener.onUploadStarted(request);
    }

    try {
      log.debug("Uploading file: {} to {}", localFilePath, hdfsFilePath);

      // Check if local file exists
      if (!Files.exists(localFilePath)) {
        throw new IOException("Local file does not exist: " + localFilePath);
      }

      // Upload the file using the HDFS client with InputStream
      try (InputStream baseIn = Files.newInputStream(localFilePath)) {
        InputStream in = baseIn;

        // Wrap with progress tracking if listener is provided
        if (uploadListener != null) {
          in = new ProgressTrackingInputStream(baseIn, request, uploadListener);
        }

        hdfsClient.copy(hdfsFilePath, in);
      }

      long endTime = System.nanoTime();
      long fileSize = Files.size(localFilePath);
      long uploadTimeNs = endTime - startTime;
      long uploadTimeMs = TimeUnit.NANOSECONDS.toMillis(uploadTimeNs);

      log.debug(
          "Successfully uploaded file: {} ({} bytes in {} ms)",
          localFilePath,
          fileSize,
          uploadTimeMs);

      // Notify listener that upload completed successfully
      if (uploadListener != null) {
        uploadListener.onUploadCompleted(request, fileSize, uploadTimeMs);
      }

      return CompletedFileUpload.FileUploadResult.builder()
          .uploadRequest(request)
          .success(true)
          .exception(null)
          .fileSizeBytes(fileSize)
          .uploadTimeMs(uploadTimeMs)
          .build();

    } catch (Exception e) {
      long endTime = System.nanoTime();
      long uploadTimeNs = endTime - startTime;
      long uploadTimeMs = TimeUnit.NANOSECONDS.toMillis(uploadTimeNs);

      log.debug("Failed to upload file: {} to {}", localFilePath, hdfsFilePath, e);

      // Notify listener that upload failed
      if (uploadListener != null) {
        uploadListener.onUploadFailed(request, e);
      }

      return CompletedFileUpload.FileUploadResult.builder()
          .uploadRequest(request)
          .success(false)
          .exception(e)
          .fileSizeBytes(0)
          .uploadTimeMs(uploadTimeMs)
          .build();
    }
  }

  /**
   * Creates a builder for configuring and creating DefaultHdfsTransferManager instances.
   *
   * @return a new DefaultHdfsTransferManagerBuilder instance
   */
  public static DefaultHdfsTransferManagerBuilder builder() {
    return new CustomHdfsTransferManagerBuilder();
  }

  /** Custom builder implementation that creates the ExecutorService during build(). */
  private static class CustomHdfsTransferManagerBuilder extends DefaultHdfsTransferManagerBuilder {

    @Override
    public DefaultHdfsTransferManager build() {
      // Get values from the super builder
      DefaultHdfsTransferManager temp = super.build();

      // Validate threadPoolSize
      if (temp.threadPoolSize < 1) {
        throw new IllegalArgumentException(
            "Thread pool size must be at least 1: " + temp.threadPoolSize);
      }

      // Create thread pool with daemon threads
      ExecutorService executor =
          Executors.newFixedThreadPool(
              temp.threadPoolSize,
              r -> {
                Thread t = new Thread(r, "hdfs-transfer-" + System.nanoTime());
                t.setDaemon(true);
                return t;
              });
      log.debug("Created thread pool with {} threads for HDFS transfers", temp.threadPoolSize);

      // Create the instance with the configured executor
      return new DefaultHdfsTransferManager(temp.hdfsClient, temp.threadPoolSize, executor);
    }
  }

  /**
   * Shuts down the internal thread pool and releases resources. Should be called when the transfer
   * manager is no longer needed.
   */
  @Override
  public void close() {
    if (executorService != null && !executorService.isShutdown()) {
      log.debug("Shutting down HDFS transfer manager thread pool");
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
          log.debug("Thread pool did not terminate gracefully, forcing shutdown");
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        log.debug("Interrupted while waiting for thread pool termination");
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }
}
