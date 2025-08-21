package io.valier.hdfs.filemanager;

import java.io.IOException;

/**
 * High-level file transfer utility for HDFS that provides parallel directory and file transfer
 * operations.
 *
 * <p>The HdfsTransferManager uses a configurable thread pool to perform parallel file transfers,
 * improving performance for bulk operations.
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
public interface HdfsTransferManager extends AutoCloseable {

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
  CompletedDirectoryDownload downloadDirectory(DownloadDirectoryRequest request) throws IOException;

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
  CompletedDirectoryUpload uploadDirectory(UploadDirectoryRequest request) throws IOException;

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
  CompletedFileDownload download(DownloadRequest request);

  /**
   * Uploads a single file from a local path to HDFS with optional progress tracking.
   *
   * <p>This method uploads a single file to HDFS and provides progress notifications through the
   * UploadListener included in the request (if provided). The listener is notified when the upload
   * starts, progresses (on each write operation), completes successfully, or fails.
   *
   * @param request The upload request containing source and destination file paths, and optional
   *     listener
   * @return CompletedFileUpload with a future that can be used to wait for completion
   */
  CompletedFileUpload upload(UploadRequest request);

  @Override
  void close() throws IOException;
}
