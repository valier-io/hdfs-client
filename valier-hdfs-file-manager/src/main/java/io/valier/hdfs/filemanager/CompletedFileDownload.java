package io.valier.hdfs.filemanager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Builder;
import lombok.Value;

/**
 * Result object representing a completed or in-progress file download from HDFS.
 *
 * <p>This class provides access to the completion future that can be used to wait for the download
 * to complete and retrieve the final download result.
 *
 * <p>Example usage:
 *
 * <pre>
 * CompletedFileDownload download = transferManager.download(request, listener);
 *
 * // Wait for completion
 * download.waitForCompletion();
 *
 * // Check if successful
 * if (download.isSuccess()) {
 *     System.out.println("Downloaded " + download.getFileSizeBytes() + " bytes");
 * }
 * </pre>
 */
@Value
@Builder
public class CompletedFileDownload {

  /** The original download request. */
  DownloadRequest downloadRequest;

  /**
   * Future that completes when the file download is finished. Contains the final FileDownloadResult
   * with success status and metrics.
   */
  CompletableFuture<FileDownloadResult> completionFuture;

  /**
   * Waits for the download to complete and returns the result.
   *
   * @return FileDownloadResult with success status and metrics
   * @throws RuntimeException if the download fails or is interrupted
   */
  public FileDownloadResult waitForCompletion() {
    try {
      return completionFuture.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Download was interrupted", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Download failed", e.getCause());
    }
  }

  /**
   * Waits for the download to complete with a timeout and returns the result.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return FileDownloadResult with success status and metrics
   * @throws RuntimeException if the download fails, is interrupted, or times out
   */
  public FileDownloadResult waitForCompletion(long timeout, TimeUnit unit) {
    try {
      return completionFuture.get(timeout, unit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Download was interrupted", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Download failed", e.getCause());
    } catch (TimeoutException e) {
      throw new RuntimeException("Download timed out", e);
    }
  }

  /**
   * Returns true if the download has completed (successfully or with failure).
   *
   * @return true if the download is done, false otherwise
   */
  public boolean isDone() {
    return completionFuture.isDone();
  }

  /**
   * Returns true if the download completed successfully. Only valid if isDone() returns true.
   *
   * @return true if the download was successful, false if it failed or is still in progress
   */
  public boolean isSuccess() {
    if (!completionFuture.isDone()) {
      return false;
    }
    try {
      FileDownloadResult result = completionFuture.get();
      return result.isSuccess();
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Returns the size of the downloaded file in bytes. Only valid if the download completed
   * successfully.
   *
   * @return file size in bytes, or 0 if download failed or is still in progress
   */
  public long getFileSizeBytes() {
    if (!completionFuture.isDone()) {
      return 0;
    }
    try {
      FileDownloadResult result = completionFuture.get();
      return result.getFileSizeBytes();
    } catch (Exception e) {
      return 0;
    }
  }

  /**
   * Returns the download time in milliseconds. Only valid if the download has completed
   * (successfully or with failure).
   *
   * @return download time in milliseconds, or 0 if still in progress
   */
  public long getDownloadTimeMs() {
    if (!completionFuture.isDone()) {
      return 0;
    }
    try {
      FileDownloadResult result = completionFuture.get();
      return result.getDownloadTimeMs();
    } catch (Exception e) {
      return 0;
    }
  }

  /** Result object containing download metrics and success status. */
  @Value
  @Builder
  public static class FileDownloadResult {

    /** The original download request. */
    DownloadRequest downloadRequest;

    /** Whether the download completed successfully. */
    boolean success;

    /** Exception that occurred during download, if any. */
    Exception exception;

    /** Size of the downloaded file in bytes. */
    long fileSizeBytes;

    /** Time taken to download the file in milliseconds. */
    long downloadTimeMs;
  }
}
