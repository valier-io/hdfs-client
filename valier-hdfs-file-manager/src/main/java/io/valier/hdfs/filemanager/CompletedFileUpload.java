package io.valier.hdfs.filemanager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Builder;
import lombok.Value;

/**
 * Result object representing a completed or in-progress file upload to HDFS.
 *
 * <p>This class provides access to the completion future that can be used to wait for the upload to
 * complete and retrieve the final upload result.
 *
 * <p>Example usage:
 *
 * <pre>
 * CompletedFileUpload upload = transferManager.upload(request, listener);
 *
 * // Wait for completion
 * upload.waitForCompletion();
 *
 * // Check if successful
 * if (upload.isSuccess()) {
 *     System.out.println("Uploaded " + upload.getFileSizeBytes() + " bytes");
 * }
 * </pre>
 */
@Value
@Builder
public class CompletedFileUpload {

  /** The original upload request. */
  UploadRequest uploadRequest;

  /**
   * Future that completes when the file upload is finished. Contains the final FileUploadResult
   * with success status and metrics.
   */
  CompletableFuture<FileUploadResult> completionFuture;

  /**
   * Waits for the upload to complete and returns the result.
   *
   * @return FileUploadResult with success status and metrics
   * @throws RuntimeException if the upload fails or is interrupted
   */
  public FileUploadResult waitForCompletion() {
    try {
      return completionFuture.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Upload was interrupted", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Upload failed", e.getCause());
    }
  }

  /**
   * Waits for the upload to complete with a timeout and returns the result.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return FileUploadResult with success status and metrics
   * @throws RuntimeException if the upload fails, is interrupted, or times out
   */
  public FileUploadResult waitForCompletion(long timeout, TimeUnit unit) {
    try {
      return completionFuture.get(timeout, unit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Upload was interrupted", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Upload failed", e.getCause());
    } catch (TimeoutException e) {
      throw new RuntimeException("Upload timed out", e);
    }
  }

  /**
   * Returns true if the upload has completed (successfully or with failure).
   *
   * @return true if the upload is done, false otherwise
   */
  public boolean isDone() {
    return completionFuture.isDone();
  }

  /**
   * Returns true if the upload completed successfully. Only valid if isDone() returns true.
   *
   * @return true if the upload was successful, false if it failed or is still in progress
   */
  public boolean isSuccess() {
    if (!completionFuture.isDone()) {
      return false;
    }
    try {
      FileUploadResult result = completionFuture.get();
      return result.isSuccess();
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Returns the size of the uploaded file in bytes. Only valid if the upload completed
   * successfully.
   *
   * @return file size in bytes, or 0 if upload failed or is still in progress
   */
  public long getFileSizeBytes() {
    if (!completionFuture.isDone()) {
      return 0;
    }
    try {
      FileUploadResult result = completionFuture.get();
      return result.getFileSizeBytes();
    } catch (Exception e) {
      return 0;
    }
  }

  /**
   * Returns the upload time in milliseconds. Only valid if the upload has completed (successfully
   * or with failure).
   *
   * @return upload time in milliseconds, or 0 if still in progress
   */
  public long getUploadTimeMs() {
    if (!completionFuture.isDone()) {
      return 0;
    }
    try {
      FileUploadResult result = completionFuture.get();
      return result.getUploadTimeMs();
    } catch (Exception e) {
      return 0;
    }
  }

  /** Result object containing upload metrics and success status. */
  @Value
  @Builder
  public static class FileUploadResult {

    /** The original upload request. */
    UploadRequest uploadRequest;

    /** Whether the upload completed successfully. */
    boolean success;

    /** Exception that occurred during upload, if any. */
    Exception exception;

    /** Size of the uploaded file in bytes. */
    long fileSizeBytes;

    /** Time taken to upload the file in milliseconds. */
    long uploadTimeMs;
  }
}
