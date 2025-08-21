package io.valier.hdfs.filemanager;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Builder;
import lombok.Value;

/**
 * Result of a directory upload operation that provides access to completion status and allows
 * waiting for all file transfers to complete.
 *
 * <p>This class is similar to AWS Transfer Manager's completed transfer objects, providing both
 * synchronous and asynchronous access to transfer results.
 */
@Value
@Builder
public class CompletedDirectoryUpload {

  /** The local directory path that was uploaded. */
  Path localDirectoryPath;

  /** The HDFS directory path where files were uploaded. */
  String hdfsDirectoryPath;

  /** List of individual file upload results. */
  List<FileUploadResult> fileUploadResults;

  /**
   * Future that completes when all file uploads are finished. Users can call join() on this future
   * to wait for completion.
   */
  CompletableFuture<Void> completionFuture;

  /** Total number of files that were scheduled for upload. */
  int totalFileCount;

  /**
   * Blocks until all file uploads complete.
   *
   * @throws RuntimeException if any upload failed
   */
  public void waitForCompletion() {
    completionFuture.join();
  }

  /**
   * Checks if all uploads have completed (successfully or with errors).
   *
   * @return true if all uploads are done, false if still in progress
   */
  public boolean isDone() {
    return completionFuture.isDone();
  }

  /**
   * Gets the number of successfully completed uploads.
   *
   * @return number of successful uploads
   */
  public long getSuccessfulUploadCount() {
    return fileUploadResults.stream().filter(FileUploadResult::isSuccess).count();
  }

  /**
   * Gets the number of failed uploads.
   *
   * @return number of failed uploads
   */
  public long getFailedUploadCount() {
    return fileUploadResults.stream().filter(result -> !result.isSuccess()).count();
  }

  /** Result of an individual file upload within the directory upload. */
  @Value
  @Builder
  public static class FileUploadResult {

    /** The local file path that was uploaded. */
    Path localFilePath;

    /** The HDFS file path where the file was uploaded. */
    String hdfsFilePath;

    /** Whether the upload was successful. */
    boolean success;

    /** Exception that occurred during upload, if any. */
    Throwable exception;

    /** Size of the uploaded file in bytes. */
    long fileSizeBytes;

    /** Time taken to upload the file in milliseconds. */
    long uploadTimeMs;
  }
}
