package io.valier.hdfs.filemanager;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Builder;
import lombok.Value;

/**
 * Result of a directory download operation that provides access to completion status and allows
 * waiting for all file transfers to complete.
 *
 * <p>This class is similar to AWS Transfer Manager's completed transfer objects, providing both
 * synchronous and asynchronous access to transfer results.
 */
@Value
@Builder
public class CompletedDirectoryDownload {

  /** The HDFS directory path that was downloaded. */
  String hdfsDirectoryPath;

  /** The local directory path where files were downloaded. */
  Path localDirectoryPath;

  /** List of individual file download results. */
  List<FileDownloadResult> fileDownloadResults;

  /**
   * Future that completes when all file downloads are finished. Users can call join() on this
   * future to wait for completion.
   */
  CompletableFuture<Void> completionFuture;

  /** Total number of files that were scheduled for download. */
  int totalFileCount;

  /**
   * Blocks until all file downloads complete.
   *
   * @throws RuntimeException if any download failed
   */
  public void waitForCompletion() {
    completionFuture.join();
  }

  /**
   * Checks if all downloads have completed (successfully or with errors).
   *
   * @return true if all downloads are done, false if still in progress
   */
  public boolean isDone() {
    return completionFuture.isDone();
  }

  /**
   * Gets the number of successfully completed downloads.
   *
   * @return number of successful downloads
   */
  public long getSuccessfulDownloadCount() {
    return fileDownloadResults.stream().filter(FileDownloadResult::isSuccess).count();
  }

  /**
   * Gets the number of failed downloads.
   *
   * @return number of failed downloads
   */
  public long getFailedDownloadCount() {
    return fileDownloadResults.stream().filter(result -> !result.isSuccess()).count();
  }

  /** Result of an individual file download within the directory download. */
  @Value
  @Builder
  public static class FileDownloadResult {

    /** The HDFS file path that was downloaded. */
    String hdfsFilePath;

    /** The local file path where the file was saved. */
    Path localFilePath;

    /** Whether the download was successful. */
    boolean success;

    /** Exception that occurred during download, if any. */
    Throwable exception;

    /** Size of the downloaded file in bytes. */
    long fileSizeBytes;

    /** Time taken to download the file in milliseconds. */
    long downloadTimeMs;
  }
}
