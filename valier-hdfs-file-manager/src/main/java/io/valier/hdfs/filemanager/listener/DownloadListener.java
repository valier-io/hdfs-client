package io.valier.hdfs.filemanager.listener;

import io.valier.hdfs.filemanager.DownloadRequest;

/**
 * Listener interface for receiving notifications about file download lifecycle events.
 *
 * <p>This interface allows callers to track download progress and respond to various events during
 * the download process, including start, progress, completion, and failure.
 *
 * <p>All methods receive the DownloadRequest object as context for the caller to use.
 *
 * <p>Example usage:
 *
 * <pre>
 * DownloadListener listener = new DownloadListener() {
 *     {@code @Override}
 *     public void onDownloadStarted(DownloadRequest request) {
 *         System.out.println("Started downloading: " + request.getHdfsFilePath());
 *     }
 *
 *     {@code @Override}
 *     public void onDataDownloaded(DownloadRequest request, int bytesWritten, long totalBytesWritten) {
 *         System.out.println("Progress: " + totalBytesWritten + " bytes downloaded");
 *     }
 *
 *     {@code @Override}
 *     public void onDownloadCompleted(DownloadRequest request, long fileSizeBytes, long downloadTimeMs) {
 *         System.out.println("Completed: " + fileSizeBytes + " bytes in " + downloadTimeMs + "ms");
 *     }
 *
 *     {@code @Override}
 *     public void onDownloadFailed(DownloadRequest request, Exception exception) {
 *         System.err.println("Failed to download: " + exception.getMessage());
 *     }
 * };
 * </pre>
 */
public interface DownloadListener {

  /**
   * Called when a download is started.
   *
   * @param request the download request that is starting
   */
  void onDownloadStarted(DownloadRequest request);

  /**
   * Called when data has been downloaded from HDFS and written to the local file. This method is
   * called for each write operation on the underlying OutputStream.
   *
   * @param request the download request being processed
   * @param bytesWritten the number of bytes written in this specific write operation
   * @param totalBytesWritten the total number of bytes written so far for this download
   */
  void onDataDownloaded(DownloadRequest request, int bytesWritten, long totalBytesWritten);

  /**
   * Called when a download completes successfully.
   *
   * @param request the download request that completed
   * @param fileSizeBytes the total size of the downloaded file in bytes
   * @param downloadTimeMs the time taken to download the file in milliseconds
   */
  void onDownloadCompleted(DownloadRequest request, long fileSizeBytes, long downloadTimeMs);

  /**
   * Called when a download fails due to an exception.
   *
   * @param request the download request that failed
   * @param exception the exception that caused the failure
   */
  void onDownloadFailed(DownloadRequest request, Exception exception);
}
