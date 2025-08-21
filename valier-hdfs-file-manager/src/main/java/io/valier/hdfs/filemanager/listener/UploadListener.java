package io.valier.hdfs.filemanager.listener;

import io.valier.hdfs.filemanager.UploadRequest;

/**
 * Listener interface for receiving upload progress and lifecycle notifications.
 *
 * <p>Implementations of this interface can be provided to upload operations to receive callbacks
 * during the upload lifecycle, including start, progress, completion, and failure notifications.
 *
 * <p>Example usage:
 *
 * <pre>
 * UploadListener listener = new UploadListener() {
 *     {@literal @}Override
 *     public void onUploadStarted(UploadRequest request) {
 *         System.out.println("Starting upload of " + request.getLocalFilePath());
 *     }
 *
 *     {@literal @}Override
 *     public void onUploadProgress(UploadRequest request, long bytesUploaded) {
 *         System.out.println("Uploaded " + bytesUploaded + " bytes");
 *     }
 *
 *     {@literal @}Override
 *     public void onUploadCompleted(UploadRequest request, long totalBytes, long uploadTimeMs) {
 *         System.out.println("Upload completed: " + totalBytes + " bytes in " + uploadTimeMs + " ms");
 *     }
 *
 *     {@literal @}Override
 *     public void onUploadFailed(UploadRequest request, Exception exception) {
 *         System.err.println("Upload failed: " + exception.getMessage());
 *     }
 * };
 * </pre>
 */
public interface UploadListener {

  /**
   * Called when the upload operation starts.
   *
   * @param request the upload request that started
   */
  void onUploadStarted(UploadRequest request);

  /**
   * Called periodically during the upload operation to report progress. This method may be called
   * multiple times as data is read from the local file and written to HDFS blocks.
   *
   * @param request the upload request in progress
   * @param bytesUploaded the number of bytes uploaded so far
   */
  void onUploadProgress(UploadRequest request, long bytesUploaded);

  /**
   * Called when the upload operation completes successfully.
   *
   * @param request the upload request that completed
   * @param totalBytes the total number of bytes uploaded
   * @param uploadTimeMs the total time taken for the upload in milliseconds
   */
  void onUploadCompleted(UploadRequest request, long totalBytes, long uploadTimeMs);

  /**
   * Called when the upload operation fails.
   *
   * @param request the upload request that failed
   * @param exception the exception that caused the failure
   */
  void onUploadFailed(UploadRequest request, Exception exception);
}
