package io.valier.hdfs.filemanager;

import io.valier.hdfs.filemanager.listener.UploadListener;
import java.nio.file.Path;
import lombok.Builder;
import lombok.Value;

/**
 * Request object for uploading a single file from a local path to HDFS.
 *
 * <p>This request encapsulates the parameters needed for file upload operations, including the
 * source local file path, the destination HDFS file path, and an optional upload listener for
 * progress tracking.
 *
 * <p>Example usage:
 *
 * <pre>
 * UploadRequest request = UploadRequest.builder()
 *     .localFilePath(Paths.get("/local/source/file.txt"))
 *     .hdfsFilePath("/hdfs/destination/file.txt")
 *     .uploadListener(myUploadListener)  // optional
 *     .build();
 *
 * CompletedFileUpload upload = transferManager.upload(request);
 * </pre>
 */
@Value
@Builder
public class UploadRequest {

  /** The local file path to upload. The file must exist and be readable. */
  Path localFilePath;

  /**
   * The HDFS file path where the file should be saved (e.g., "/user/data/file.txt"). This should be
   * an absolute path in the HDFS filesystem. If the parent directory doesn't exist, it will be
   * created along with any necessary parent directories.
   */
  String hdfsFilePath;

  /**
   * Optional upload listener to receive progress and lifecycle notifications. If null, no progress
   * notifications will be sent.
   */
  UploadListener uploadListener;
}
