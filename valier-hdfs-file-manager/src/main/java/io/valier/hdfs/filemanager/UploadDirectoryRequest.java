package io.valier.hdfs.filemanager;

import java.nio.file.Path;
import lombok.Builder;
import lombok.Value;

/**
 * Request object for uploading an entire directory from the local filesystem to HDFS.
 *
 * <p>This request encapsulates the parameters needed for directory upload operations, including the
 * source local directory path and the destination HDFS directory path.
 *
 * <p>Example usage:
 *
 * <pre>
 * UploadDirectoryRequest request = UploadDirectoryRequest.builder()
 *     .localDirectoryPath(Paths.get("/local/source/directory"))
 *     .hdfsDirectoryPath("/hdfs/destination/directory")
 *     .build();
 *
 * CompletedDirectoryUpload upload = transferManager.uploadDirectory(request);
 * </pre>
 */
@Value
@Builder
public class UploadDirectoryRequest {

  /**
   * The local directory path to upload. This should be an existing directory on the local
   * filesystem.
   */
  Path localDirectoryPath;

  /**
   * The HDFS directory path where files should be uploaded (e.g., "/user/data"). This should be an
   * absolute path in the HDFS filesystem. If this directory doesn't exist, it will be created along
   * with any necessary parent directories.
   */
  String hdfsDirectoryPath;
}
