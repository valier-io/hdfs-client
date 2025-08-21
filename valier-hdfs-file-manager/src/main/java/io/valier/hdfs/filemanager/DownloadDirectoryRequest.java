package io.valier.hdfs.filemanager;

import java.nio.file.Path;
import lombok.Builder;
import lombok.Value;

/**
 * Request object for downloading an entire directory from HDFS to a local path.
 *
 * <p>This request encapsulates the parameters needed for directory download operations, including
 * the source HDFS directory path and the destination local directory path.
 *
 * <p>Example usage:
 *
 * <pre>
 * DownloadDirectoryRequest request = DownloadDirectoryRequest.builder()
 *     .hdfsDirectoryPath("/hdfs/source/directory")
 *     .localDirectoryPath(Paths.get("/local/destination/directory"))
 *     .build();
 *
 * CompletedDirectoryDownload download = transferManager.downloadDirectory(request);
 * </pre>
 */
@Value
@Builder
public class DownloadDirectoryRequest {

  /**
   * The HDFS directory path to download (e.g., "/user/data"). This should be an absolute path in
   * the HDFS filesystem.
   */
  String hdfsDirectoryPath;

  /**
   * The local directory where files should be saved. If this directory doesn't exist, it will be
   * created along with any necessary parent directories.
   */
  Path localDirectoryPath;
}
