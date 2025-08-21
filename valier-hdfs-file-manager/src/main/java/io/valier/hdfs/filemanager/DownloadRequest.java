package io.valier.hdfs.filemanager;

import io.valier.hdfs.filemanager.listener.DownloadListener;
import java.nio.file.Path;
import lombok.Builder;
import lombok.Value;

/**
 * Request object for downloading a single file from HDFS to a local path.
 *
 * <p>This request encapsulates the parameters needed for file download operations, including the
 * source HDFS file path, the destination local file path, and an optional download listener for
 * progress tracking.
 *
 * <p>Example usage:
 *
 * <pre>
 * DownloadRequest request = DownloadRequest.builder()
 *     .hdfsFilePath("/hdfs/source/file.txt")
 *     .localFilePath(Paths.get("/local/destination/file.txt"))
 *     .downloadListener(myDownloadListener)  // optional
 *     .build();
 *
 * CompletedFileDownload download = transferManager.download(request);
 * </pre>
 */
@Value
@Builder
public class DownloadRequest {

  /**
   * The HDFS file path to download (e.g., "/user/data/file.txt"). This should be an absolute path
   * in the HDFS filesystem.
   */
  String hdfsFilePath;

  /**
   * The local file path where the file should be saved. If the parent directory doesn't exist, it
   * will be created along with any necessary parent directories.
   */
  Path localFilePath;

  /**
   * Optional download listener to receive progress and lifecycle notifications. If null, no
   * progress notifications will be sent.
   */
  DownloadListener downloadListener;
}
