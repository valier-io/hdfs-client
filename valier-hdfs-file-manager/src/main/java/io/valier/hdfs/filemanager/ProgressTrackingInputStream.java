package io.valier.hdfs.filemanager;

import io.valier.hdfs.filemanager.listener.UploadListener;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A FilterInputStream that tracks upload progress by notifying an UploadListener whenever data is
 * read from the underlying stream.
 *
 * <p>This implementation wraps an existing InputStream and intercepts all read operations to
 * provide progress callbacks to the registered listener.
 */
class ProgressTrackingInputStream extends FilterInputStream {

  private final UploadRequest uploadRequest;
  private final UploadListener uploadListener;
  private long totalBytesRead = 0;

  /**
   * Creates a new ProgressTrackingInputStream that wraps the given InputStream.
   *
   * @param in the underlying InputStream to wrap
   * @param uploadRequest the upload request context
   * @param uploadListener the listener to notify of progress events
   */
  public ProgressTrackingInputStream(
      InputStream in, UploadRequest uploadRequest, UploadListener uploadListener) {
    super(in);
    this.uploadRequest = uploadRequest;
    this.uploadListener = uploadListener;
  }

  @Override
  public int read() throws IOException {
    int result = in.read();
    if (result != -1) {
      totalBytesRead += 1;
      uploadListener.onUploadProgress(uploadRequest, totalBytesRead);
    }
    return result;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = in.read(b, off, len);
    if (bytesRead > 0) {
      totalBytesRead += bytesRead;
      uploadListener.onUploadProgress(uploadRequest, totalBytesRead);
    }
    return bytesRead;
  }

  /**
   * Returns the total number of bytes read through this stream.
   *
   * @return the total number of bytes read
   */
  public long getTotalBytesRead() {
    return totalBytesRead;
  }
}
