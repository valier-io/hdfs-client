package io.valier.hdfs.filemanager;

import io.valier.hdfs.filemanager.listener.DownloadListener;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A FilterOutputStream that tracks download progress by notifying a DownloadListener whenever data
 * is written to the underlying stream.
 *
 * <p>This implementation wraps an existing OutputStream and intercepts all write operations to
 * provide progress callbacks to the registered listener.
 */
class ProgressTrackingOutputStream extends FilterOutputStream {

  private final DownloadRequest downloadRequest;
  private final DownloadListener downloadListener;
  private long totalBytesWritten = 0;

  /**
   * Creates a new ProgressTrackingOutputStream that wraps the given OutputStream.
   *
   * @param out the underlying OutputStream to wrap
   * @param downloadRequest the download request context
   * @param downloadListener the listener to notify of progress events
   */
  public ProgressTrackingOutputStream(
      OutputStream out, DownloadRequest downloadRequest, DownloadListener downloadListener) {
    super(out);
    this.downloadRequest = downloadRequest;
    this.downloadListener = downloadListener;
  }

  @Override
  public void write(int b) throws IOException {
    out.write(b);
    totalBytesWritten += 1;
    downloadListener.onDataDownloaded(downloadRequest, 1, totalBytesWritten);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
    totalBytesWritten += len;
    downloadListener.onDataDownloaded(downloadRequest, len, totalBytesWritten);
  }

  /**
   * Returns the total number of bytes written through this stream.
   *
   * @return the total number of bytes written
   */
  public long getTotalBytesWritten() {
    return totalBytesWritten;
  }
}
