package io.valier.hdfs.client;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A FilterInputStream that limits the number of bytes that can be read from the underlying stream
 * to a specified maximum (typically block size of 128MB).
 *
 * <p>This stream is designed to enforce HDFS block size boundaries when writing data to blocks.
 * Once the limit is reached, subsequent read operations will return -1 (end of stream) even if the
 * underlying stream has more data.
 */
class BlockSizeLimitInputStream extends FilterInputStream {

  /** Default HDFS block size (128MB). */
  static final long DEFAULT_BLOCK_SIZE = 134217728L; // 128MB

  private final long maxBytes;
  private long bytesRead = 0;

  /**
   * Creates a new BlockSizeLimitInputStream with the default block size limit (128MB).
   *
   * @param in the underlying input stream
   */
  public BlockSizeLimitInputStream(InputStream in) {
    this(in, DEFAULT_BLOCK_SIZE);
  }

  /**
   * Creates a new BlockSizeLimitInputStream with the specified byte limit.
   *
   * @param in the underlying input stream
   * @param maxBytes the maximum number of bytes that can be read
   * @throws IllegalArgumentException if maxBytes is negative
   */
  public BlockSizeLimitInputStream(InputStream in, long maxBytes) {
    super(in);
    if (maxBytes < 0) {
      throw new IllegalArgumentException("maxBytes cannot be negative");
    }
    this.maxBytes = maxBytes;
  }

  @Override
  public int read() throws IOException {
    if (bytesRead >= maxBytes) {
      return -1; // End of stream reached for this block
    }

    int b = super.read();
    if (b != -1) {
      bytesRead++;
    }
    return b;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (bytesRead >= maxBytes) {
      return -1; // End of stream reached for this block
    }

    // Limit the read to not exceed the maximum bytes
    long remainingBytes = maxBytes - bytesRead;
    int bytesToRead = (int) Math.min(len, remainingBytes);

    int actualBytesRead = super.read(b, off, bytesToRead);
    if (actualBytesRead > 0) {
      bytesRead += actualBytesRead;
    }

    return actualBytesRead;
  }

  @Override
  public long skip(long n) throws IOException {
    if (bytesRead >= maxBytes) {
      return 0; // No more bytes can be skipped
    }

    // Limit the skip to not exceed the maximum bytes
    long remainingBytes = maxBytes - bytesRead;
    long bytesToSkip = Math.min(n, remainingBytes);

    long actualBytesSkipped = super.skip(bytesToSkip);
    bytesRead += actualBytesSkipped;

    return actualBytesSkipped;
  }

  @Override
  public int available() throws IOException {
    if (bytesRead >= maxBytes) {
      return 0; // No more bytes available for this block
    }

    // Return the minimum of underlying available bytes and remaining block capacity
    long remainingBytes = maxBytes - bytesRead;
    int underlyingAvailable = super.available();

    return (int) Math.min(underlyingAvailable, remainingBytes);
  }

  /**
   * Returns the number of bytes read so far.
   *
   * @return the number of bytes read from this stream
   */
  public long getBytesRead() {
    return bytesRead;
  }

  /**
   * Returns the maximum number of bytes that can be read from this stream.
   *
   * @return the maximum byte limit
   */
  public long getMaxBytes() {
    return maxBytes;
  }

  /**
   * Returns whether this stream has reached its byte limit.
   *
   * @return true if the maximum number of bytes has been read, false otherwise
   */
  public boolean isLimitReached() {
    return bytesRead >= maxBytes;
  }
}
