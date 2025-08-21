package io.valier.hdfs.client.ex;

import io.valier.hdfs.crt.ex.HdfsBaseException;

/**
 * Runtime exception for HDFS client operations.
 *
 * <p>This exception is thrown when HDFS client operations fail due to issues with the highly
 * available HDFS infrastructure itself, such as network connectivity issues, NameNode/DataNode
 * unavailability, or protocol errors. Since HDFS is designed to be highly available, these
 * exceptions indicate infrastructure problems that should be handled at the application level.
 *
 * <p>This exception should NOT be thrown for issues with the source InputStream or target
 * OutputStream in copy operations, as those are related to the caller's environment, not HDFS
 * infrastructure.
 *
 * <p>Example usage:
 *
 * <pre>
 * try {
 *     List&lt;HdfsFileSummary&gt; files = hdfsClient.list("/path");
 * } catch (HdfsClientException e) {
 *     // Handle HDFS infrastructure issues
 *     log.error("HDFS client operation failed", e);
 *     // Potentially retry or fail over
 * }
 * </pre>
 */
public class HdfsClientException extends HdfsBaseException {

  private static final long serialVersionUID = 1L;

  /** Constructs a new HDFS client exception with no detail message. */
  public HdfsClientException() {
    super();
  }

  /**
   * Constructs a new HDFS client exception with the specified detail message.
   *
   * @param message the detail message explaining the reason for the exception
   */
  public HdfsClientException(String message) {
    super(message);
  }

  /**
   * Constructs a new HDFS client exception with the specified detail message and cause.
   *
   * @param message the detail message explaining the reason for the exception
   * @param cause the underlying cause of this exception (may be null)
   */
  public HdfsClientException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new HDFS client exception with the specified cause. The detail message will be
   * derived from the cause's detail message.
   *
   * @param cause the underlying cause of this exception (may be null)
   */
  public HdfsClientException(Throwable cause) {
    super(cause);
  }
}
