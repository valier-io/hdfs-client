package io.valier.hdfs.dn.ex;

import io.valier.hdfs.crt.ex.HdfsBaseException;

/**
 * Runtime exception for DataNode-related HDFS operations.
 *
 * <p>This exception is thrown when DataNode operations fail due to issues with the highly available
 * HDFS infrastructure itself, such as network connectivity issues, DataNode unavailability, or
 * protocol errors. Since HDFS is designed to be highly available, these exceptions indicate
 * infrastructure problems that should be handled at the application level.
 *
 * <p>This exception should NOT be thrown for issues with the target OutputStream, as those are
 * related to the caller's environment, not HDFS infrastructure.
 *
 * <p>Example usage:
 *
 * <pre>
 * try {
 *     dataNodeClient.copy(locatedFile, outputStream);
 * } catch (DataNodeHdfsException e) {
 *     // Handle HDFS DataNode infrastructure issues
 *     log.error("DataNode operation failed", e);
 *     // Potentially retry or fail over to different DataNodes
 * } catch (IOException e) {
 *     // Handle output stream issues
 *     log.error("Failed to write to output stream", e);
 * }
 * </pre>
 */
public class DataNodeHdfsException extends HdfsBaseException {

  private static final long serialVersionUID = 1L;

  /** Constructs a new DataNode HDFS exception with no detail message. */
  public DataNodeHdfsException() {
    super();
  }

  /**
   * Constructs a new DataNode HDFS exception with the specified detail message.
   *
   * @param message the detail message explaining the reason for the exception
   */
  public DataNodeHdfsException(String message) {
    super(message);
  }

  /**
   * Constructs a new DataNode HDFS exception with the specified detail message and cause.
   *
   * @param message the detail message explaining the reason for the exception
   * @param cause the underlying cause of this exception (may be null)
   */
  public DataNodeHdfsException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new DataNode HDFS exception with the specified cause. The detail message will be
   * derived from the cause's detail message.
   *
   * @param cause the underlying cause of this exception (may be null)
   */
  public DataNodeHdfsException(Throwable cause) {
    super(cause);
  }
}
