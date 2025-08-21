package io.valier.hdfs.nn.ex;

import io.valier.hdfs.crt.ex.HdfsBaseException;

/**
 * Runtime exception for NameNode-related HDFS operations.
 *
 * <p>This exception is thrown when NameNode operations fail due to issues with the highly available
 * HDFS infrastructure itself, such as network connectivity issues, NameNode unavailability, or
 * protocol errors. Since HDFS is designed to be highly available, these exceptions indicate
 * infrastructure problems that should be handled at the application level.
 *
 * <p>Example usage:
 *
 * <pre>
 * try {\n     List&lt;HdfsFileSummary&gt; files = nameNodeClient.list(\"/path\");\n * } catch (NameNodeHdfsException e) {\n     // Handle HDFS NameNode infrastructure issues\n     log.error(\"NameNode operation failed\", e);\n     // Potentially retry or fail over to different NameNodes\n * }\n *
 * </pre>
 */
public class NameNodeHdfsException extends HdfsBaseException {

  private static final long serialVersionUID = 1L;

  /** Constructs a new NameNode HDFS exception with no detail message. */
  public NameNodeHdfsException() {
    super();
  }

  /**
   * Constructs a new NameNode HDFS exception with the specified detail message.
   *
   * @param message the detail message explaining the reason for the exception
   */
  public NameNodeHdfsException(String message) {
    super(message);
  }

  /**
   * Constructs a new NameNode HDFS exception with the specified detail message and cause.
   *
   * @param message the detail message explaining the reason for the exception
   * @param cause the underlying cause of this exception (may be null)
   */
  public NameNodeHdfsException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new NameNode HDFS exception with the specified cause. The detail message will be
   * derived from the cause's detail message.
   *
   * @param cause the underlying cause of this exception (may be null)
   */
  public NameNodeHdfsException(Throwable cause) {
    super(cause);
  }
}
