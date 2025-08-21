package io.valier.hdfs.nn.ex;

/**
 * Runtime exception thrown when a requested HDFS path does not exist.
 *
 * <p>This exception is thrown when HDFS operations fail because the target file or directory cannot
 * be found in the filesystem. It extends {@link NameNodeHdfsException} to maintain consistency with
 * the HDFS infrastructure exception hierarchy.
 *
 * <p>Example usage:
 *
 * <pre>
 * try {
 *     HdfsFileSummary file = hdfsClient.readAttributes("/non/existent/path");
 * } catch (HdfsFileNotFoundException e) {
 *     // Handle file not found scenario
 *     log.warn("HDFS path not found: {}", e.getMessage());
 * }
 * </pre>
 */
public class HdfsFileNotFoundException extends NameNodeHdfsException {

  private static final long serialVersionUID = 1L;

  /** Constructs a new HDFS file not found exception with no detail message. */
  public HdfsFileNotFoundException() {
    super();
  }

  /**
   * Constructs a new HDFS file not found exception with the specified detail message.
   *
   * @param message the detail message explaining the reason for the exception
   */
  public HdfsFileNotFoundException(String message) {
    super(message);
  }

  /**
   * Constructs a new HDFS file not found exception with the specified detail message and cause.
   *
   * @param message the detail message explaining the reason for the exception
   * @param cause the underlying cause of this exception (may be null)
   */
  public HdfsFileNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new HDFS file not found exception with the specified cause. The detail message
   * will be derived from the cause's detail message.
   *
   * @param cause the underlying cause of this exception (may be null)
   */
  public HdfsFileNotFoundException(Throwable cause) {
    super(cause);
  }
}
