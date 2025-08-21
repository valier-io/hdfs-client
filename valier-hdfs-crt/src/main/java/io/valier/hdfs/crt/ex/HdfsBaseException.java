package io.valier.hdfs.crt.ex;

/**
 * Base runtime exception for all HDFS-related operations.
 *
 * <p>This exception serves as the root exception class for all HDFS client library errors. It
 * extends RuntimeException to provide unchecked exception semantics, allowing client code to handle
 * HDFS errors without mandatory try-catch blocks while still providing the option to catch and
 * handle specific error conditions.
 *
 * <p>Subclasses of this exception should be created for specific error types (e.g., file not found,
 * permission denied, network errors, etc.).
 *
 * <p>Example usage:
 *
 * <pre>
 * try {
 *     hdfsClient.copy("/path/to/file", outputStream);
 * } catch (HdfsBaseException e) {
 *     // Handle any HDFS-related error
 *     log.error("HDFS operation failed", e);
 * }
 * </pre>
 */
public class HdfsBaseException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** Constructs a new HDFS exception with no detail message. */
  public HdfsBaseException() {
    super();
  }

  /**
   * Constructs a new HDFS exception with the specified detail message.
   *
   * @param message the detail message explaining the reason for the exception
   */
  public HdfsBaseException(String message) {
    super(message);
  }

  /**
   * Constructs a new HDFS exception with the specified detail message and cause.
   *
   * @param message the detail message explaining the reason for the exception
   * @param cause the underlying cause of this exception (may be null)
   */
  public HdfsBaseException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new HDFS exception with the specified cause. The detail message will be derived
   * from the cause's detail message.
   *
   * @param cause the underlying cause of this exception (may be null)
   */
  public HdfsBaseException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs a new HDFS exception with the specified detail message, cause, suppression enabled
   * or disabled, and writable stack trace enabled or disabled.
   *
   * @param message the detail message explaining the reason for the exception
   * @param cause the underlying cause of this exception (may be null)
   * @param enableSuppression whether suppression is enabled or disabled
   * @param writableStackTrace whether the stack trace should be writable
   */
  protected HdfsBaseException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
