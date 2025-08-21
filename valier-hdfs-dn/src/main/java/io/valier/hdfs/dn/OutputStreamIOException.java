package io.valier.hdfs.dn;

import java.io.IOException;

/**
 * Internal exception to distinguish OutputStream errors from DataNode errors.
 *
 * <p>This exception is used internally by DataNode client implementations to wrap IOException from
 * OutputStream operations so they can be distinguished from DataNode infrastructure errors. When
 * caught, the original IOException cause should be unwrapped and rethrown.
 */
class OutputStreamIOException extends IOException {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new OutputStreamIOException with the specified detail message and cause.
   *
   * @param message the detail message explaining the reason for the exception
   * @param cause the underlying IOException from the OutputStream operation
   */
  public OutputStreamIOException(String message, Throwable cause) {
    super(message, cause);
  }
}
