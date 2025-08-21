package io.valier.hdfs.dn;

import java.io.IOException;

/**
 * Internal exception to distinguish InputStream errors from DataNode errors.
 *
 * <p>This exception is used internally by DataNode client implementations to wrap IOException from
 * InputStream operations so they can be distinguished from DataNode infrastructure errors. When
 * caught, the original IOException cause should be unwrapped and rethrown.
 */
class InputStreamIOException extends IOException {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new InputStreamIOException with the specified detail message and cause.
   *
   * @param message the detail message explaining the reason for the exception
   * @param cause the underlying IOException from the InputStream operation
   */
  public InputStreamIOException(String message, Throwable cause) {
    super(message, cause);
  }
}
