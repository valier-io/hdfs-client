package io.valier.hdfs.crt;

/**
 * Utility class for working with HDFS paths.
 *
 * <p>This class provides methods for parsing, constructing, and manipulating HDFS paths. HDFS uses
 * forward slash (/) as the path delimiter, similar to Unix/Linux filesystems.
 *
 * <p>Example usage:
 *
 * <pre>
 * String root = HdfsPaths.rootDirectory();  // returns "/"
 * String path = HdfsPaths.get("user", "data", "file.txt");  // returns "/user/data/file.txt"
 * String path2 = HdfsPaths.get("/user", "data", "file.txt"); // returns "/user/data/file.txt"
 * </pre>
 */
public final class HdfsPaths {

  /** HDFS path delimiter character. */
  public static final String DELIMITER = "/";

  /** Private constructor to prevent instantiation of utility class. */
  private HdfsPaths() {
    throw new AssertionError("HdfsPaths is a utility class and should not be instantiated");
  }

  /**
   * Returns the root directory path for HDFS.
   *
   * @return the root directory path "/"
   */
  public static String rootDirectory() {
    return DELIMITER;
  }

  /**
   * Constructs an HDFS path by joining the given path elements with the HDFS delimiter.
   *
   * <p>This method works similarly to Java's Paths.get() method but for HDFS paths. It joins all
   * provided path elements with forward slashes and ensures the result is a properly formatted HDFS
   * path.
   *
   * <p>Rules: - Empty or null path elements are ignored - Leading and trailing slashes on
   * individual elements are handled appropriately - The result always starts with a single forward
   * slash (absolute path) - Multiple consecutive slashes are collapsed to a single slash
   *
   * @param first the first path element (required)
   * @param more additional path elements (optional)
   * @return a properly formatted HDFS path starting with "/"
   * @throws IllegalArgumentException if the first path element is null or empty
   */
  public static String get(String first, String... more) {
    if (first == null || first.isEmpty()) {
      throw new IllegalArgumentException("First path element cannot be null or empty");
    }

    StringBuilder pathBuilder = new StringBuilder();

    // Ensure path starts with root delimiter
    if (!first.startsWith(DELIMITER)) {
      pathBuilder.append(DELIMITER);
    }

    // Add the first path element
    appendPathElement(pathBuilder, first);

    // Add additional path elements
    if (more != null) {
      for (String element : more) {
        if (element != null && !element.isEmpty()) {
          appendPathElement(pathBuilder, element);
        }
      }
    }

    // Normalize the path (remove double slashes, etc.)
    return normalizePath(pathBuilder.toString());
  }

  /**
   * Appends a path element to the StringBuilder, handling delimiters appropriately.
   *
   * @param pathBuilder the StringBuilder to append to
   * @param element the path element to append
   */
  private static void appendPathElement(StringBuilder pathBuilder, String element) {
    // Remove leading slash from element (we'll add our own delimiter)
    String cleanElement = element.startsWith(DELIMITER) ? element.substring(1) : element;

    // Remove trailing slash from element
    if (cleanElement.endsWith(DELIMITER)) {
      cleanElement = cleanElement.substring(0, cleanElement.length() - 1);
    }

    // Skip empty elements after cleaning
    if (cleanElement.isEmpty()) {
      return;
    }

    // Add delimiter if needed (not at the start, and not if the last character is already a
    // delimiter)
    if (pathBuilder.length() > 0 && !pathBuilder.toString().endsWith(DELIMITER)) {
      pathBuilder.append(DELIMITER);
    }

    pathBuilder.append(cleanElement);
  }

  /**
   * Normalizes an HDFS path by removing duplicate slashes and ensuring proper formatting.
   *
   * @param path the path to normalize
   * @return the normalized path
   */
  private static String normalizePath(String path) {
    if (path == null || path.isEmpty()) {
      return rootDirectory();
    }

    // Replace multiple consecutive slashes with single slash
    String normalized = path.replaceAll("/+", DELIMITER);

    // Ensure it starts with a single slash
    if (!normalized.startsWith(DELIMITER)) {
      normalized = DELIMITER + normalized;
    }

    // Handle special case of root directory
    if (normalized.equals(DELIMITER)) {
      return normalized;
    }

    // Remove trailing slash (except for root)
    if (normalized.endsWith(DELIMITER) && normalized.length() > 1) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }

    return normalized;
  }

  /**
   * Extracts the file name from a full HDFS path.
   *
   * <p>This method returns the last component of the path, which represents the file or directory
   * name. For example:
   *
   * <pre>
   * HdfsPaths.getName("/user/data/file.txt")  // returns "file.txt"
   * HdfsPaths.getName("/user/data/")          // returns "data"
   * HdfsPaths.getName("/")                    // returns ""
   * </pre>
   *
   * @param fullPath the full HDFS path
   * @return the file or directory name, or empty string for root path
   */
  public static String getName(String fullPath) {
    if (fullPath == null || fullPath.isEmpty()) {
      return "";
    }

    // Remove trailing slash if present (except for root)
    if (fullPath.endsWith(DELIMITER) && fullPath.length() > 1) {
      fullPath = fullPath.substring(0, fullPath.length() - 1);
    }

    // Handle root directory case
    if (fullPath.equals(DELIMITER)) {
      return "";
    }

    // Extract last component of path
    int lastSlash = fullPath.lastIndexOf(DELIMITER);
    if (lastSlash >= 0 && lastSlash < fullPath.length() - 1) {
      return fullPath.substring(lastSlash + 1);
    }

    return fullPath;
  }

  /**
   * Validates that an HDFS path is absolute (starts with '/') and not null.
   *
   * <p>HDFS paths must always be absolute paths starting with a forward slash. This method provides
   * a consistent validation mechanism for ensuring path requirements are met.
   *
   * @param hdfsPath the HDFS path to validate
   * @throws IllegalArgumentException if the path is null or does not start with '/'
   */
  public static void requireAbsolute(String hdfsPath) {
    if (hdfsPath == null || !hdfsPath.startsWith("/")) {
      throw new IllegalArgumentException(
          "HDFS path must be absolute and start with '/': " + hdfsPath);
    }
  }
}
