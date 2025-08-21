package io.valier.hdfs.client;

import io.valier.hdfs.client.ex.HdfsClientException;
import io.valier.hdfs.client.ex.HdfsFileNotFoundException;
import io.valier.hdfs.nn.HdfsFileSummary;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Stream;

/**
 * High-level HDFS client interface that provides convenient methods for interacting with HDFS. This
 * client combines NameNode and DataNode operations to provide a unified API for HDFS operations.
 */
public interface HdfsClient {

  /**
   * Copies a file from HDFS to an OutputStream.
   *
   * <p>This method: 1. Queries the NameNode to get file metadata and block locations 2. Uses the
   * DataNode client to read block data from DataNodes 3. Writes the combined data to the provided
   * OutputStream
   *
   * @param hdfsPath the path of the file in HDFS (e.g., "/user/data/file.txt")
   * @param outputStream the OutputStream where the file content should be written
   * @throws IOException if there's an error during the copy operation
   */
  void copy(String hdfsPath, OutputStream outputStream) throws IOException;

  /**
   * Copies data from an InputStream to a file in HDFS.
   *
   * <p>This method: 1. Checks if the file already exists and fails if it does 2. Uses the NameNode
   * client to create the file and get the first block location 3. Writes data to blocks using the
   * DataNode client 4. Creates additional blocks as needed when current block is full 5. Completes
   * the file using the NameNode client when all data is written
   *
   * @param hdfsPath the absolute path where the file should be created in HDFS (e.g.,
   *     "/user/data/file.txt")
   * @param input the InputStream containing the data to write to HDFS
   * @throws IOException if there's an error during the copy operation, including if the file
   *     already exists
   */
  void copy(String hdfsPath, InputStream input) throws IOException;

  /**
   * Lists files and directories in the specified HDFS path.
   *
   * <p>This method queries the NameNode to get directory contents and returns detailed file
   * information similar to JDK NIO Files.list().
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * HdfsClientException}s.
   *
   * @param hdfsPath the directory path in HDFS to list (e.g., "/", "/user", "/tmp")
   * @return A stream of HdfsFileSummary objects containing file metadata
   * @throws HdfsClientException if there's an error with HDFS infrastructure operations
   * @throws HdfsFileNotFoundException if the specified path does not exist
   */
  Stream<HdfsFileSummary> list(String hdfsPath);

  /**
   * Creates a new directory in HDFS. The directory creation is atomic with respect to other
   * filesystem activities. The parent directory must already exist.
   *
   * <p>This method is similar to Java NIO {@code Files.createDirectory()} but operates on HDFS
   * paths and returns HDFS-specific metadata.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * HdfsClientException}s.
   *
   * @param hdfsPath the absolute path where the directory should be created in HDFS (e.g.,
   *     "/user/data/newdir")
   * @return HdfsFileSummary containing the created directory's metadata
   * @throws HdfsClientException if there's an error with HDFS infrastructure operations
   */
  HdfsFileSummary createDirectory(String hdfsPath);

  /**
   * Creates a directory by creating all nonexistent parent directories first. The directory
   * creation is atomic for each individual directory but the overall operation may leave the file
   * system in a partially created state if it fails.
   *
   * <p>This method is similar to Java NIO {@code Files.createDirectories()} but operates on HDFS
   * paths and returns HDFS-specific metadata for the target directory.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * HdfsClientException}s.
   *
   * @param hdfsPath the absolute path where the directory (and any necessary parent directories)
   *     should be created in HDFS (e.g., "/user/data/nested/newdir")
   * @return HdfsFileSummary containing the target directory's metadata
   * @throws HdfsClientException if there's an error with HDFS infrastructure operations
   */
  HdfsFileSummary createDirectories(String hdfsPath);

  /**
   * Reads a file's attributes as a bulk operation. This method provides functionality similar to
   * Java NIO {@code Files.readAttributes()} but always returns HDFS-specific metadata in the form
   * of an HdfsFileSummary object.
   *
   * <p>Unlike the NIO version which allows specifying the type of attributes to read, this method
   * always returns the complete set of HDFS file attributes available through the NameNode.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * HdfsClientException}s.
   *
   * @param hdfsPath the absolute path to the file or directory whose attributes should be read in
   *     HDFS (e.g., "/user/data/file.txt")
   * @return HdfsFileSummary containing the file's metadata and attributes
   * @throws HdfsClientException if there's an error with HDFS infrastructure operations
   * @throws HdfsFileNotFoundException if the specified path does not exist
   */
  HdfsFileSummary readAttributes(String hdfsPath);

  /**
   * Reads all bytes from a file in HDFS. This method provides functionality similar to Java NIO
   * {@code Files.readAllBytes()} but operates on HDFS files.
   *
   * <p>This method: 1. Queries the NameNode to get file metadata and block locations 2. Uses the
   * DataNode client to read all block data from DataNodes 3. Combines all blocks into a single byte
   * array
   *
   * <p><strong>Note:</strong> This method is not intended for reading large files as it loads the
   * entire file content into memory. For large files, consider using the streaming {@link
   * #copy(String, OutputStream)} method instead.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode/DataNode unavailability, protocol errors) are thrown as
   * unchecked {@link HdfsClientException}s.
   *
   * @param hdfsPath the absolute path to the file in HDFS (e.g., "/user/data/file.txt")
   * @return a byte array containing the complete file content
   * @throws HdfsClientException if there's an error with HDFS infrastructure operations
   */
  byte[] readAllBytes(String hdfsPath);

  /**
   * Reads all lines from a file in HDFS using the specified charset. This method provides
   * functionality similar to Java NIO {@code Files.readAllLines()} but operates on HDFS files.
   *
   * <p>This method: 1. Reads all bytes from the file using {@link #readAllBytes(String)} 2.
   * Converts the bytes to a string using the specified charset 3. Splits the content into lines
   * using standard line separators
   *
   * <p><strong>Note:</strong> This method is not intended for reading large files as it loads the
   * entire file content into memory. For large files, consider using streaming approaches.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode/DataNode unavailability, protocol errors) are thrown as
   * unchecked {@link HdfsClientException}s.
   *
   * @param hdfsPath the absolute path to the file in HDFS (e.g., "/user/data/file.txt")
   * @param charset the charset to use for decoding the file content
   * @return a List containing all lines from the file
   * @throws HdfsClientException if there's an error with HDFS infrastructure operations
   */
  List<String> readAllLines(String hdfsPath, Charset charset);

  /**
   * Deletes a file or directory in HDFS. This method is similar to Java NIO {@code Files.delete()}
   * but operates on HDFS paths.
   *
   * <p>For directories, the directory must be empty to be deleted. If the path is a symbolic link,
   * the link itself is deleted, not its target.
   *
   * <p>The operation may require examining the file to determine if it's a directory, and therefore
   * may not be atomic with respect to other file system operations.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * HdfsClientException}s.
   *
   * @param hdfsPath the absolute path to the file or directory to delete in HDFS (e.g.,
   *     "/user/data/file.txt" or "/user/data/emptydir")
   * @throws HdfsClientException if there's an error with HDFS infrastructure operations
   */
  void delete(String hdfsPath);

  /**
   * Deletes a file or directory in HDFS if it exists. This method is similar to Java NIO {@code
   * Files.deleteIfExists()} but operates on HDFS paths.
   *
   * <p>For directories, the directory must be empty to be deleted. If the path is a symbolic link,
   * the link itself is deleted, not its target.
   *
   * <p>The operation may require examining the file to determine if it's a directory, and therefore
   * may not be atomic with respect to other file system operations.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * HdfsClientException}s.
   *
   * @param hdfsPath the absolute path to the file or directory to delete in HDFS (e.g.,
   *     "/user/data/file.txt" or "/user/data/emptydir")
   * @return {@code true} if the file was deleted by this method; {@code false} if the file could
   *     not be deleted because it did not exist
   * @throws HdfsClientException if there's an error with HDFS infrastructure operations
   */
  boolean deleteIfExists(String hdfsPath);
}
