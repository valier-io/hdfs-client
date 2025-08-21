package io.valier.hdfs.nn;

import io.valier.hdfs.nn.ex.NameNodeHdfsException;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Interface for NameNode client that communicates directly with HDFS NameNode using protobuf
 * messages without relying on full Hadoop libraries.
 *
 * <p>This interface provides methods for interacting with HDFS NameNode operations such as
 * directory listing and file operations.
 *
 * <p>Usage example:
 *
 * <pre>
 * NameNodeClient client = NameNodeClient.builder()
 *     .nameNodeUri("hdfs://namenode:9000")
 *     .userInformationProvider(() -> SimpleUserInformation.currentUser())
 *     .build();
 *
 * Stream&lt;HdfsFileSummary&gt; files = client.list("/");
 * </pre>
 */
public interface NameNodeClient {

  /**
   * Lists files and directories in the specified path from the NameNode. This method returns a
   * stream of HdfsFileSummary objects.
   *
   * <p><strong>Limitation:</strong> This method currently returns only the first 1000 files in the
   * directory due to HDFS protocol paging. Directories with more than 1000 entries will have their
   * results truncated.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * NameNodeHdfsException}s.
   *
   * @param path The directory path to list (e.g., "/", "/user", "/tmp")
   * @return A stream of HdfsFileSummary objects containing file metadata (limited to first 1000
   *     entries)
   * @throws NameNodeHdfsException If there's an error with HDFS NameNode operations
   */
  Stream<HdfsFileSummary> list(String path);

  /**
   * Gets the server information from the NameNode.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * NameNodeHdfsException}s.
   *
   * @return HdfsServerInfo containing server metadata including version information
   * @throws NameNodeHdfsException If there's an error with HDFS NameNode operations
   */
  HdfsServerInfo getBuildVersion();

  /**
   * Creates a new file in HDFS and initializes the file creation process.
   *
   * <p><strong>File Write Lifecycle:</strong>
   *
   * <ol>
   *   <li><strong>Initialize:</strong> Call {@code create()} once to initialize the file creation
   *       process
   *   <li><strong>Add blocks:</strong> Call {@link #completeBlockAndAddNext(HdfsFileSummary)} N
   *       times - once for each block except the last. Each call completes the previous block and
   *       allocates a new block for writing
   *   <li><strong>Finalize:</strong> Call {@link #complete(HdfsFileSummary)} to acknowledge the
   *       final block and mark the entire file as complete and ready for reading
   * </ol>
   *
   * <p><strong>Example:</strong>
   *
   * <pre>
   * // 1. Initialize file creation
   * HdfsFileSummary file = client.create("/path/file.txt", true, (short) 3, 128MB);
   *
   * // 2. Write first block, then get next block
   * file = client.completeBlockAndAddNext(file);  // Complete block 0, add block 1
   * // Write to block 1...
   * file = client.completeBlockAndAddNext(file);  // Complete block 1, add block 2
   * // Write to block 2...
   *
   * // 3. Complete the file after writing the last block
   * boolean success = client.complete(file);  // Complete final block and file
   * </pre>
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * NameNodeHdfsException}s.
   *
   * @param path The absolute path where the file should be created
   * @param createParent Whether to create parent directories if they don't exist
   * @param replication The replication factor for the file
   * @param blockSize The block size for the file
   * @return HdfsFileSummary containing the created file's metadata (initially with no blocks)
   * @throws NameNodeHdfsException If there's an error with HDFS NameNode operations
   */
  HdfsFileSummary create(String path, boolean createParent, short replication, long blockSize);

  /**
   * Completes the current block and adds a new block to an existing file in HDFS.
   *
   * <p>This operation performs two actions:
   *
   * <ol>
   *   <li>Acknowledges and completes the last block in the file (if any) based on the actual bytes
   *       written as recorded in the block locations
   *   <li>Allocates a new empty block for continued writing
   * </ol>
   *
   * <p>The dual nature of this operation ensures that each block's final size is properly recorded
   * in the NameNode's metadata before a new block is allocated, maintaining consistency between the
   * actual data written to DataNodes and the metadata stored in the NameNode.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * NameNodeHdfsException}s.
   *
   * @param target The existing file to complete the last block and add a new block to
   * @return HdfsFileSummary containing the updated file metadata with new block locations
   * @throws NameNodeHdfsException If there's an error with HDFS NameNode operations
   */
  HdfsFileSummary completeBlockAndAddNext(HdfsFileSummary target);

  /**
   * Completes a file in HDFS, marking it as fully written and ready for reading. after the last
   * packet of the last block has been written and acknowledged by the DataNodes. The last block
   * length is obtained from the target's last block location.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * NameNodeHdfsException}s.
   *
   * @param target The file to complete
   * @return true if the file was successfully completed, false otherwise
   * @throws NameNodeHdfsException If there's an error with HDFS NameNode operations
   */
  boolean complete(HdfsFileSummary target);

  /**
   * Creates a new directory in HDFS. The directory creation is atomic with respect to other
   * filesystem activities. The parent directory must already exist.
   *
   * <p>This method is similar to Java NIO {@code Files.createDirectory()} but operates on HDFS
   * paths and returns HDFS-specific metadata.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * NameNodeHdfsException}s.
   *
   * @param path The path where the directory should be created
   * @return HdfsFileSummary containing the created directory's metadata
   * @throws NameNodeHdfsException If there's an error with HDFS NameNode operations
   */
  HdfsFileSummary createDirectory(String path);

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
   * NameNodeHdfsException}s.
   *
   * @param path The path where the directory (and any necessary parent directories) should be
   *     created
   * @return HdfsFileSummary containing the target directory's metadata
   * @throws NameNodeHdfsException If there's an error with HDFS NameNode operations
   */
  HdfsFileSummary createDirectories(String path);

  /**
   * Reads a file's attributes as a bulk operation, returning an Optional to indicate presence. This
   * method provides functionality similar to Java NIO {@code Files.readAttributes()} but returns an
   * Optional wrapper to handle non-existent files gracefully.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * NameNodeHdfsException}s.
   *
   * @param path The path to the file or directory whose attributes should be read
   * @return Optional containing HdfsFileSummary with file metadata, or empty if the path doesn't
   *     exist
   * @throws NameNodeHdfsException If there's an error with HDFS NameNode operations (not including
   *     file not found)
   */
  Optional<HdfsFileSummary> readAttributesOptional(String path);

  /**
   * Reads a file's attributes as a bulk operation. This method provides functionality similar to
   * Java NIO {@code Files.readAttributes()} but always returns HDFS-specific metadata in the form
   * of an HdfsFileSummary object.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * NameNodeHdfsException}s.
   *
   * @param path The path to the file or directory whose attributes should be read
   * @return HdfsFileSummary containing the file's metadata and attributes
   * @throws NameNodeHdfsException If there's an error with HDFS NameNode operations
   */
  HdfsFileSummary readAttributes(String path);

  /**
   * Deletes a file or directory.
   *
   * <p>For directories, the directory must be empty to be deleted. If the path is a symbolic link,
   * the link itself is deleted, not its target.
   *
   * <p>The operation may require examining the file to determine if it's a directory, and therefore
   * may not be atomic with respect to other file system operations.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, NameNode unavailability, protocol errors) are thrown as unchecked {@link
   * NameNodeHdfsException}s.
   *
   * @param path The path to the file or directory to delete
   * @throws NameNodeHdfsException If there's an error with HDFS NameNode operations
   */
  void delete(String path);
}
