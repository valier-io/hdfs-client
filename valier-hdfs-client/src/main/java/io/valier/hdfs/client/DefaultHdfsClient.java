package io.valier.hdfs.client;

import io.valier.hdfs.client.ex.HdfsClientException;
import io.valier.hdfs.client.ex.HdfsFileNotFoundException;
import io.valier.hdfs.crt.HdfsPaths;
import io.valier.hdfs.dn.DataNodeClient;
import io.valier.hdfs.dn.LocatedBlock;
import io.valier.hdfs.dn.LocatedFile;
import io.valier.hdfs.nn.HdfsFileSummary;
import io.valier.hdfs.nn.NameNodeClient;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation of HdfsClient that combines NameNode and DataNode operations to provide a
 * unified interface for HDFS operations.
 *
 * <p>This implementation uses the NameNode client to get file metadata and block locations, then
 * uses the DataNode client to read actual block data.
 */
@Slf4j
@Value
@Builder
public class DefaultHdfsClient implements HdfsClient {

  /** NameNode client for metadata operations. */
  NameNodeClient nameNodeClient;

  /** DataNode client provider for creating DataNode connections. */
  @Builder.Default
  DataNodeClientProvider dataNodeClientProvider = DefaultDataNodeClientProvider.builder().build();

  /**
   * Local mode flag for testing in containerized environments. When enabled (true), all DataNode
   * hostnames are mapped to 'localhost' to facilitate testing scenarios where HDFS is running in
   * Docker containers but accessed from the host. This bypasses normal hostname resolution and
   * forces connections through localhost, which is typical when Docker containers expose ports on
   * the host machine.
   *
   * <p>Default: false (use actual hostnames from HDFS metadata)
   */
  @Builder.Default boolean localMode = false;

  /**
   * Default replication factor for new files. This determines how many copies of each block are
   * stored across different DataNodes.
   *
   * <p>Default: 3 (standard HDFS replication factor)
   */
  @Builder.Default int replicationFactor = 3;

  /**
   * Default block size for new files in bytes. This determines the size of each block when
   * splitting large files.
   *
   * <p>Default: 134217728 (128MB - standard HDFS block size)
   */
  @Builder.Default long blockSize = 134217728L; // 128MB

  /**
   * Validates that the NameNodeClient is configured and throws an exception if it's null.
   *
   * @throws IllegalStateException if nameNodeClient is null
   */
  private void requireNameNodeClient() {
    if (nameNodeClient == null) {
      throw new IllegalStateException("NameNodeClient is required");
    }
  }

  @Override
  public Stream<HdfsFileSummary> list(String hdfsPath) {
    requireNameNodeClient();
    try {
      return nameNodeClient.list(hdfsPath);
    } catch (Exception e) {
      throw new HdfsClientException("Failed to list HDFS path: " + hdfsPath, e);
    }
  }

  @Override
  public HdfsFileSummary createDirectory(String hdfsPath) {
    requireNameNodeClient();
    try {
      return nameNodeClient.createDirectory(hdfsPath);
    } catch (Exception e) {
      throw new HdfsClientException("Failed to create directory: " + hdfsPath, e);
    }
  }

  @Override
  public HdfsFileSummary createDirectories(String hdfsPath) {
    requireNameNodeClient();
    try {
      return nameNodeClient.createDirectories(hdfsPath);
    } catch (Exception e) {
      throw new HdfsClientException("Failed to create directories: " + hdfsPath, e);
    }
  }

  @Override
  public HdfsFileSummary readAttributes(String hdfsPath) {
    requireNameNodeClient();
    Optional<HdfsFileSummary> result = Optional.empty();
    try {
      result = nameNodeClient.readAttributesOptional(hdfsPath);
    } catch (Exception e) {
      throw new HdfsClientException("Failed to read attributes for: " + hdfsPath, e);
    }
    return result.orElseThrow(
        () -> new HdfsFileNotFoundException("HDFS path not found: " + hdfsPath));
  }

  @Override
  public byte[] readAllBytes(String hdfsPath) {
    requireNameNodeClient();

    try {
      // Get the file attributes and validate it exists and is not a directory
      HdfsFileSummary fileSummary = nameNodeClient.readAttributes(hdfsPath);

      if (fileSummary.isDirectory()) {
        throw new HdfsClientException("Path is a directory, not a file: " + hdfsPath);
      }

      // Handle empty file case
      if (fileSummary.getLength() == 0) {
        return new byte[0];
      }

      // Use ByteArrayOutputStream to collect all bytes
      try (ByteArrayOutputStream baos =
          new ByteArrayOutputStream(Math.toIntExact(fileSummary.getLength()))) {
        copy(hdfsPath, baos);
        return baos.toByteArray();
      } catch (IOException e) {
        throw new HdfsClientException("Failed to read all bytes from: " + hdfsPath, e);
      }
    } catch (HdfsClientException e) {
      throw e;
    } catch (Exception e) {
      throw new HdfsClientException("Failed to read all bytes from: " + hdfsPath, e);
    }
  }

  @Override
  public List<String> readAllLines(String hdfsPath, Charset charset) {
    requireNameNodeClient();

    if (charset == null) {
      throw new IllegalArgumentException("Charset cannot be null");
    }

    try {
      // Read all bytes from the file
      byte[] bytes = readAllBytes(hdfsPath);

      // Handle empty file case
      if (bytes.length == 0) {
        return Collections.emptyList();
      }

      // Convert bytes to string using the specified charset
      String content = new String(bytes, charset);

      // Split into lines - handle both Unix (\n) and Windows (\r\n) line endings
      // Remove empty trailing line that results from splitting on line ending at end of file
      String[] lines = content.split("\\r?\\n", -1);
      List<String> result = new ArrayList<>(Arrays.asList(lines));

      // Remove the last empty line if the file ends with a line separator
      if (!result.isEmpty() && result.get(result.size() - 1).isEmpty()) {
        result.remove(result.size() - 1);
      }

      return result;
    } catch (Exception e) {
      if (e instanceof HdfsClientException) {
        throw e;
      }
      throw new HdfsClientException("Failed to read all lines from: " + hdfsPath, e);
    }
  }

  @Override
  public void delete(String hdfsPath) {
    requireNameNodeClient();
    try {
      nameNodeClient.delete(hdfsPath);
    } catch (Exception e) {
      throw new HdfsClientException("Failed to delete: " + hdfsPath, e);
    }
  }

  @Override
  public boolean deleteIfExists(String hdfsPath) {
    requireNameNodeClient();

    try {
      nameNodeClient.delete(hdfsPath);
      return true; // Delete succeeded
    } catch (Exception e) {
      // Check if the error is because the file doesn't exist
      try {
        Optional<HdfsFileSummary> fileExists = nameNodeClient.readAttributesOptional(hdfsPath);
        if (fileExists.isPresent()) {
          // File exists but delete failed for another reason
          throw new HdfsClientException("Failed to delete existing file: " + hdfsPath, e);
        } else {
          // File doesn't exist, return false
          return false;
        }
      } catch (Exception attributeException) {
        // Infrastructure error while checking file existence
        throw new HdfsClientException(
            "Failed to verify file existence during delete: " + hdfsPath, attributeException);
      }
    }
  }

  @Override
  public void copy(String hdfsPath, OutputStream outputStream) throws IOException {
    requireNameNodeClient();

    // Validate that the path is absolute
    HdfsPaths.requireAbsolute(hdfsPath);

    if (outputStream == null) {
      throw new IllegalArgumentException("outputStream cannot be null");
    }

    try {
      // Get the file attributes and validate it exists and is not a directory
      Optional<HdfsFileSummary> fileSummaryOpt = nameNodeClient.readAttributesOptional(hdfsPath);
      HdfsFileSummary fileSummary =
          fileSummaryOpt.orElseThrow(
              () -> new HdfsFileNotFoundException("File not found: " + hdfsPath));

      if (fileSummary.isDirectory()) {
        throw new HdfsClientException("Path is a directory, not a file: " + hdfsPath);
      }

      // Convert HdfsFileSummary to LocatedFile for DataNode client
      LocatedFile locatedFile = convertToLocatedFile(fileSummary);

      // Use DataNode client to copy the file to the output stream
      // We need to read each block from its respective DataNode
      copyLocatedFileToOutputStream(locatedFile, outputStream);
    } catch (HdfsFileNotFoundException e) {
      throw e;
    } catch (IOException e) {
      // Pass through IOException (from OutputStream errors in DataNode client)
      throw e;
    } catch (Exception e) {
      // Wrap other HDFS infrastructure errors
      throw new HdfsClientException("Failed to copy file from HDFS: " + hdfsPath, e);
    }
  }

  @Override
  public void copy(String hdfsPath, InputStream input) throws IOException {
    requireNameNodeClient();

    // Validate that the path is absolute
    HdfsPaths.requireAbsolute(hdfsPath);

    if (input == null) {
      throw new IllegalArgumentException("InputStream cannot be null");
    }

    // Check if the file already exists
    try {
      Optional<HdfsFileSummary> existingFile = nameNodeClient.readAttributesOptional(hdfsPath);
      if (existingFile.isPresent()) {
        // File exists - this is an HDFS validation error
        throw new HdfsClientException("File already exists: " + hdfsPath);
      }
      // File doesn't exist, which is what we want for creation
      // Continue with file creation
    } catch (HdfsClientException e) {
      throw e;
    } catch (Exception e) {
      // Infrastructure error while checking file existence
      throw new HdfsClientException(
          "Failed to verify file existence before creation: " + hdfsPath, e);
    }

    try {
      // Step 1: Create the file (block locations will be null initially)
      HdfsFileSummary fileSummary =
          nameNodeClient.create(hdfsPath, true, (short) replicationFactor, blockSize);

      // Step 2: Add the first block explicitly
      fileSummary = nameNodeClient.completeBlockAndAddNext(fileSummary);

      if (fileSummary.getBlockLocations() == null || fileSummary.getBlockLocations().isEmpty()) {
        throw new HdfsClientException(
            "Failed to get block location after adding first block for file: " + hdfsPath);
      }

      // Step 3: Write data to blocks
      HdfsFileSummary currentFileSummary = fileSummary;
      long totalBytesWritten = 0;

      try {
        // Wrap input with PushbackInputStream to peek for more data
        PushbackInputStream pushbackInput = new PushbackInputStream(input, 1);

        while (true) {
          // Use single byte read to check if there's more data
          int nextByte = pushbackInput.read();
          if (nextByte == -1) {
            // End of input stream reached
            break;
          }

          // Push the byte back since we only wanted to check
          pushbackInput.unread(nextByte);

          // Check if we need a new block (after the first block is written)
          if (totalBytesWritten > 0 && totalBytesWritten % blockSize == 0) {
            // Complete current block and add a new block to the file
            currentFileSummary = nameNodeClient.completeBlockAndAddNext(currentFileSummary);
            if (currentFileSummary.getBlockLocations() == null
                || currentFileSummary.getBlockLocations().isEmpty()) {
              throw new HdfsClientException("Failed to add new block to file: " + hdfsPath);
            }
          }

          // Get the current (last) block to write to
          HdfsFileSummary.BlockLocation lastBlockLocation =
              currentFileSummary
                  .getBlockLocations()
                  .get(currentFileSummary.getBlockLocations().size() - 1);

          // Convert to LocatedBlock for DataNode client
          LocatedBlock locatedBlock = convertBlockLocationToLocatedBlock(lastBlockLocation);

          // Wrap the input stream with BlockSizeLimitInputStream to limit bytes per block
          BlockSizeLimitInputStream blockLimitedInput =
              new BlockSizeLimitInputStream(pushbackInput, blockSize);

          // Get DataNodeClient for the first host of this block (with hostname mapping)
          String targetHost = mapDockerHostToLocalhost(locatedBlock.getHosts().get(0));
          try (DataNodeClient dataNodeClient = dataNodeClientProvider.getClient(targetHost)) {
            // Write the data to the DataNode (limited to block size)
            long bytesWrittenToBlock = dataNodeClient.copy(locatedBlock, blockLimitedInput);

            // If no bytes were written, break to avoid infinite loop
            if (bytesWrittenToBlock == 0) {
              break;
            }

            // Update the last block location with actual bytes written
            HdfsFileSummary.BlockLocation updatedBlockLocation =
                lastBlockLocation.toBuilder()
                    .length(bytesWrittenToBlock) // Set to actual bytes written
                    .build();

            // Create updated block locations list
            List<HdfsFileSummary.BlockLocation> updatedBlockLocations =
                new ArrayList<>(currentFileSummary.getBlockLocations());
            updatedBlockLocations.set(
                updatedBlockLocations.size() - 1, updatedBlockLocation); // Update last block

            // Create new currentFileSummary with updated block location
            currentFileSummary =
                currentFileSummary.toBuilder().blockLocations(updatedBlockLocations).build();

            totalBytesWritten += bytesWrittenToBlock;
          }
        }

        // Step 4: Complete the file
        boolean completed = nameNodeClient.complete(currentFileSummary);
        if (!completed) {
          throw new HdfsClientException("Failed to complete file: " + hdfsPath);
        }

      } catch (IOException e) {
        // IOException from InputStream operations (reading from pushbackInput) - pass through
        throw e;
      } catch (Exception e) {
        // HDFS infrastructure errors - wrap in HdfsClientException
        throw new HdfsClientException("Failed to copy data to HDFS file: " + hdfsPath, e);
      }
    } catch (HdfsClientException e) {
      throw e;
    } catch (Exception e) {
      // HDFS infrastructure errors from file creation and initial setup
      throw new HdfsClientException("Failed to create HDFS file: " + hdfsPath, e);
    }
  }

  /** Converts HdfsFileSummary to LocatedFile for use with DataNode client. */
  private LocatedFile convertToLocatedFile(HdfsFileSummary fileSummary) {
    if (fileSummary.getBlockLocations() == null || fileSummary.getBlockLocations().isEmpty()) {
      // Empty file
      return LocatedFile.builder()
          .fileName(fileSummary.getName())
          .blockPoolId("")
          .locatedBlocks(Collections.emptyList())
          .build();
    }

    // Get block pool ID from NameNode server info, or use the one from block locations
    String blockPoolId = fileSummary.getBlockLocations().get(0).getPoolId();
    if (blockPoolId == null || blockPoolId.isEmpty()) {
      try {
        blockPoolId = nameNodeClient.getBuildVersion().getBlockPoolID();
      } catch (Exception e) {
        // Fallback to empty string if server info is not available
        blockPoolId = "";
      }
    }

    // Convert block locations
    List<LocatedBlock> locatedBlocks = new ArrayList<>();

    for (HdfsFileSummary.BlockLocation blockLocation : fileSummary.getBlockLocations()) {
      // Convert hostnames
      List<String> hosts = new ArrayList<>();
      for (String host : blockLocation.getHosts()) {
        // Map Docker container hostnames to localhost for integration testing
        String resolvedHost = mapDockerHostToLocalhost(host);
        hosts.add(resolvedHost);
      }

      if (!hosts.isEmpty()) {
        LocatedBlock locatedBlock =
            LocatedBlock.builder()
                .blockId(blockLocation.getBlockId())
                .generationStamp(blockLocation.getGenerationStamp())
                .poolId(blockLocation.getPoolId())
                .hosts(hosts)
                .offset(blockLocation.getOffset())
                .length(blockLocation.getLength())
                .build();

        locatedBlocks.add(locatedBlock);
      }
    }

    return LocatedFile.builder()
        .fileName(fileSummary.getName())
        .blockPoolId(blockPoolId)
        .locatedBlocks(locatedBlocks)
        .build();
  }

  /**
   * Copies the contents of a LocatedFile to an OutputStream by reading each block from its
   * respective DataNode hosts.
   *
   * @param locatedFile the file with block location information
   * @param outputStream the stream to write the file content to
   * @throws IOException if there's an error writing to the output stream
   */
  private void copyLocatedFileToOutputStream(LocatedFile locatedFile, OutputStream outputStream)
      throws IOException {
    for (LocatedBlock block : locatedFile.getLocatedBlocks()) {
      // Try each host for this block until one succeeds
      boolean blockRead = false;

      for (String host : block.getHosts()) {
        String mappedHost = mapDockerHostToLocalhost(host);

        try (DataNodeClient dataNodeClient = dataNodeClientProvider.getClient(mappedHost)) {
          dataNodeClient.copy(block, outputStream);
          blockRead = true;
          break; // Successfully read from this host
        } catch (Exception e) {
          // Try next host if this one fails
          log.debug("Failed to read block {} from host {}", block.getBlockId(), mappedHost, e);
        }
      }

      if (!blockRead) {
        throw new HdfsClientException(
            "Failed to read block " + block.getBlockId() + " from any DataNode host");
      }
    }
  }

  /**
   * Maps DataNode hostnames to localhost when local mode is enabled. When localMode is true, all
   * hostnames are mapped to localhost for containerized testing. When localMode is false, only
   * specific Docker container hostnames and internal IPs are mapped. This allows the client to
   * connect to DataNodes running in Docker containers that are exposed on localhost ports.
   *
   * @param host the original hostname from HDFS metadata
   * @return 'localhost' if mapping is needed, or the original host if no mapping is required
   */
  private String mapDockerHostToLocalhost(String host) {
    // If local mode is enabled, always return localhost
    if (localMode) {
      return "localhost";
    }
    // Return the original host if no mapping is needed
    return host;
  }

  /** Converts HdfsFileSummary.BlockLocation to LocatedBlock for DataNode operations. */
  private LocatedBlock convertBlockLocationToLocatedBlock(
      HdfsFileSummary.BlockLocation blockLocation) {
    // Convert hostnames
    List<String> hosts = new ArrayList<>();
    for (String host : blockLocation.getHosts()) {
      // Map Docker container hostnames to localhost for integration testing
      String resolvedHost = mapDockerHostToLocalhost(host);
      hosts.add(resolvedHost);
    }

    if (hosts.isEmpty()) {
      throw new HdfsClientException(
          "No valid DataNode hosts found for block: " + blockLocation.getBlockId());
    }

    return LocatedBlock.builder()
        .blockId(blockLocation.getBlockId())
        .generationStamp(blockLocation.getGenerationStamp())
        .poolId(blockLocation.getPoolId())
        .hosts(hosts)
        .offset(blockLocation.getOffset())
        .length(blockLocation.getLength())
        .build();
  }
}
