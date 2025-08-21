package io.valier.hdfs.nn;

import com.google.protobuf.ByteString;
import io.valier.hdfs.crt.HdfsPaths;
import io.valier.hdfs.nn.auth.SimpleUserInformation;
import io.valier.hdfs.nn.auth.UserInformationProvider;
import io.valier.hdfs.nn.connection.HdfsConnection;
import io.valier.hdfs.nn.connection.HdfsProtoConnection;
import io.valier.hdfs.nn.ex.HdfsFileNotFoundException;
import io.valier.hdfs.nn.ex.NameNodeHdfsException;
import io.valier.hdfs.nn.handler.ClientRpcRequestHandler;
import io.valier.hdfs.nn.handler.NameNodeRpcRequestHandler;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.*;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.*;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.*;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos.*;

/**
 * Default implementation of NameNodeClient that communicates directly with HDFS NameNode using
 * protobuf messages without relying on full Hadoop libraries.
 *
 * <p>This implementation uses the official Hadoop protobuf definitions and follows the Hadoop RPC
 * protocol to provide directory listing functionality.
 */
@Slf4j
@Value
@Builder
public class DefaultNameNodeClient implements NameNodeClient {

  private static final String CLIENT_PROTOCOL_NAME =
      "org.apache.hadoop.hdfs.protocol.ClientProtocol";
  private static final int CLIENT_PROTOCOL_VERSION = 1;
  private static final String NAMENODE_PROTOCOL_NAME =
      "org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol";
  private static final int NAMENODE_PROTOCOL_VERSION = 1;

  /**
   * HDFS connection implementation for handling protocol communication. This is the primary
   * configuration for the client.
   */
  HdfsConnection hdfsConnection;

  /**
   * List of NameNode URIs in the format "hdfs://host:port". Used internally by the builder to
   * create the HdfsConnection.
   */
  @Singular("nameNodeUri")
  List<String> nameNodeUris;

  /**
   * Provider for user information used in HDFS operations. Used internally by the builder to create
   * the HdfsConnection.
   */
  @Builder.Default
  UserInformationProvider userInformationProvider = SimpleUserInformation::currentUser;

  /**
   * Unique client identifier used for all RPC requests to the NameNode. This is used to identify
   * the client session and ensure consistency across operations. Defaults to a random UUID if not
   * provided.
   */
  @Builder.Default UUID clientId = UUID.randomUUID();

  /**
   * Client name used to identify this client instance in HDFS operations. Defaults to
   * valier-hdfs-client with a random number and thread ID.
   */
  @Builder.Default
  String clientName =
      "valier-hdfs-nn-client-"
          + Thread.currentThread().getId()
          + "-"
          + ThreadLocalRandom.current().nextInt();

  NameNodeRpcRequestHandler nameNodeRpcHandler = new NameNodeRpcRequestHandler();
  ClientRpcRequestHandler clientRpcHandler = new ClientRpcRequestHandler();

  /**
   * Lists files and directories in the specified path from the NameNode. This method implements the
   * ClientNamenodeProtocol.getListing RPC call and returns detailed file information similar to JDK
   * NIO Files.list().
   *
   * @param path The directory path to list (e.g., "/", "/user", "/tmp")
   * @return A stream of HdfsFileSummary objects containing file metadata
   * @throws IOException If there's an error communicating with the NameNode
   */
  @Override
  public Stream<HdfsFileSummary> list(String path) {
    requireHdfsConnection();

    // Since HdfsConnection might handle multiple URIs internally, we delegate to it
    return getDirectoryListingFromConnection(path).stream();
  }

  /**
   * Gets the server information from the NameNode. This method implements the
   * NamenodeProtocol.versionRequest RPC call.
   *
   * @return HdfsServerInfo containing server metadata including version information
   * @throws IOException If there's an error communicating with the NameNode
   */
  @Override
  public HdfsServerInfo getBuildVersion() {
    requireHdfsConnection();

    return getBuildVersionFromConnection();
  }

  /**
   * Creates a new file in HDFS and returns the file summary. This method implements the
   * ClientNamenodeProtocol.create RPC call.
   *
   * @param src The path where the file should be created
   * @param createParent Whether to create parent directories if they don't exist
   * @param replication The replication factor for the file
   * @param blockSize The block size for the file
   * @return HdfsFileSummary containing the created file's metadata
   * @throws IOException If there's an error communicating with the NameNode or creating the file
   */
  @Override
  public HdfsFileSummary create(
      String src, boolean createParent, short replication, long blockSize) {
    requireHdfsConnection();

    return createFileFromConnection(src, createParent, replication, blockSize);
  }

  /**
   * Adds a new block to an existing file in HDFS and returns the updated file summary. This method
   * implements the ClientNamenodeProtocol.addBlock RPC call.
   *
   * @param target The existing file to add a block to
   * @return HdfsFileSummary containing the updated file metadata with new block locations
   * @throws IOException If there's an error communicating with the NameNode or adding the block
   */
  @Override
  public HdfsFileSummary completeBlockAndAddNext(HdfsFileSummary target) {
    requireHdfsConnection();

    if (target == null) {
      throw new IllegalArgumentException("Target file summary cannot be null");
    }

    return addBlockToFileFromConnection(target);
  }

  /**
   * Creates a new directory in HDFS. The directory creation is atomic with respect to other
   * filesystem activities. The parent directory must already exist.
   *
   * @param path The path where the directory should be created
   * @return HdfsFileSummary containing the created directory's metadata
   * @throws IOException If there's an error communicating with the NameNode, the directory already
   *     exists, or the parent directory doesn't exist
   */
  @Override
  public HdfsFileSummary createDirectory(String path) {
    requireHdfsConnection();

    return createDirectoryFromConnection(path, false);
  }

  /**
   * Creates a directory by creating all nonexistent parent directories first. The directory
   * creation is atomic for each individual directory but the overall operation may leave the file
   * system in a partially created state if it fails.
   *
   * @param path The path where the directory (and any necessary parent directories) should be
   *     created
   * @return HdfsFileSummary containing the target directory's metadata
   * @throws IOException If there's an error communicating with the NameNode
   */
  @Override
  public HdfsFileSummary createDirectories(String path) {
    requireHdfsConnection();

    return createDirectoryFromConnection(path, true);
  }

  /**
   * Reads a file's attributes as a bulk operation. This method provides functionality similar to
   * Java NIO Files.readAttributes() but always returns HDFS-specific metadata in the form of an
   * HdfsFileSummary object.
   *
   * @param path The path to the file or directory whose attributes should be read
   * @return HdfsFileSummary containing the file's metadata and attributes
   * @throws IOException If there's an error communicating with the NameNode or the file doesn't
   *     exist
   */
  @Override
  public Optional<HdfsFileSummary> readAttributesOptional(String path) {
    requireHdfsConnection();
    return readAttributesOptionalFromConnection(path);
  }

  @Override
  public HdfsFileSummary readAttributes(String path) {
    requireHdfsConnection();
    return readAttributesOptional(path)
        .orElseThrow(() -> new HdfsFileNotFoundException("File not found: " + path));
  }

  /**
   * Completes a file in HDFS, marking it as fully written and ready for reading. This method
   * implements the ClientNamenodeProtocol.complete RPC call and should be called after the last
   * packet of the last block has been written and acknowledged by the DataNodes.
   *
   * @param target The file to complete
   * @return true if the file was successfully completed, false otherwise
   * @throws IOException If there's an error communicating with the NameNode or completing the file
   */
  @Override
  public boolean complete(HdfsFileSummary target) {
    requireHdfsConnection();

    if (target == null) {
      throw new IllegalArgumentException("Target file summary cannot be null");
    }

    // Get the last block length from the target's block locations
    long lastBlockLength = 0;
    if (target.getBlockLocations() != null && !target.getBlockLocations().isEmpty()) {
      HdfsFileSummary.BlockLocation lastBlock =
          target.getBlockLocations().get(target.getBlockLocations().size() - 1);
      lastBlockLength = lastBlock.getLength();
    }

    if (lastBlockLength < 0) {
      throw new IllegalArgumentException("Last block length cannot be negative");
    }

    return completeFileFromConnection(target, lastBlockLength);
  }

  /** Gets the server information using the configured HdfsConnection. */
  private HdfsServerInfo getBuildVersionFromConnection() {
    if (nameNodeUris == null || nameNodeUris.isEmpty()) {
      throw new IllegalStateException("No NameNode URIs configured in DefaultNameNodeClient");
    }

    Exception lastException = null;

    // Try each NameNode URI until one succeeds
    for (String nameNodeUri : nameNodeUris) {
      try {
        return getBuildVersionFromUri(nameNodeUri);
      } catch (Exception e) {
        lastException = e;
        // Continue to next NameNode if available
      }
    }

    throw new NameNodeHdfsException(
        "Failed to get server information from any NameNode", lastException);
  }

  /** Gets a directory listing using the configured HdfsConnection. */
  private List<HdfsFileSummary> getDirectoryListingFromConnection(String path) {
    if (nameNodeUris == null || nameNodeUris.isEmpty()) {
      throw new IllegalStateException("No NameNode URIs configured in DefaultNameNodeClient");
    }

    Exception lastException = null;

    // Try each NameNode URI until one succeeds
    for (String nameNodeUri : nameNodeUris) {
      try {
        return getDirectoryListingFromUri(nameNodeUri, path);
      } catch (HdfsFileNotFoundException e) {
        // File not found is a valid response, don't loop to other NameNodes
        throw e;
      } catch (Exception e) {
        lastException = e;
        // Continue to next NameNode if available
      }
    }

    throw new NameNodeHdfsException(
        "Failed to get directory listing from any NameNode for path: " + path, lastException);
  }

  /** Gets a directory listing from a specific NameNode URI. */
  private List<HdfsFileSummary> getDirectoryListingFromUri(String nameNodeUri, String path) {
    try (HdfsConnection.NameNodeConnectionStreams streams = hdfsConnection.connect(nameNodeUri)) {
      // Create getListing request
      // NOTE: This currently only retrieves the first page of results (up to 1000 entries).
      // The HDFS protocol supports paging through large directories using the startAfter parameter,
      // but this implementation will need to be updated in the future to iterate through all pages
      // to support directories with more than 1000 entries.
      GetListingRequestProto getListingRequest =
          GetListingRequestProto.newBuilder()
              .setSrc(path)
              .setStartAfter(ByteString.EMPTY)
              .setNeedLocation(true)
              .build();

      // Use the Client RPC handler to send request and get response bytes
      ByteString responseBytes =
          clientRpcHandler.sendRequestAndGetResponseBytes(getListingRequest, streams);

      // Parse the response from the ByteString
      GetListingResponseProto response =
          GetListingResponseProto.parseDelimitedFrom(responseBytes.newInput());

      if (response == null) {
        throw new NameNodeHdfsException(
            "Failed to parse GetListingResponseProto from response bytes");
      }

      return extractFileSummariesFromResponse(response);

    } catch (Exception e) {
      throw new NameNodeHdfsException(
          "Failed to get directory listing from NameNode at " + nameNodeUri + " for path: " + path,
          e);
    }
  }

  /** Gets the server information from a specific NameNode URI. */
  private HdfsServerInfo getBuildVersionFromUri(String nameNodeUri) {
    try (HdfsConnection.NameNodeConnectionStreams streams = hdfsConnection.connect(nameNodeUri)) {
      // Create versionRequest (empty request)
      VersionRequestProto versionRequest = VersionRequestProto.newBuilder().build();

      // Use the NameNode RPC handler to send request and get response bytes
      ByteString responseBytes =
          nameNodeRpcHandler.sendRequestAndGetResponseBytes(
              versionRequest,
              streams,
              // Usually the 'Request' is not part of the method name, but it is here for some
              // reason
              "versionRequest");

      // Parse the response from the ByteString
      VersionResponseProto response =
          VersionResponseProto.parseDelimitedFrom(responseBytes.newInput());

      if (response == null) {
        throw new NameNodeHdfsException("Failed to parse VersionResponseProto from response bytes");
      }

      return extractServerInfoFromResponse(response);

    } catch (Exception e) {
      throw new NameNodeHdfsException(
          "Failed to get server information from NameNode at " + nameNodeUri, e);
    }
  }

  /** Extracts HdfsFileSummary objects from a GetListingResponseProto. */
  private List<HdfsFileSummary> extractFileSummariesFromResponse(GetListingResponseProto response) {
    List<HdfsFileSummary> fileSummaries = new ArrayList<>();

    if (response.hasDirList()) {
      DirectoryListingProto dirList = response.getDirList();

      for (HdfsFileStatusProto fileStatus : dirList.getPartialListingList()) {
        HdfsFileSummary summary = convertToHdfsFileSummary(fileStatus, null);
        if (summary != null) {
          fileSummaries.add(summary);
        }
      }
    }

    return fileSummaries;
  }

  /** Extracts server information from a VersionResponseProto. */
  private HdfsServerInfo extractServerInfoFromResponse(VersionResponseProto response) {
    if (!response.hasInfo()) {
      throw new IllegalStateException("VersionResponseProto does not contain namespace info");
    }

    NamespaceInfoProto info = response.getInfo();

    HdfsServerInfo.HdfsServerInfoBuilder builder = HdfsServerInfo.builder();

    // Extract required fields
    if (info.hasBuildVersion()) {
      builder.buildVersion(info.getBuildVersion());
    } else {
      throw new IllegalStateException("VersionResponseProto does not contain build version");
    }

    if (info.hasBlockPoolID()) {
      builder.blockPoolID(info.getBlockPoolID());
    } else {
      throw new IllegalStateException("VersionResponseProto does not contain block pool ID");
    }

    if (info.hasSoftwareVersion()) {
      builder.softwareVersion(info.getSoftwareVersion());
    } else {
      throw new IllegalStateException("VersionResponseProto does not contain software version");
    }

    // Extract optional capabilities field
    if (info.hasCapabilities()) {
      builder.capabilities(info.getCapabilities());
    }

    return builder.build();
  }

  /** Converts a HdfsFileStatusProto to an HdfsFileSummary. */
  private HdfsFileSummary convertToHdfsFileSummary(
      HdfsFileStatusProto fileStatus, String requestedPath) {
    try {
      // Use the requested path if available, otherwise fall back to path from protobuf
      String fullPath = requestedPath;
      if (fullPath == null || fullPath.isEmpty()) {
        fullPath = fileStatus.getPath().toStringUtf8();
      }

      String fileName = HdfsPaths.getName(fullPath);

      // Determine file type
      HdfsFileSummary.FileType fileType;
      if (fileStatus.getFileType() == HdfsFileStatusProto.FileType.IS_DIR) {
        fileType = HdfsFileSummary.FileType.DIRECTORY;
      } else if (fileStatus.getFileType() == HdfsFileStatusProto.FileType.IS_SYMLINK) {
        fileType = HdfsFileSummary.FileType.SYMLINK;
      } else {
        fileType = HdfsFileSummary.FileType.FILE;
      }

      // Build HdfsFileSummary
      HdfsFileSummary.HdfsFileSummaryBuilder builder =
          HdfsFileSummary.builder()
              .fileType(fileType)
              .name(fileName)
              .path(fullPath)
              .length(fileStatus.getLength())
              .permissions(fileStatus.getPermission().getPerm())
              .owner(fileStatus.getOwner())
              .group(fileStatus.getGroup())
              .modificationTime(Instant.ofEpochMilli(fileStatus.getModificationTime()))
              .accessTime(Instant.ofEpochMilli(fileStatus.getAccessTime()))
              .blockReplication(fileStatus.getBlockReplication())
              .blockSize(fileStatus.getBlocksize())
              .fileId(fileStatus.getFileId())
              .childrenCount(fileStatus.getChildrenNum())
              .storagePolicy(fileStatus.getStoragePolicy())
              .flags(fileStatus.getFlags());

      // Add symlink target if present
      if (fileStatus.hasSymlink()) {
        builder.symlinkTarget(fileStatus.getSymlink().toStringUtf8());
      }

      // Add namespace if present
      if (fileStatus.hasNamespace()) {
        builder.namespace(fileStatus.getNamespace());
      }

      // Convert block locations if present
      if (fileStatus.hasLocations()) {
        LocatedBlocksProto locations = fileStatus.getLocations();
        List<HdfsFileSummary.BlockLocation> blockLocations = new ArrayList<>();

        for (LocatedBlockProto locatedBlock : locations.getBlocksList()) {
          ExtendedBlockProto block = locatedBlock.getB();
          List<String> hosts = new ArrayList<>();
          List<String> names = new ArrayList<>();
          List<String> topologyPaths = new ArrayList<>();

          for (DatanodeInfoProto datanode : locatedBlock.getLocsList()) {
            hosts.add(datanode.getId().getHostName());
            names.add(datanode.getId().getDatanodeUuid());
            if (datanode.hasLocation()) {
              topologyPaths.add(datanode.getLocation());
            }
          }

          blockLocations.add(
              HdfsFileSummary.BlockLocation.builder()
                  .offset(locatedBlock.getOffset())
                  .length(block.getNumBytes())
                  .poolId(block.getPoolId())
                  .blockId(block.getBlockId())
                  .generationStamp(block.getGenerationStamp())
                  .hosts(hosts)
                  .names(names)
                  .topologyPaths(topologyPaths)
                  .build());
        }

        builder.blockLocations(blockLocations);
      }

      return builder.build();

    } catch (Exception e) {
      // Log and skip problematic files rather than failing the entire listing
      log.error("Warning: Failed to convert file status to HdfsFileSummary: {}", e.getMessage());
      return null;
    }
  }

  /** Creates a file using the configured HdfsConnection. */
  private HdfsFileSummary createFileFromConnection(
      String src, boolean createParent, short replication, long blockSize) {
    if (nameNodeUris == null || nameNodeUris.isEmpty()) {
      throw new IllegalStateException("No NameNode URIs configured in DefaultNameNodeClient");
    }

    Exception lastException = null;

    // Try each NameNode URI until one succeeds
    for (String nameNodeUri : nameNodeUris) {
      try {
        return createFileFromUri(nameNodeUri, src, createParent, replication, blockSize);
      } catch (Exception e) {
        lastException = e;
        // Continue to next NameNode if available
      }
    }

    throw new NameNodeHdfsException(
        "Failed to create file from any NameNode for path: " + src, lastException);
  }

  /** Creates a file from a specific NameNode URI. */
  private HdfsFileSummary createFileFromUri(
      String nameNodeUri, String src, boolean createParent, short replication, long blockSize) {
    try (HdfsConnection.NameNodeConnectionStreams streams = hdfsConnection.connect(nameNodeUri)) {

      // Create file creation request
      CreateRequestProto createRequest =
          CreateRequestProto.newBuilder()
              .setSrc(src)
              .setMasked(
                  FsPermissionProto.newBuilder().setPerm(0644).build()) // Default file permissions
              .setClientName(this.clientName)
              .setCreateFlag(
                  createParent ? CreateFlagProto.CREATE_VALUE : CreateFlagProto.CREATE_VALUE)
              .setCreateParent(createParent)
              .setReplication(replication)
              .setBlockSize(blockSize)
              .build();

      // Use the Client RPC handler to send request and get response bytes
      ByteString responseBytes =
          clientRpcHandler.sendRequestAndGetResponseBytes(createRequest, streams);

      // Parse the response from the ByteString
      CreateResponseProto response =
          CreateResponseProto.parseDelimitedFrom(responseBytes.newInput());

      if (response == null) {
        throw new NameNodeHdfsException("Failed to parse CreateResponseProto from response bytes");
      }

      return extractHdfsFileSummaryFromCreateResponse(response, src);

    } catch (Exception e) {
      throw new NameNodeHdfsException(
          "Failed to create file at NameNode " + nameNodeUri + " for path: " + src, e);
    }
  }

  /** Extracts HdfsFileSummary from a CreateResponseProto. */
  private HdfsFileSummary extractHdfsFileSummaryFromCreateResponse(
      CreateResponseProto response, String src) {
    if (!response.hasFs()) {
      throw new NameNodeHdfsException("CreateResponseProto does not contain file status");
    }

    HdfsFileStatusProto fileStatus = response.getFs();

    // Use the existing convertToHdfsFileSummary method
    HdfsFileSummary summary = convertToHdfsFileSummary(fileStatus, src);
    if (summary == null) {
      throw new NameNodeHdfsException("Failed to convert file status to HdfsFileSummary");
    }

    return summary;
  }

  /** Adds a block to a file using the configured HdfsConnection. */
  private HdfsFileSummary addBlockToFileFromConnection(HdfsFileSummary target) {
    if (nameNodeUris == null || nameNodeUris.isEmpty()) {
      throw new IllegalStateException("No NameNode URIs configured in DefaultNameNodeClient");
    }

    Exception lastException = null;

    // Try each NameNode URI until one succeeds
    for (String nameNodeUri : nameNodeUris) {
      try {
        return addBlockToFileFromUri(nameNodeUri, target);
      } catch (Exception e) {
        lastException = e;
        // Continue to next NameNode if available
      }
    }

    throw new NameNodeHdfsException(
        "Failed to add block from any NameNode for path: " + target.getPath(), lastException);
  }

  /** Adds a block to a file from a specific NameNode URI. */
  private HdfsFileSummary addBlockToFileFromUri(String nameNodeUri, HdfsFileSummary target) {
    try (HdfsConnection.NameNodeConnectionStreams streams = hdfsConnection.connect(nameNodeUri)) {

      // Build the previous block info if the file has existing blocks
      ExtendedBlockProto.Builder previousBlockBuilder = null;
      if (target.getBlockLocations() != null && !target.getBlockLocations().isEmpty()) {
        // Get the last block as the previous block
        HdfsFileSummary.BlockLocation lastBlock =
            target.getBlockLocations().get(target.getBlockLocations().size() - 1);
        previousBlockBuilder =
            ExtendedBlockProto.newBuilder()
                .setPoolId(lastBlock.getPoolId())
                .setBlockId(lastBlock.getBlockId())
                .setGenerationStamp(lastBlock.getGenerationStamp())
                .setNumBytes(lastBlock.getLength());
      }

      // Create addBlock request
      AddBlockRequestProto.Builder requestBuilder =
          AddBlockRequestProto.newBuilder()
              .setSrc(target.getPath())
              .setClientName(this.clientName)
              .setFileId(target.getFileId());

      if (previousBlockBuilder != null) {
        requestBuilder.setPrevious(previousBlockBuilder.build());
      }

      AddBlockRequestProto addBlockRequest = requestBuilder.build();

      // Use the Client RPC handler to send request and get response bytes
      ByteString responseBytes =
          clientRpcHandler.sendRequestAndGetResponseBytes(addBlockRequest, streams);

      // Parse the response from the ByteString
      AddBlockResponseProto response =
          AddBlockResponseProto.parseDelimitedFrom(responseBytes.newInput());

      if (response == null) {
        throw new NameNodeHdfsException(
            "Failed to parse AddBlockResponseProto from response bytes");
      }

      return extractHdfsFileSummaryFromAddBlockResponse(response, target);

    } catch (Exception e) {
      throw new NameNodeHdfsException(
          "Failed to add block at NameNode " + nameNodeUri + " for path: " + target.getPath(), e);
    }
  }

  /**
   * Extracts HdfsFileSummary from an AddBlockResponseProto, updating the target with new block
   * information.
   */
  private HdfsFileSummary extractHdfsFileSummaryFromAddBlockResponse(
      AddBlockResponseProto response, HdfsFileSummary target) {
    if (!response.hasBlock()) {
      throw new NameNodeHdfsException("AddBlockResponseProto does not contain block information");
    }

    LocatedBlockProto newBlock = response.getBlock();
    ExtendedBlockProto block = newBlock.getB();

    // Create new block location
    List<String> hosts = new ArrayList<>();
    List<String> names = new ArrayList<>();
    List<String> topologyPaths = new ArrayList<>();

    for (DatanodeInfoProto datanode : newBlock.getLocsList()) {
      hosts.add(datanode.getId().getHostName());
      names.add(datanode.getId().getDatanodeUuid());
      if (datanode.hasLocation()) {
        topologyPaths.add(datanode.getLocation());
      }
    }

    HdfsFileSummary.BlockLocation newBlockLocation =
        HdfsFileSummary.BlockLocation.builder()
            .offset(newBlock.getOffset())
            .length(block.getNumBytes())
            .poolId(block.getPoolId())
            .blockId(block.getBlockId())
            .generationStamp(block.getGenerationStamp())
            .hosts(hosts)
            .names(names)
            .topologyPaths(topologyPaths)
            .build();

    // Create updated block locations list
    List<HdfsFileSummary.BlockLocation> updatedBlockLocations = new ArrayList<>();
    if (target.getBlockLocations() != null) {
      updatedBlockLocations.addAll(target.getBlockLocations());
    }
    updatedBlockLocations.add(newBlockLocation);

    // Return updated HdfsFileSummary with new block location
    return target.toBuilder().blockLocations(updatedBlockLocations).build();
  }

  /** Completes a file using the configured HdfsConnection. */
  private boolean completeFileFromConnection(HdfsFileSummary target, long lastBlockLength) {
    if (nameNodeUris == null || nameNodeUris.isEmpty()) {
      throw new IllegalStateException("No NameNode URIs configured in DefaultNameNodeClient");
    }

    Exception lastException = null;

    // Try each NameNode URI until one succeeds
    for (String nameNodeUri : nameNodeUris) {
      try {
        return completeFileFromUri(nameNodeUri, target, lastBlockLength);
      } catch (Exception e) {
        lastException = e;
        // Continue to next NameNode if available
      }
    }

    throw new NameNodeHdfsException(
        "Failed to complete file from any NameNode for path: " + target.getPath(), lastException);
  }

  /** Completes a file from a specific NameNode URI. */
  private boolean completeFileFromUri(
      String nameNodeUri, HdfsFileSummary target, long lastBlockLength) {
    try (HdfsConnection.NameNodeConnectionStreams streams = hdfsConnection.connect(nameNodeUri)) {

      // Build the last block info with the final length
      ExtendedBlockProto.Builder lastBlockBuilder = null;
      if (target.getBlockLocations() != null && !target.getBlockLocations().isEmpty()) {
        // Get the last block
        HdfsFileSummary.BlockLocation lastBlock =
            target.getBlockLocations().get(target.getBlockLocations().size() - 1);
        lastBlockBuilder =
            ExtendedBlockProto.newBuilder()
                .setPoolId(lastBlock.getPoolId())
                .setBlockId(lastBlock.getBlockId())
                .setGenerationStamp(lastBlock.getGenerationStamp())
                .setNumBytes(lastBlockLength); // Use the provided final length
      }

      // Create complete request
      CompleteRequestProto.Builder requestBuilder =
          CompleteRequestProto.newBuilder()
              .setSrc(target.getPath())
              .setClientName(this.clientName)
              .setFileId(target.getFileId());

      if (lastBlockBuilder != null) {
        requestBuilder.setLast(lastBlockBuilder.build());
      }

      CompleteRequestProto completeRequest = requestBuilder.build();

      // Use the Client RPC handler to send request and get response bytes
      ByteString responseBytes =
          clientRpcHandler.sendRequestAndGetResponseBytes(completeRequest, streams);

      // Parse the response from the ByteString
      CompleteResponseProto response =
          CompleteResponseProto.parseDelimitedFrom(responseBytes.newInput());

      if (response == null) {
        throw new NameNodeHdfsException(
            "Failed to parse CompleteResponseProto from response bytes");
      }

      return response.getResult();

    } catch (Exception e) {
      throw new NameNodeHdfsException(
          "Failed to complete file at NameNode " + nameNodeUri + " for path: " + target.getPath(),
          e);
    }
  }

  /**
   * Creates a builder for configuring and creating DefaultNameNodeClient instances.
   *
   * @return a new DefaultNameNodeClientBuilder instance
   */
  public static DefaultNameNodeClientBuilder builder() {
    return new DefaultNameNodeClientBuilder() {
      @Override
      public DefaultNameNodeClient build() {
        DefaultNameNodeClient client = super.build();

        // If no hdfsConnection was provided but we have nameNodeUris, create the connection
        if (client.hdfsConnection == null
            && client.nameNodeUris != null
            && !client.nameNodeUris.isEmpty()) {
          HdfsConnection connection =
              HdfsProtoConnection.builder()
                  .userInformationProvider(
                      client.userInformationProvider != null
                          ? client.userInformationProvider
                          : SimpleUserInformation::currentUser)
                  .build();

          // Create a new instance with the connection
          return new DefaultNameNodeClient(
              connection,
              client.nameNodeUris,
              client.userInformationProvider,
              client.clientId,
              client.clientName);
        }

        // If still no connection, throw error
        if (client.hdfsConnection == null) {
          throw new IllegalStateException("Either hdfsConnection or nameNodeUri must be provided");
        }

        return client;
      }
    };
  }

  /** Creates a directory using the configured HdfsConnection. */
  private HdfsFileSummary createDirectoryFromConnection(String path, boolean createParents) {
    if (nameNodeUris == null || nameNodeUris.isEmpty()) {
      throw new IllegalStateException("No NameNode URIs configured in DefaultNameNodeClient");
    }

    Exception lastException = null;

    // Try each NameNode URI until one succeeds
    for (String nameNodeUri : nameNodeUris) {
      try {
        return createDirectoryFromUri(nameNodeUri, path, createParents);
      } catch (Exception e) {
        lastException = e;
        // Continue to next NameNode if available
      }
    }

    throw new NameNodeHdfsException(
        "Failed to create directory from any NameNode for path: " + path, lastException);
  }

  /** Creates a directory from a specific NameNode URI. */
  private HdfsFileSummary createDirectoryFromUri(
      String nameNodeUri, String path, boolean createParents) {
    try (HdfsConnection.NameNodeConnectionStreams streams = hdfsConnection.connect(nameNodeUri)) {
      // Create directory creation request
      MkdirsRequestProto mkdirsRequest =
          MkdirsRequestProto.newBuilder()
              .setSrc(path)
              .setMasked(
                  FsPermissionProto.newBuilder()
                      .setPerm(0755)
                      .build()) // Default directory permissions
              .setCreateParent(createParents)
              .build();

      // Use the Client RPC handler to send request and get response bytes
      ByteString responseBytes =
          clientRpcHandler.sendRequestAndGetResponseBytes(mkdirsRequest, streams);

      // Parse the response from the ByteString
      MkdirsResponseProto response =
          MkdirsResponseProto.parseDelimitedFrom(responseBytes.newInput());

      if (response == null) {
        throw new NameNodeHdfsException("Failed to parse MkdirsResponseProto from response bytes");
      }

      if (!response.getResult()) {
        throw new NameNodeHdfsException("Directory creation failed for path: " + path);
      }

      // After successful creation, get the directory information
      return getDirectoryInfo(streams, path);

    } catch (Exception e) {
      throw new NameNodeHdfsException(
          "Failed to create directory at NameNode " + nameNodeUri + " for path: " + path, e);
    }
  }

  /**
   * Gets directory information after successful creation.
   *
   * <p>This method expects the provided streams to be open and does not close them. The caller is
   * responsible for managing the lifecycle of the streams object.
   */
  private HdfsFileSummary getDirectoryInfo(
      HdfsConnection.NameNodeConnectionStreams streams, String path) {
    try {
      // Create getFileInfo request to get the created directory's metadata
      GetFileInfoRequestProto getFileInfoRequest =
          GetFileInfoRequestProto.newBuilder().setSrc(path).build();

      // Use the Client RPC handler to send request and get response bytes
      ByteString responseBytes =
          clientRpcHandler.sendRequestAndGetResponseBytes(getFileInfoRequest, streams);

      // Parse the response from the ByteString
      GetFileInfoResponseProto response =
          GetFileInfoResponseProto.parseDelimitedFrom(responseBytes.newInput());

      if (response == null) {
        throw new NameNodeHdfsException(
            "Failed to parse GetFileInfoResponseProto from response bytes");
      }

      if (!response.hasFs()) {
        throw new NameNodeHdfsException("Created directory not found: " + path);
      }

      HdfsFileSummary summary = convertToHdfsFileSummary(response.getFs(), path);
      if (summary == null) {
        throw new NameNodeHdfsException("Failed to convert directory status to HdfsFileSummary");
      }

      return summary;
    } catch (NameNodeHdfsException e) {
      throw e;
    } catch (Exception e) {
      throw new NameNodeHdfsException("Failed to get directory info for path: " + path, e);
    }
  }

  /** Reads file attributes using the configured HdfsConnection, returning Optional. */
  private Optional<HdfsFileSummary> readAttributesOptionalFromConnection(String path) {
    if (nameNodeUris == null || nameNodeUris.isEmpty()) {
      throw new IllegalStateException("No NameNode URIs configured in DefaultNameNodeClient");
    }

    Exception lastException = null;

    // Try each NameNode URI until one succeeds
    for (String nameNodeUri : nameNodeUris) {
      try {
        return readAttributesFromUri(nameNodeUri, path);
      } catch (Exception e) {
        lastException = e;
        // Continue to next NameNode if available
      }
    }

    throw new NameNodeHdfsException(
        "Failed to read attributes from any NameNode for path: " + path, lastException);
  }

  /** Reads file attributes from a specific NameNode URI, returning Optional. */
  private Optional<HdfsFileSummary> readAttributesFromUri(String nameNodeUri, String path) {
    try (HdfsConnection.NameNodeConnectionStreams streams = hdfsConnection.connect(nameNodeUri)) {
      // Create getLocatedFileInfo request to get the file's metadata with block locations
      GetLocatedFileInfoRequestProto getLocatedFileInfoRequest =
          GetLocatedFileInfoRequestProto.newBuilder().setSrc(path).setNeedBlockToken(false).build();

      // Use the Client RPC handler to send request and get response bytes
      ByteString responseBytes =
          clientRpcHandler.sendRequestAndGetResponseBytes(getLocatedFileInfoRequest, streams);

      // Parse the response from the ByteString
      GetLocatedFileInfoResponseProto response =
          GetLocatedFileInfoResponseProto.parseDelimitedFrom(responseBytes.newInput());

      if (response == null) {
        throw new NameNodeHdfsException(
            "Failed to parse GetLocatedFileInfoResponseProto from response bytes");
      }

      if (!response.hasFs()) {
        // File not found via HDFS API - return empty Optional instead of throwing
        return Optional.empty();
      }

      HdfsFileSummary summary = convertToHdfsFileSummary(response.getFs(), path);
      if (summary == null) {
        throw new NameNodeHdfsException("Failed to convert file status to HdfsFileSummary");
      }

      return Optional.of(summary);

    } catch (NameNodeHdfsException e) {
      throw e;
    } catch (Exception e) {
      throw new NameNodeHdfsException(
          "Failed to read attributes from NameNode " + nameNodeUri + " for path: " + path, e);
    }
  }

  @Override
  public void delete(String path) {
    requireHdfsConnection();
    deleteFromConnection(path);
  }

  /** Deletes a file or directory using the configured HdfsConnection. */
  private void deleteFromConnection(String path) {
    if (nameNodeUris == null || nameNodeUris.isEmpty()) {
      throw new IllegalStateException("No NameNode URIs configured in DefaultNameNodeClient");
    }

    Exception lastException = null;

    // Try each NameNode URI until one succeeds
    for (String nameNodeUri : nameNodeUris) {
      try {
        deleteFromUri(nameNodeUri, path);
        return; // Success
      } catch (Exception e) {
        lastException = e;
        // Continue to next NameNode if available
      }
    }

    throw new NameNodeHdfsException(
        "Failed to delete from any NameNode for path: " + path, lastException);
  }

  /** Deletes a file or directory from a specific NameNode URI. */
  private void deleteFromUri(String nameNodeUri, String path) {
    try (HdfsConnection.NameNodeConnectionStreams streams = hdfsConnection.connect(nameNodeUri)) {
      // Create delete request
      DeleteRequestProto deleteRequest =
          DeleteRequestProto.newBuilder()
              .setSrc(path)
              .setRecursive(false) // Similar to Files.delete(), only delete if empty directory
              .build();

      // Use the Client RPC handler to send request and get response bytes
      ByteString responseBytes =
          clientRpcHandler.sendRequestAndGetResponseBytes(deleteRequest, streams);

      // Parse the response from the ByteString
      DeleteResponseProto response =
          DeleteResponseProto.parseDelimitedFrom(responseBytes.newInput());

      if (response == null) {
        throw new NameNodeHdfsException("Failed to parse DeleteResponseProto from response bytes");
      }

      if (!response.getResult()) {
        throw new NameNodeHdfsException("Delete operation failed for path: " + path);
      }

    } catch (Exception e) {
      throw new NameNodeHdfsException(
          "Failed to delete from NameNode " + nameNodeUri + " for path: " + path, e);
    }
  }

  /**
   * Validates that the HdfsConnection is configured and throws an exception if it's null.
   *
   * @throws IllegalStateException if hdfsConnection is null
   */
  private void requireHdfsConnection() {
    if (hdfsConnection == null) {
      throw new IllegalStateException("No HdfsConnection configured");
    }
  }
}
