package io.valier.hdfs.nn;

import java.time.Instant;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/**
 * Summary of an HDFS file or directory with essential metadata. This class mirrors the information
 * available in HdfsFileStatusProto and provides a cleaner API similar to JDK NIO Files operations.
 */
@Value
@Builder(toBuilder = true)
public class HdfsFileSummary {

  /** Type of the file entry (file, directory, or symlink). */
  FileType fileType;

  /** Name of the file or directory (not the full path). */
  String name;

  /** Full path of the file or directory. */
  String path;

  /** File size in bytes (0 for directories). */
  long length;

  /** HDFS permissions as an integer. */
  int permissions;

  /** Owner of the file or directory. */
  String owner;

  /** Group owner of the file or directory. */
  String group;

  /** Last modification time. */
  Instant modificationTime;

  /** Last access time. */
  Instant accessTime;

  /** Symlink target (only present for symlinks). */
  String symlinkTarget;

  /** Block replication factor (only present for files). */
  int blockReplication;

  /** Block size in bytes (only present for files). */
  long blockSize;

  /** File ID (internal HDFS identifier). */
  long fileId;

  /** Number of children (for directories). */
  int childrenCount;

  /** Storage policy ID. */
  int storagePolicy;

  /** Various flags (ACL, encryption, erasure coding, etc.). */
  int flags;

  /** Namespace (for federated HDFS). */
  String namespace;

  /** Block locations (only present if requested and for files). */
  List<BlockLocation> blockLocations;

  /** File type enumeration. */
  public enum FileType {
    DIRECTORY,
    FILE,
    SYMLINK
  }

  /** Block location information. */
  @Value
  @Builder(toBuilder = true)
  public static class BlockLocation {
    /** Block offset within the file. */
    long offset;

    /** Block length in bytes. */
    long length;

    /** Block pool ID that this block belongs to. */
    String poolId;

    /** Block ID within HDFS. */
    long blockId;

    /** Generation stamp for the block. */
    long generationStamp;

    /** Data node hostnames where this block is stored. */
    List<String> hosts;

    /** Data node names where this block is stored. */
    List<String> names;

    /** Topology paths for the data nodes. */
    List<String> topologyPaths;
  }

  /** Convenience method to check if this entry is a directory. */
  public boolean isDirectory() {
    return fileType == FileType.DIRECTORY;
  }

  /** Convenience method to check if this entry is a regular file. */
  public boolean isFile() {
    return fileType == FileType.FILE;
  }

  /** Convenience method to check if this entry is a symbolic link. */
  public boolean isSymlink() {
    return fileType == FileType.SYMLINK;
  }

  /** Returns a string representation of the permissions in Unix format (e.g., "rwxr-xr-x"). */
  public String getPermissionsString() {
    return formatPermissions(permissions);
  }

  private static String formatPermissions(int perm) {
    StringBuilder sb = new StringBuilder(9);

    // Owner permissions
    sb.append((perm & 0400) != 0 ? 'r' : '-');
    sb.append((perm & 0200) != 0 ? 'w' : '-');
    sb.append((perm & 0100) != 0 ? 'x' : '-');

    // Group permissions
    sb.append((perm & 0040) != 0 ? 'r' : '-');
    sb.append((perm & 0020) != 0 ? 'w' : '-');
    sb.append((perm & 0010) != 0 ? 'x' : '-');

    // Other permissions
    sb.append((perm & 0004) != 0 ? 'r' : '-');
    sb.append((perm & 0002) != 0 ? 'w' : '-');
    sb.append((perm & 0001) != 0 ? 'x' : '-');

    return sb.toString();
  }
}
