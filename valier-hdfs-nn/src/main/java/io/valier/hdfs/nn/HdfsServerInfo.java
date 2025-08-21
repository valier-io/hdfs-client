package io.valier.hdfs.nn;

import lombok.Builder;
import lombok.Value;

/**
 * Represents HDFS server information retrieved from a NameNode. This class encapsulates server
 * metadata including version information, block pool ID, and capabilities.
 *
 * <p>The data is typically retrieved from a VersionResponseProto and provides essential information
 * about the HDFS cluster configuration.
 */
@Value
@Builder
public class HdfsServerInfo {

  /**
   * Software revision version (e.g. an SVN or Git revision). This represents the specific build or
   * commit that was used to create the server binary.
   */
  String buildVersion;

  /**
   * Block pool ID used by the namespace. This uniquely identifies the block pool associated with
   * this NameNode.
   */
  String blockPoolID;

  /**
   * Software version number (e.g. 2.0.0, 3.3.4). This represents the semantic version of the HDFS
   * software.
   */
  String softwareVersion;

  /**
   * Feature flags representing server capabilities. This is a bitmask that indicates what features
   * are supported by the server. Defaults to 0 if not provided by the server.
   */
  @Builder.Default long capabilities = 0L;
}
