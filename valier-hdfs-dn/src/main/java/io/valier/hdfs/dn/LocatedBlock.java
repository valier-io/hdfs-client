package io.valier.hdfs.dn;

import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class LocatedBlock {

  /** Block ID within HDFS. */
  long blockId;

  /** Generation stamp for the block. */
  long generationStamp;

  /** Block pool ID for this block. */
  String poolId;

  /** List of DataNode hosts where this block is stored. */
  List<String> hosts;

  /** Offset of this block within the file. */
  long offset;

  /** Length of this block in bytes. */
  long length;
}
