package io.valier.hdfs.dn;

import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class LocatedFile {

  /** Name of the file. */
  String fileName;

  /** Block pool ID associated with this file. */
  String blockPoolId;

  /** List of located blocks that make up this file. */
  List<LocatedBlock> locatedBlocks;
}
