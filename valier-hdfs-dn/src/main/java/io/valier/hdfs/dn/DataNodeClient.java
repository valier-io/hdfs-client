package io.valier.hdfs.dn;

import io.valier.hdfs.dn.ex.DataNodeHdfsException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface DataNodeClient extends AutoCloseable {

  /**
   * Reads a single block from this DataNode and writes it to the output stream. This method only
   * works if the block is located on this specific DataNode.
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, DataNode unavailability, protocol errors) are thrown as unchecked {@link
   * DataNodeHdfsException}s. Only failures related to writing to the target OutputStream are thrown
   * as checked {@link IOException}s, as these relate to the caller's environment.
   *
   * @param block the block to read from this DataNode
   * @param out the output stream to write the block data to
   * @throws IOException if there's an error writing to the output stream
   * @throws DataNodeHdfsException if this DataNode doesn't contain the requested block
   */
  void copy(LocatedBlock block, OutputStream out) throws IOException;

  /**
   * Writes data to a specific block using the Hadoop Data Transfer Protocol. This method streams
   * data from the provided InputStream to the specified block, reading until the InputStream
   * returns -1 (end of stream).
   *
   * <p>Since HDFS is designed to be highly available, failures related to HDFS infrastructure
   * (network connectivity, DataNode unavailability, protocol errors) are thrown as unchecked {@link
   * DataNodeHdfsException}s. Only failures related to reading from the source InputStream are
   * thrown as checked {@link IOException}s, as these relate to the caller's environment.
   *
   * @param block the LocatedBlock containing the block information to write to
   * @param in the InputStream containing the data to write
   * @return the total number of bytes written to the block
   * @throws IOException if there's an error reading from the source InputStream
   * @throws DataNodeHdfsException if there's an error with HDFS DataNode operations
   */
  long copy(LocatedBlock block, InputStream in) throws IOException;
}
