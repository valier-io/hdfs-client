package io.valier.hdfs.nn.connection;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Interface for HDFS connections that handle the low-level protocol communication with NameNode
 * servers. Implementations are responsible for establishing connections, sending protocol headers,
 * and managing the connection lifecycle.
 */
public interface HdfsConnection {

  /**
   * Establishes a connection to an HDFS NameNode and initializes the protocol. This method should
   * handle the complete connection setup including: - TCP socket connection - RPC header
   * transmission - Connection context setup
   *
   * @param nameNodeUri the URI of the NameNode to connect to (e.g., "hdfs://localhost:9000")
   * @return a ConnectionStreams object containing the input and output streams
   * @throws IOException if the connection cannot be established or initialized
   */
  NameNodeConnectionStreams connect(String nameNodeUri) throws IOException;

  /** Container for the input and output streams of an established connection. */
  interface NameNodeConnectionStreams extends AutoCloseable {
    DataInputStream getInputStream();

    DataOutputStream getOutputStream();

    void close() throws IOException;
  }
}
