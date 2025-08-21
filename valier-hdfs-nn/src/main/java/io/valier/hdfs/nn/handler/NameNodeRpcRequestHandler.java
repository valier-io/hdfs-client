package io.valier.hdfs.nn.handler;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import io.valier.hdfs.nn.HdfsRpcRequestHandler;
import io.valier.hdfs.nn.connection.HdfsConnection;
import java.io.IOException;

/**
 * NameNode-specific RPC request handler that uses the NameNode protocol. This class extends the
 * abstract HdfsRpcRequestHandler and provides a simplified interface for NameNode protocol
 * communications.
 */
public class NameNodeRpcRequestHandler extends HdfsRpcRequestHandler {

  private static final String NAMENODE_PROTOCOL_NAME =
      "org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol";
  private static final int NAMENODE_PROTOCOL_VERSION = 1;

  /**
   * Sends a protobuf request and returns the response as ByteString. This method uses the hardcoded
   * NameNode protocol settings.
   *
   * @param message the protobuf message to send
   * @param streams the connection streams containing input/output streams
   * @param methodName the RPC method name (e.g., "versionRequest")
   * @return ByteString containing the response protobuf message data
   * @throws IOException if there's an error during RPC communication or parsing
   */
  public ByteString sendRequestAndGetResponseBytes(
      GeneratedMessageV3 message,
      HdfsConnection.NameNodeConnectionStreams streams,
      String methodName)
      throws IOException {

    return super.sendRequestAndGetResponseBytes(
        message, streams, methodName, NAMENODE_PROTOCOL_NAME, NAMENODE_PROTOCOL_VERSION);
  }
}
