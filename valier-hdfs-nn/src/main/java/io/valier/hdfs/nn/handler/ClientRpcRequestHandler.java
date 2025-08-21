package io.valier.hdfs.nn.handler;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import io.valier.hdfs.nn.HdfsRpcRequestHandler;
import io.valier.hdfs.nn.connection.HdfsConnection;
import java.io.IOException;

/**
 * Client-specific RPC request handler that uses the Client protocol. This class extends the
 * abstract HdfsRpcRequestHandler and provides a simplified interface for Client protocol
 * communications.
 */
public class ClientRpcRequestHandler extends HdfsRpcRequestHandler {

  private static final String CLIENT_PROTOCOL_NAME =
      "org.apache.hadoop.hdfs.protocol.ClientProtocol";
  private static final int CLIENT_PROTOCOL_VERSION = 1;

  /**
   * Sends a protobuf request and returns the response as ByteString. This method uses the hardcoded
   * Client protocol settings.
   *
   * @param message the protobuf message to send
   * @param streams the connection streams containing input/output streams
   * @param methodName the RPC method name (e.g., "getListing")
   * @return ByteString containing the response protobuf message data
   * @throws IOException if there's an error during RPC communication or parsing
   */
  public ByteString sendRequestAndGetResponseBytes(
      GeneratedMessageV3 message,
      HdfsConnection.NameNodeConnectionStreams streams,
      String methodName)
      throws IOException {

    return super.sendRequestAndGetResponseBytes(
        message, streams, methodName, CLIENT_PROTOCOL_NAME, CLIENT_PROTOCOL_VERSION);
  }

  /**
   * Sends a protobuf request and returns the response as ByteString. This method uses the hardcoded
   * Client protocol settings and automatically derives the method name from the message class name.
   *
   * @param message the protobuf message to send
   * @param streams the connection streams containing input/output streams
   * @return ByteString containing the response protobuf message data
   * @throws IOException if there's an error during RPC communication or parsing
   */
  public ByteString sendRequestAndGetResponseBytes(
      GeneratedMessageV3 message, HdfsConnection.NameNodeConnectionStreams streams)
      throws IOException {

    return super.sendRequestAndGetResponseBytes(
        message, streams, CLIENT_PROTOCOL_NAME, CLIENT_PROTOCOL_VERSION);
  }
}
