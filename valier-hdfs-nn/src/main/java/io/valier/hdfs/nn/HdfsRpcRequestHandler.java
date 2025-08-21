package io.valier.hdfs.nn;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import io.valier.hdfs.nn.connection.HdfsConnection;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.*;

/**
 * Abstract stateful class that handles HDFS RPC request/response communication. Manages call IDs
 * and provides methods for sending protobuf requests and parsing responses according to the Hadoop
 * RPC protocol.
 */
public abstract class HdfsRpcRequestHandler {

  private final AtomicInteger callId = new AtomicInteger(0);
  private final UUID clientId;

  /** Default constructor that generates a random UUID for clientId. */
  public HdfsRpcRequestHandler() {
    this(UUID.randomUUID());
  }

  /** Package-private constructor for testing that accepts a specific clientId. */
  HdfsRpcRequestHandler(UUID clientId) {
    this.clientId = clientId;
  }

  /**
   * Sends a protobuf request and returns the response as ByteString. This method creates the RPC
   * and request headers, serializes everything to the OutputStream, then parses the response and
   * returns the protobuf message portion as a ByteString for the caller to parse.
   *
   * @param message the protobuf message to send
   * @param streams the connection streams containing input/output streams
   * @param methodName the RPC method name (e.g., "versionRequest", "getListing")
   * @param protocolName the protocol name (e.g.,
   *     "org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol")
   * @param protocolVersion the protocol version
   * @return ByteString containing the response protobuf message data
   * @throws IOException if there's an error during RPC communication or parsing
   */
  public ByteString sendRequestAndGetResponseBytes(
      GeneratedMessageV3 message,
      HdfsConnection.NameNodeConnectionStreams streams,
      String methodName,
      String protocolName,
      int protocolVersion)
      throws IOException {

    // Send the request
    sendRequest(message, streams, methodName, protocolName, protocolVersion);

    // Parse response and return the message bytes
    return parseResponseBytes(streams);
  }

  /**
   * Sends a protobuf request and returns the response as ByteString. The method name is
   * automatically derived from the message class name. For example: "VersionRequestProto" ->
   * "versionRequest", "GetListingRequestProto" -> "getListing"
   *
   * @param message the protobuf message to send
   * @param streams the connection streams containing input/output streams
   * @param protocolName the protocol name (e.g.,
   *     "org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol")
   * @param protocolVersion the protocol version
   * @return ByteString containing the response protobuf message data
   * @throws IOException if there's an error during RPC communication or parsing
   */
  public ByteString sendRequestAndGetResponseBytes(
      GeneratedMessageV3 message,
      HdfsConnection.NameNodeConnectionStreams streams,
      String protocolName,
      int protocolVersion)
      throws IOException {

    String methodName = parseMethodNameFromMessage(message);

    return sendRequestAndGetResponseBytes(
        message, streams, methodName, protocolName, protocolVersion);
  }

  /**
   * Parses the method name from the protobuf message class name. Converts class names like
   * "VersionRequestProto" to "versionRequest" and "GetListingRequestProto" to "getListing".
   */
  private String parseMethodNameFromMessage(GeneratedMessageV3 message) {
    String className = message.getClass().getSimpleName();

    // Remove "Proto" suffix if present
    if (className.endsWith("Proto")) {
      className = className.substring(0, className.length() - 5);
    }

    // Remove "Request" suffix if present
    if (className.endsWith("Request")) {
      className = className.substring(0, className.length() - 7);
    }

    // Convert to camelCase: "GetListing" -> "getListing"
    if (!className.isEmpty()) {
      return Character.toLowerCase(className.charAt(0)) + className.substring(1);
    }

    return className;
  }

  /** Sends a protobuf request using the Hadoop RPC protocol format. */
  private synchronized void sendRequest(
      GeneratedMessageV3 message,
      HdfsConnection.NameNodeConnectionStreams streams,
      String methodName,
      String protocolName,
      int protocolVersion)
      throws IOException {

    DataOutputStream out = streams.getOutputStream();
    int currentCallId = callId.getAndIncrement();

    // Create RPC request header
    RpcRequestHeaderProto rpcRequestHeader =
        RpcRequestHeaderProto.newBuilder()
            .setRpcKind(RpcKindProto.RPC_PROTOCOL_BUFFER)
            .setRpcOp(RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET)
            .setCallId(currentCallId)
            .setClientId(ByteString.copyFrom(getClientId()))
            .setRetryCount(0)
            .build();

    // Create request header with method and protocol information
    RequestHeaderProto requestHeader =
        RequestHeaderProto.newBuilder()
            .setMethodName(methodName)
            .setDeclaringClassProtocolName(protocolName)
            .setClientProtocolVersion(protocolVersion)
            .build();

    // Use ByteArrayOutputStream to build the complete message first
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    // Write RpcRequestHeaderProto delimited to buffer
    rpcRequestHeader.writeDelimitedTo(buffer);

    // Write RequestHeaderProto delimited to buffer
    requestHeader.writeDelimitedTo(buffer);

    // Write message delimited to buffer
    message.writeDelimitedTo(buffer);

    // Get the complete message bytes
    byte[] completeMessage = buffer.toByteArray();

    // Send the complete RPC message with total length prefix
    out.writeInt(completeMessage.length);
    out.write(completeMessage);

    out.flush();
  }

  private byte[] getClientId() {
    return convertUuidToBytes(this.clientId);
  }

  private static byte[] convertUuidToBytes(UUID uuid) {
    ByteBuffer buf = ByteBuffer.wrap(new byte[16]);
    buf.putLong(uuid.getMostSignificantBits());
    buf.putLong(uuid.getLeastSignificantBits());
    return buf.array();
  }

  /**
   * Parses the response from NameNode and returns the protobuf message portion as ByteString. Uses
   * similar logic to HdfsRpcMessageUtils.parseResponse but returns the raw message bytes instead of
   * parsing them into a specific type.
   */
  private ByteString parseResponseBytes(HdfsConnection.NameNodeConnectionStreams streams)
      throws IOException {
    try {
      // Read the response size and create buffer
      DataInputStream in = streams.getInputStream();
      int responseLength = in.readInt();

      if (responseLength < 0) {
        throw new IOException("Invalid response length: " + responseLength);
      }

      if (responseLength == 0) {
        throw new IOException("Empty response from NameNode");
      }

      // Create buffer and read the response data
      byte[] responseBuffer = new byte[responseLength];
      in.readFully(responseBuffer);

      // Parse RPC response header first
      ByteArrayInputStream bufferStream = new ByteArrayInputStream(responseBuffer);

      // Read and validate RPC response header
      RpcResponseHeaderProto responseHeader =
          RpcResponseHeaderProto.parseDelimitedFrom(bufferStream);
      if (responseHeader == null) {
        throw new IOException("Failed to parse RPC response header");
      }

      if (responseHeader.getStatus() != RpcResponseHeaderProto.RpcStatusProto.SUCCESS) {
        String errorMessage =
            responseHeader.hasExceptionClassName()
                ? responseHeader.getExceptionClassName() + ": " + responseHeader.getErrorMsg()
                : "RPC call failed with status: " + responseHeader.getStatus();
        throw new IOException(errorMessage);
      }

      // Instead of parsing the actual message, return the remaining bytes as ByteString
      // The remaining bytes in bufferStream contain the delimited protobuf message
      byte[] remainingBytes = new byte[bufferStream.available()];
      int bytesRead = bufferStream.read(remainingBytes);

      if (bytesRead <= 0) {
        throw new IOException("No protobuf message data found after RPC response header");
      }

      return ByteString.copyFrom(remainingBytes, 0, bytesRead);

    } catch (IOException e) {
      throw e; // Re-throw IOException as-is
    } catch (Exception e) {
      throw new IOException("Failed to parse response bytes: " + e.getMessage(), e);
    }
  }
}
