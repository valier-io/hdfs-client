package io.valier.hdfs.nn.connection;

import com.google.protobuf.ByteString;
import io.valier.hdfs.nn.auth.SimpleUserInformation;
import io.valier.hdfs.nn.auth.UserInformation;
import io.valier.hdfs.nn.auth.UserInformationProvider;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import lombok.Builder;
import lombok.Value;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.UserInformationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.*;

/**
 * Implementation of HdfsConnection that uses the Hadoop RPC protocol over TCP sockets. This class
 * handles the low-level protocol communication with HDFS NameNodes, including connection
 * establishment, protocol header exchange, and connection context setup.
 */
@Value
@Builder
public class HdfsProtoConnection implements HdfsConnection {

  private static final String HADOOP_RPC_HEADER = "hrpc";
  private static final byte RPC_VERSION = 9;
  private static final byte RPC_SERVICE_CLASS = 0; // RPC_PROTOCOL_BUFFER
  private static final byte AUTH_PROTOCOL = 0; // Simple authentication

  private static final String CLIENT_PROTOCOL_NAME =
      "org.apache.hadoop.hdfs.protocol.ClientProtocol";
  private static final String CLIENT_ID = "valier-hdfs-client";

  /**
   * Provider for user information used in HDFS operations. If not provided, defaults to the current
   * system user.
   */
  @Builder.Default
  UserInformationProvider userInformationProvider = SimpleUserInformation::currentUser;

  /** Connection timeout in milliseconds. */
  @Builder.Default int connectTimeoutMs = 10000;

  /** Socket read timeout in milliseconds. */
  @Builder.Default int readTimeoutMs = 30000;

  @Override
  public NameNodeConnectionStreams connect(String nameNodeUri) throws IOException {
    URI uri = parseNameNodeUri(nameNodeUri);

    Socket socket = new Socket();
    try {
      // Connect to NameNode with timeout
      InetSocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
      socket.connect(address, connectTimeoutMs);
      socket.setSoTimeout(readTimeoutMs);

      DataInputStream in = new DataInputStream(socket.getInputStream());
      DataOutputStream out = new DataOutputStream(socket.getOutputStream());

      // Send RPC header
      sendRpcHeader(out);

      // Send connection context
      sendConnectionContext(out);

      return new HdfsConnectionStreams(socket, in, out);
    } catch (IOException e) {
      try {
        socket.close();
      } catch (IOException closeException) {
        e.addSuppressed(closeException);
      }
      throw new IOException("Failed to establish connection to NameNode at " + nameNodeUri, e);
    }
  }

  /** Parses a NameNode URI and validates its format. */
  private URI parseNameNodeUri(String nameNodeUri) throws IOException {
    try {
      URI uri = URI.create(nameNodeUri);
      if (!"hdfs".equals(uri.getScheme())) {
        throw new IOException(
            "Invalid NameNode URI scheme. Expected 'hdfs', got: " + uri.getScheme());
      }
      if (uri.getHost() == null || uri.getPort() == -1) {
        throw new IOException(
            "Invalid NameNode URI format. Expected 'hdfs://host:port', got: " + nameNodeUri);
      }
      return uri;
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid NameNode URI format: " + nameNodeUri, e);
    }
  }

  /** Sends the initial RPC header to establish the protocol version. */
  private void sendRpcHeader(DataOutputStream out) throws IOException {
    // Write "hrpc" magic bytes
    out.writeBytes(HADOOP_RPC_HEADER);

    // Write RPC version
    out.writeByte(RPC_VERSION);

    // Write service class (RPC_PROTOCOL_BUFFER)
    out.writeByte(RPC_SERVICE_CLASS);

    // Write auth protocol (SIMPLE)
    out.writeByte(AUTH_PROTOCOL);
  }

  /**
   * Sends the connection context following Hadoop RPC protocol. This sends RpcRequestHeaderProto
   * and IpcConnectionContextProto as separate delimited messages using protobuf's writeDelimitedTo
   * method.
   */
  private void sendConnectionContext(DataOutputStream out) throws IOException {
    // Get user information from provider
    UserInformation userInfo = userInformationProvider.getUserInformation();
    UserInformationProto userInfoProto =
        UserInformationProto.newBuilder()
            .setEffectiveUser(userInfo.getEffectiveUser())
            .setRealUser(userInfo.getUser())
            .build();

    // Create RPC request header for connection context (special call ID -3)
    RpcRequestHeaderProto contextHeader =
        RpcRequestHeaderProto.newBuilder()
            .setRpcKind(RpcKindProto.RPC_PROTOCOL_BUFFER)
            .setRpcOp(RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET)
            .setCallId(-3) // Special call ID for connection context
            .setClientId(ByteString.copyFromUtf8(CLIENT_ID))
            .setRetryCount(-1)
            .build();

    // Create connection context
    IpcConnectionContextProto connectionContext =
        IpcConnectionContextProto.newBuilder()
            .setUserInfo(userInfoProto)
            .setProtocol(CLIENT_PROTOCOL_NAME)
            .build();

    // Use ByteArrayOutputStream to capture the delimited output and calculate total length
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    contextHeader.writeDelimitedTo(baos);
    connectionContext.writeDelimitedTo(baos);

    byte[] delimitedBytes = baos.toByteArray();
    int totalLength = delimitedBytes.length;

    // Write total length and then the delimited messages
    out.writeInt(totalLength);
    out.write(delimitedBytes);
    out.flush();
  }

  /** Implementation of ConnectionStreams that manages socket lifecycle. */
  private static class HdfsConnectionStreams implements NameNodeConnectionStreams {
    private final Socket socket;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;

    public HdfsConnectionStreams(
        Socket socket, DataInputStream inputStream, DataOutputStream outputStream) {
      this.socket = socket;
      this.inputStream = inputStream;
      this.outputStream = outputStream;
    }

    @Override
    public DataInputStream getInputStream() {
      return inputStream;
    }

    @Override
    public DataOutputStream getOutputStream() {
      return outputStream;
    }

    @Override
    public void close() throws IOException {
      try {
        inputStream.close();
      } catch (IOException e) {
        // Continue with cleanup
      }
      try {
        outputStream.close();
      } catch (IOException e) {
        // Continue with cleanup
      }
      socket.close();
    }
  }
}
