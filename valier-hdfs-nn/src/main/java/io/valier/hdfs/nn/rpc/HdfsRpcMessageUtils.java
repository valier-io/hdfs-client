package io.valier.hdfs.nn.rpc;

import com.google.protobuf.GeneratedMessageV3;
import io.valier.hdfs.nn.connection.HdfsConnection;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

/**
 * Utility class for handling HDFS RPC message communication and response processing. Provides
 * methods for sending protobuf requests and parsing responses following the Hadoop RPC protocol.
 */
public class HdfsRpcMessageUtils {

  /**
   * Sends a protobuf request message and parses the response from NameNode. This method handles the
   * complete RPC request/response cycle: 1. Sends the request using writeDelimitedTo() 2. Reads
   * response length from the input stream 3. Creates a buffer and reads the response data 4. Parses
   * RpcResponseHeaderProto and checks for errors 5. Parses and returns the response message of the
   * specified type
   *
   * @param streams the connection streams containing input/output streams
   * @param request the protobuf request message to send
   * @param responseClass the class of the expected response message
   * @param <T> the type of the response message
   * @return the parsed response message
   * @throws IOException if there's an error during RPC communication, parsing, or RPC failure
   */
  public static <T extends GeneratedMessageV3> T sendRequestAndParseResponse(
      HdfsConnection.NameNodeConnectionStreams streams,
      GeneratedMessageV3 request,
      Class<T> responseClass)
      throws IOException {

    try {
      // 1. Send the request.
      // The writeDelimitedTo() method automatically writes the message's
      // length first, then the message data.
      request.writeDelimitedTo(streams.getOutputStream());
      streams.getOutputStream().flush();

      // 2. Read the response size and create buffer
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

      // 3. Parse RPC response header first
      // Create input stream from buffer for parsing delimited messages
      java.io.ByteArrayInputStream bufferStream = new java.io.ByteArrayInputStream(responseBuffer);

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

      // 4. Parse the actual response message
      // We use reflection to call the static parseDelimitedFrom(InputStream)
      // method on the generated Protobuf response class. This is necessary
      // because static methods cannot be called on a generic type T directly.
      Method parserMethod = responseClass.getMethod("parseDelimitedFrom", InputStream.class);

      // The first argument to invoke() is null because it's a static method.
      Object response = parserMethod.invoke(null, bufferStream);

      if (response == null) {
        throw new IOException(
            "Failed to parse response message of type: " + responseClass.getSimpleName());
      }

      // 5. Cast the parsed object to the correct type and return it.
      return responseClass.cast(response);

    } catch (IOException e) {
      throw e; // Re-throw IOException as-is
    } catch (Exception e) {
      throw new IOException("Failed to send request and parse response: " + e.getMessage(), e);
    }
  }

  /**
   * Parses a response from NameNode when the request has already been sent. This method handles
   * response parsing only: 1. Reads response length from the input stream 2. Creates a buffer and
   * reads the response data 3. Parses RpcResponseHeaderProto and checks for errors 4. Parses and
   * returns the response message of the specified type
   *
   * @param streams the connection streams containing input/output streams
   * @param responseClass the class of the expected response message
   * @param <T> the type of the response message
   * @return the parsed response message
   * @throws IOException if there's an error during parsing or RPC failure
   */
  public static <T extends GeneratedMessageV3> T parseResponse(
      HdfsConnection.NameNodeConnectionStreams streams, Class<T> responseClass) throws IOException {

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
      // Create input stream from buffer for parsing delimited messages
      java.io.ByteArrayInputStream bufferStream = new java.io.ByteArrayInputStream(responseBuffer);

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

      // Parse the actual response message
      // We use reflection to call the static parseDelimitedFrom(InputStream)
      // method on the generated Protobuf response class.
      Method parserMethod = responseClass.getMethod("parseDelimitedFrom", InputStream.class);

      // The first argument to invoke() is null because it's a static method.
      Object response = parserMethod.invoke(null, bufferStream);

      if (response == null) {
        throw new IOException(
            "Failed to parse response message of type: " + responseClass.getSimpleName());
      }

      // Cast the parsed object to the correct type and return it.
      return responseClass.cast(response);

    } catch (IOException e) {
      throw e; // Re-throw IOException as-is
    } catch (Exception e) {
      throw new IOException("Failed to parse response: " + e.getMessage(), e);
    }
  }
}
