package io.valier.hdfs.dn;

import io.valier.hdfs.dn.ex.DataNodeHdfsException;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.*;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.*;

/**
 * Default implementation of DataNodeClient that communicates with HDFS DataNodes using the native
 * HDFS Data Transfer Protocol.
 *
 * <p>This implementation establishes direct TCP connections to DataNodes and uses the official
 * Hadoop protobuf definitions for block transfer operations.
 */
@Slf4j
@Data
public class DefaultDataNodeClient implements DataNodeClient {

  /** Data Transfer Protocol version supported by this client. */
  private static final int DATA_TRANSFER_VERSION = 28;

  /** Operation code for READ_BLOCK operation. */
  private static final byte OP_READ_BLOCK = (byte) 81;

  /** Operation code for WRITE_BLOCK operation. */
  private static final byte OP_WRITE_BLOCK = (byte) 80;

  /** Buffer size for data transfer operations (64KB). */
  private static final int BUFFER_SIZE = 65536;

  /** Default DataNode port for data transfer operations. */
  private static final int DEFAULT_DATANODE_PORT = 9866;

  /** Hostname of the DataNode to connect to. */
  private final String hostname;

  /** Port of the DataNode to connect to. */
  private final int port;

  /** Connection timeout in milliseconds. */
  private final int connectionTimeoutMs;

  /** Socket read timeout in milliseconds. */
  private final int readTimeoutMs;

  /**
   * Client name used for HDFS Data Transfer Protocol operations. This identifies the client in
   * protocol messages and logs.
   */
  private final String clientName;

  /** Lazily initialized socket connection to the DataNode. */
  private transient Socket socket;

  /** Input stream from the DataNode socket. */
  private transient InputStream socketInputStream;

  /** Output stream to the DataNode socket. */
  private transient OutputStream socketOutputStream;

  @Builder
  public DefaultDataNodeClient(
      @NonNull String hostname,
      int port,
      int connectionTimeoutMs,
      int readTimeoutMs,
      String clientName) {
    this.hostname = hostname;
    this.port = port == 0 ? DEFAULT_DATANODE_PORT : port;
    this.connectionTimeoutMs = connectionTimeoutMs == 0 ? 5000 : connectionTimeoutMs;
    this.readTimeoutMs = readTimeoutMs == 0 ? 3000 : readTimeoutMs;
    this.clientName =
        clientName != null
            ? clientName
            : "valier-hdfs-dn-client-"
                + Thread.currentThread().getId()
                + "-"
                + ThreadLocalRandom.current().nextInt();
  }

  @Override
  public synchronized void copy(LocatedBlock block, OutputStream out) throws IOException {
    if (block == null) {
      throw new IllegalArgumentException("LocatedBlock cannot be null");
    }
    if (out == null) {
      throw new IllegalArgumentException("OutputStream cannot be null");
    }

    // Verify this DataNode hosts the requested block
    boolean hostsBlock =
        block.getHosts().stream()
            .anyMatch(host -> host.equals(hostname) || host.equals(hostname + ":" + port));

    if (!hostsBlock) {
      throw new DataNodeHdfsException(
          "Block " + block.getBlockId() + " is not hosted on DataNode " + hostname + ":" + port);
    }

    try {
      ensureConnected();
      readBlockFromDataNode(block, hostname, out);
    } catch (IOException e) {
      // IOException from OutputStream operations - pass through
      throw e;
    } catch (Exception e) {
      throw new DataNodeHdfsException(
          "Failed to read block " + block.getBlockId() + " from DataNode " + hostname + ":" + port,
          e);
    }
  }

  @Override
  public synchronized long copy(LocatedBlock block, InputStream in) throws IOException {
    if (block == null) {
      throw new IllegalArgumentException("LocatedBlock cannot be null");
    }
    if (in == null) {
      throw new IllegalArgumentException("Data InputStream cannot be null");
    }

    // Verify this DataNode is a target for the block
    boolean isTarget =
        block.getHosts().stream()
            .anyMatch(host -> host.equals(hostname) || host.equals(hostname + ":" + port));

    if (!isTarget) {
      throw new DataNodeHdfsException(
          "DataNode " + hostname + ":" + port + " is not a target for block " + block.getBlockId());
    }

    try {
      ensureConnected();
      return writeBlockToDataNode(block, hostname, in);
    } catch (IOException e) {
      // IOException from InputStream operations - pass through
      throw e;
    } catch (Exception e) {
      throw new DataNodeHdfsException(
          "Failed to write block " + block.getBlockId() + " to DataNode " + hostname + ":" + port,
          e);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (socket != null && !socket.isClosed()) {
      try {
        if (socketInputStream != null) {
          socketInputStream.close();
        }
        if (socketOutputStream != null) {
          socketOutputStream.close();
        }
        socket.close();
      } finally {
        socket = null;
        socketInputStream = null;
        socketOutputStream = null;
      }
    }
  }

  /** Ensures a connection to the DataNode exists, creating one if necessary. */
  private synchronized void ensureConnected() throws IOException {
    if (socket == null || socket.isClosed()) {
      socket = new Socket();
      socket.connect(new InetSocketAddress(hostname, port), connectionTimeoutMs);
      socket.setSoTimeout(readTimeoutMs);

      socketInputStream = socket.getInputStream();
      socketOutputStream = socket.getOutputStream();

      log.debug("Connected to DataNode {}:{}", hostname, port);
    }
  }

  /** Reads a block from a specific DataNode using the HDFS Data Transfer Protocol. */
  private void readBlockFromDataNode(LocatedBlock locatedBlock, String host, OutputStream out)
      throws IOException {

    try {
      try (DataInputStream in = new DataInputStream(socketInputStream);
          DataOutputStream socketOut = new DataOutputStream(socketOutputStream)) {

        // Send data transfer protocol header
        sendDataTransferHeader(socketOut);

        // Send read block operation
        sendReadBlockOperation(socketOut, locatedBlock);

        // Read response and stream data
        readBlockResponse(in, out, locatedBlock.getLength());
      }
    } catch (OutputStreamIOException e) {
      // OutputStream errors should propagate as IOException - unwrap the original cause
      throw (IOException) e.getCause();
    } catch (IOException e) {
      // DataNode infrastructure errors should be wrapped as DataNodeHdfsException
      throw new DataNodeHdfsException(
          "Failed to read block " + locatedBlock.getBlockId() + " from DataNode " + host, e);
    }
  }

  /** Sends the data transfer protocol header. */
  private void sendDataTransferHeader(DataOutputStream out) throws IOException {
    // Write data transfer version
    out.writeShort(DATA_TRANSFER_VERSION);
  }

  /** Sends a read block operation request. */
  private void sendReadBlockOperation(DataOutputStream out, LocatedBlock locatedBlock)
      throws IOException {

    // Create extended block info
    ExtendedBlockProto extendedBlock =
        ExtendedBlockProto.newBuilder()
            .setPoolId(locatedBlock.getPoolId())
            .setBlockId(locatedBlock.getBlockId())
            .setNumBytes(locatedBlock.getLength())
            .setGenerationStamp(locatedBlock.getGenerationStamp())
            .build();

    // Create base header
    BaseHeaderProto baseHeader =
        BaseHeaderProto.newBuilder()
            .setBlock(extendedBlock)
            // Token can be omitted for insecure clusters
            .build();

    // Create client operation header
    ClientOperationHeaderProto header =
        ClientOperationHeaderProto.newBuilder()
            .setBaseHeader(baseHeader)
            .setClientName(clientName)
            .build();

    // Create read block operation
    OpReadBlockProto readBlockOp =
        OpReadBlockProto.newBuilder()
            .setHeader(header)
            .setOffset(0)
            .setLen(locatedBlock.getLength())
            .setSendChecksums(false)
            .setCachingStrategy(CachingStrategyProto.getDefaultInstance())
            .build();

    // Send operation code and request
    out.writeByte(OP_READ_BLOCK);
    readBlockOp.writeDelimitedTo(out);
    out.flush();
  }

  /** Reads the block response and streams data to output using HDFS packet format. */
  private void readBlockResponse(DataInputStream in, OutputStream out, long expectedLength)
      throws IOException {
    // Read operation response
    BlockOpResponseProto response = BlockOpResponseProto.parseDelimitedFrom(in);

    if (response == null) {
      throw new IOException("No response received from DataNode");
    }

    if (response.getStatus() != Status.SUCCESS) {
      throw new IOException(
          "DataNode returned error status: "
              + response.getStatus()
              + " - "
              + response.getMessage());
    }

    // Read data packets until the "last packet" flag is received
    long totalBytesRead = 0;
    boolean lastPacket = false;

    while (!lastPacket) {
      // Read packet length (4 bytes)
      int payloadLen = in.readInt();

      // Read header length (2 bytes)
      short headerLen = in.readShort();

      log.debug("Reading packet - payloadLength: {}, header length: {}", payloadLen, headerLen);

      // Read packet header
      byte[] headerData = new byte[headerLen];
      in.readFully(headerData);
      PacketHeaderProto header = PacketHeaderProto.parseFrom(headerData);

      // Check if this is the last packet in the block
      lastPacket = header.getLastPacketInBlock();

      int dataLen = header.getDataLen();

      if (dataLen > 0) {
        // Read packet data
        byte[] data = new byte[dataLen];
        in.readFully(data);

        // Write data to output stream - wrap OutputStream errors to distinguish them
        try {
          out.write(data);
        } catch (IOException e) {
          // Wrap OutputStream errors so they can be distinguished from DataNode errors
          throw new OutputStreamIOException("Failed to write to output stream", e);
        }
        totalBytesRead += dataLen;
      }
    }

    if (totalBytesRead != expectedLength) {
      throw new IOException("Expected " + expectedLength + " bytes, but read " + totalBytesRead);
    }
  }

  /** Writes a block to a specific DataNode using the HDFS Data Transfer Protocol. */
  private long writeBlockToDataNode(LocatedBlock block, String host, InputStream data)
      throws IOException {

    try {
      try (DataInputStream in = new DataInputStream(socketInputStream);
          DataOutputStream socketOut = new DataOutputStream(socketOutputStream)) {

        // Send data transfer protocol header
        sendDataTransferHeader(socketOut);

        // Send write block operation
        sendWriteBlockOperation(socketOut, block);

        // Read acknowledgment
        readWriteBlockResponse(in);

        // Send data packets with acknowledgments and return total bytes written
        return sendDataPackets(socketOut, in, data);
      }
    } catch (InputStreamIOException e) {
      // InputStream errors should propagate as IOException - unwrap the original cause
      throw (IOException) e.getCause();
    } catch (IOException e) {
      // DataNode infrastructure errors should be wrapped as DataNodeHdfsException
      throw new DataNodeHdfsException(
          "Failed to write block " + block.getBlockId() + " to DataNode " + host, e);
    }
  }

  /** Sends a write block operation request. */
  private void sendWriteBlockOperation(DataOutputStream out, LocatedBlock block)
      throws IOException {

    // Create extended block info - for new blocks, use 0 as initial size
    ExtendedBlockProto extendedBlock =
        ExtendedBlockProto.newBuilder()
            .setPoolId(block.getPoolId())
            .setBlockId(block.getBlockId())
            .setGenerationStamp(block.getGenerationStamp())
            .setNumBytes(0) // Initial size for new block
            .build();

    // Create base header
    BaseHeaderProto baseHeader =
        BaseHeaderProto.newBuilder()
            .setBlock(extendedBlock)
            // Token can be omitted for insecure clusters
            .build();

    // Create client operation header
    ClientOperationHeaderProto header =
        ClientOperationHeaderProto.newBuilder()
            .setBaseHeader(baseHeader)
            .setClientName(clientName)
            .build();

    // Create write block operation
    OpWriteBlockProto writeBlockOp =
        OpWriteBlockProto.newBuilder()
            .setHeader(header)
            .setStage(OpWriteBlockProto.BlockConstructionStage.PIPELINE_SETUP_CREATE)
            .setPipelineSize(block.getHosts().size())
            .setMinBytesRcvd(0)
            .setMaxBytesRcvd(0)
            .setLatestGenerationStamp(block.getGenerationStamp())
            .setRequestedChecksum(
                ChecksumProto.newBuilder()
                    .setType(ChecksumTypeProto.CHECKSUM_CRC32)
                    .setBytesPerChecksum(512)
                    .build())
            .setCachingStrategy(CachingStrategyProto.getDefaultInstance())
            .build();

    // Send operation code and request
    out.writeByte(OP_WRITE_BLOCK);
    writeBlockOp.writeDelimitedTo(out);
    out.flush();
  }

  /** Reads the write block response. */
  private void readWriteBlockResponse(DataInputStream in) throws IOException {
    // Read operation response
    BlockOpResponseProto response = BlockOpResponseProto.parseDelimitedFrom(in);

    if (response == null) {
      throw new IOException("No response received from DataNode");
    }

    if (response.getStatus() != Status.SUCCESS) {
      throw new IOException(
          "DataNode returned error status: "
              + response.getStatus()
              + " - "
              + response.getMessage());
    }
  }

  /** Sends data packets to the DataNode with acknowledgment handling. */
  private long sendDataPackets(DataOutputStream out, DataInputStream in, InputStream data)
      throws IOException {
    long totalBytesWritten = 0;
    long seqno = 0;

    byte[] dataBuffer = new byte[BUFFER_SIZE];

    // Send all data packets
    while (true) {
      // Read data from input stream - wrap InputStream errors to distinguish them
      int bytesRead;
      try {
        bytesRead = data.read(dataBuffer);
      } catch (IOException e) {
        // Wrap InputStream errors so they can be distinguished from DataNode errors
        throw new InputStreamIOException("Failed to read from input stream", e);
      }

      if (bytesRead == -1) {
        // End of stream reached
        break;
      }

      if (bytesRead == 0) {
        // No data read but not end of stream, continue
        continue;
      }

      // Create data packet with payload using ByteBuffer wrap to avoid copying
      DataPacket packet =
          DataPacket.builder()
              .offsetInBlock(totalBytesWritten)
              .sequenceNumber(seqno++)
              .lastPacket(false)
              .syncBlock(false)
              .calculateCrc(true)
              .payload(ByteBuffer.wrap(dataBuffer, 0, bytesRead))
              .build();

      // Write packet to output stream
      packet.writeTo(out);
      out.flush();

      // Wait for packet acknowledgment from DataNode
      readPacketAck(in, seqno - 1);

      totalBytesWritten += bytesRead;
    }

    // Always send an empty packet at the end marked as the last packet
    DataPacket lastPacket =
        DataPacket.builder()
            .offsetInBlock(totalBytesWritten)
            .sequenceNumber(seqno++)
            .lastPacket(true)
            .syncBlock(false)
            .calculateCrc(false)
            .payload(ByteBuffer.allocate(0))
            .build();

    // Write last packet to output stream
    lastPacket.writeTo(out);
    out.flush();

    // Wait for final packet acknowledgment from DataNode
    readPacketAck(in, seqno - 1);

    return totalBytesWritten;
  }

  /** Reads packet acknowledgment from DataNode. */
  private void readPacketAck(DataInputStream in, long expectedSeqno) throws IOException {
    // Read pipeline ACK protobuf message
    PipelineAckProto ack = PipelineAckProto.parseDelimitedFrom(in);

    if (ack == null) {
      throw new IOException("No acknowledgment received from DataNode");
    }

    // Check sequence number matches
    if (ack.getSeqno() != expectedSeqno) {
      throw new IOException("Expected seqno " + expectedSeqno + " but got " + ack.getSeqno());
    }

    // Check all replies are SUCCESS
    for (Status status : ack.getReplyList()) {
      if (status != Status.SUCCESS) {
        throw new IOException("DataNode returned error status in ACK: " + status);
      }
    }
  }
}
