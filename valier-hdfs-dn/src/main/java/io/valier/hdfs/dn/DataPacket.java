package io.valier.hdfs.dn;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PacketHeaderProto;

/**
 * Represents a data packet for HDFS Data Transfer Protocol.
 *
 * <p>Each packet follows an unusual and unexpected structure that differs from HDFS RPC protocols:
 *
 * <pre>
 *   PLEN    HLEN      HEADER     CHECKSUMS  DATA
 *   32-bit  16-bit   &lt;protobuf&gt;  &lt;variable length&gt;
 *
 *   PLEN:      Payload length
 *              = length(PLEN) + length(CHECKSUMS) + length(DATA)
 *              This length includes its own encoded length in
 *              the sum for historical reasons.
 *
 *   HLEN:      Header length
 *              = length(HEADER)
 *
 *   HEADER:    the actual packet header fields, encoded in protobuf
 *   CHECKSUMS: the crcs for the data chunk. May be missing if
 *              checksums were not requested
 *   DATA       the actual block data
 * </pre>
 *
 * <p><strong>Important:</strong> Unlike HDFS RPC protocols, PLEN is <em>not</em> the size of the
 * entire packet and does <em>not</em> include the size of the header (HLEN + HEADER). This makes
 * the data transfer protocol inconsistent with other HDFS protocols.
 *
 * <p><strong>Note:</strong> This implementation only supports CRC32 checksum calculation, even
 * though HDFS supports other checksum types. All checksums are calculated using 512-byte chunks
 * with CRC32 algorithm.
 */
@Value
@Builder
class DataPacket {

  private static final int INITIAL_BUFFER_SIZE = 65536; // 65KB
  private static final int BYTES_PER_CHECKSUM = 512;

  long offsetInBlock;
  long sequenceNumber;
  boolean lastPacket;
  boolean syncBlock;
  boolean calculateCrc;
  @NonNull ByteBuffer payload;

  /** Writes the packet to a DataOutputStream. */
  public void writeTo(DataOutputStream out) throws IOException {
    // Get payload size and remaining bytes
    int dataLength = payload.remaining();

    // Create PacketHeader protobuf
    PacketHeaderProto header =
        PacketHeaderProto.newBuilder()
            .setOffsetInBlock(offsetInBlock)
            .setSeqno(sequenceNumber)
            .setLastPacketInBlock(lastPacket)
            .setDataLen(dataLength)
            .setSyncBlock(syncBlock)
            .build();

    byte[] headerBytes = header.toByteArray();

    // Calculate checksums if required
    int checksumSize = 0;
    byte[] checksums = new byte[0];
    if (calculateCrc && payload.remaining() > 0) {
      int checksumCount = (dataLength + BYTES_PER_CHECKSUM - 1) / BYTES_PER_CHECKSUM;
      checksumSize = checksumCount * 4;
      checksums = calculateChecksums(payload);
    }

    // Create packet buffer with initial capacity
    try (ByteArrayOutputStream packetBuffer = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
        DataOutputStream packetOut = new DataOutputStream(packetBuffer)) {

      int payloadLength = 4 + checksumSize + dataLength;

      // Write PLEN (32-bit payload length)
      packetOut.writeInt(payloadLength);

      // Write HLEN (16-bit header length)
      packetOut.writeShort(headerBytes.length);

      // Write HEADER (protobuf)
      packetOut.write(headerBytes);

      // Write CHECKSUMS before data
      if (checksums.length > 0) {
        packetOut.write(checksums);
      }

      // Write DATA after checksums
      if (payload.remaining() > 0) {
        // Write the remaining bytes from ByteBuffer without modifying its position
        ByteBuffer duplicate = payload.duplicate();
        if (duplicate.hasArray()) {
          // If backed by array, write directly from array
          packetOut.write(
              duplicate.array(),
              duplicate.arrayOffset() + duplicate.position(),
              duplicate.remaining());
        } else {
          // If not backed by array, read bytes into temporary array
          byte[] tempArray = new byte[duplicate.remaining()];
          duplicate.get(tempArray);
          packetOut.write(tempArray);
        }
      }

      // Write entire packet buffer to output stream (no flush)
      out.write(packetBuffer.toByteArray());
    }
  }

  /** Calculates CRC32 checksums for the payload data. */
  private byte[] calculateChecksums(ByteBuffer data) throws IOException {
    try (ByteArrayOutputStream checksumBuffer = new ByteArrayOutputStream();
        DataOutputStream checksumOut = new DataOutputStream(checksumBuffer)) {

      // Create duplicate to avoid modifying original buffer position
      ByteBuffer duplicate = data.duplicate();

      while (duplicate.remaining() > 0) {
        int chunkSize = Math.min(BYTES_PER_CHECKSUM, duplicate.remaining());

        CRC32 crc = new CRC32();

        if (duplicate.hasArray()) {
          // If backed by array, update CRC directly from array
          int pos = duplicate.position();
          crc.update(duplicate.array(), duplicate.arrayOffset() + pos, chunkSize);
          duplicate.position(pos + chunkSize);
        } else {
          // If not backed by array, read chunk into temp array
          byte[] chunk = new byte[chunkSize];
          duplicate.get(chunk);
          crc.update(chunk);
        }

        // Write CRC32 checksum as 4 bytes (big-endian)
        checksumOut.writeInt((int) crc.getValue());
      }

      return checksumBuffer.toByteArray();
    }
  }
}
