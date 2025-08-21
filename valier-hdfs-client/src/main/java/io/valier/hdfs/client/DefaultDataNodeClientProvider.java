package io.valier.hdfs.client;

import io.valier.hdfs.dn.DataNodeClient;
import io.valier.hdfs.dn.DefaultDataNodeClient;
import lombok.Builder;
import lombok.Value;

/**
 * Default implementation of DataNodeClientProvider that creates new DefaultDataNodeClient instances
 * for each hostname request.
 *
 * <p>This implementation creates a new DataNodeClient instance for each call to getClient(),
 * configured with the specified hostname and the provider's default settings. The client uses lazy
 * socket initialization, so the actual connection is not established until the first operation is
 * performed.
 */
@Value
@Builder
public class DefaultDataNodeClientProvider implements DataNodeClientProvider {

  /** Default DataNode port (9866 is the traditional HDFS DataNode data transfer port). */
  @Builder.Default int port = 9866;

  /** Connection timeout in milliseconds for DataNode connections. */
  @Builder.Default int connectionTimeoutMs = 5000;

  /** Socket read timeout in milliseconds for DataNode connections. */
  @Builder.Default int readTimeoutMs = 30000;

  /**
   * Creates a new DataNodeClient configured for the specified hostname.
   *
   * <p>Each call to this method creates a new DefaultDataNodeClient instance configured with the
   * hostname and this provider's default settings. The returned client uses lazy initialization, so
   * no network connection is established until the first operation is performed.
   *
   * @param hostname the hostname of the DataNode to connect to
   * @return a new DataNodeClient instance configured for the specified hostname
   * @throws IllegalArgumentException if hostname is null or empty
   */
  @Override
  public DataNodeClient getClient(String hostname) {
    if (hostname == null || hostname.trim().isEmpty()) {
      throw new IllegalArgumentException("Hostname cannot be null or empty");
    }

    return DefaultDataNodeClient.builder()
        .hostname(hostname.trim())
        .port(port)
        .connectionTimeoutMs(connectionTimeoutMs)
        .readTimeoutMs(readTimeoutMs)
        .build();
  }
}
