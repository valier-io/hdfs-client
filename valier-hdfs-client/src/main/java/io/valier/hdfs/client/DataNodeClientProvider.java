package io.valier.hdfs.client;

import io.valier.hdfs.dn.DataNodeClient;

/**
 * Provider interface for creating DataNodeClient instances for specific DataNode hosts.
 *
 * <p>This interface follows the provider pattern to abstract the creation and configuration of
 * DataNodeClient instances. Implementations can handle connection pooling, configuration
 * management, and host-specific settings while providing a simple interface for obtaining clients
 * for specific DataNode hosts.
 */
public interface DataNodeClientProvider {

  /**
   * Creates or retrieves a DataNodeClient for the specified hostname.
   *
   * <p>The implementation may create a new client instance each time or manage a pool of
   * connections. The returned client should be properly configured to connect to the DataNode at
   * the specified hostname using the default HDFS DataNode port or a port configured in the
   * provider.
   *
   * @param hostname the hostname of the DataNode to connect to
   * @return a DataNodeClient configured for the specified hostname
   * @throws IllegalArgumentException if hostname is null or empty
   */
  DataNodeClient getClient(String hostname);
}
