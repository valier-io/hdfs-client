package io.valier.hdfs.nn.auth;

/**
 * Interface for providing user information for HDFS operations. Implementations of this interface
 * are responsible for determining the user context for authentication and authorization with the
 * NameNode.
 */
public interface UserInformationProvider {

  /**
   * Provides the user information for the current context. This method is called by the
   * NameNodeClient to obtain the user identity that should be used for HDFS operations.
   *
   * @return the user information containing user and effective user details
   */
  UserInformation getUserInformation();
}
