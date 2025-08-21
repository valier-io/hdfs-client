package io.valier.hdfs.nn.auth;

/**
 * Interface representing user information for HDFS operations. This interface provides the
 * necessary user context for authentication and authorization with the NameNode.
 */
public interface UserInformation {

  /**
   * Gets the username of the user making the request. This is typically the authenticated user
   * identity.
   *
   * @return the username
   */
  String getUser();

  /**
   * Gets the effective username for the request. This may differ from the user when using proxy
   * users or impersonation.
   *
   * @return the effective username
   */
  String getEffectiveUser();
}
