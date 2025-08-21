package io.valier.hdfs.nn.auth;

import lombok.Value;

/**
 * Simple immutable implementation of UserInformation. This implementation stores user and effective
 * user information for HDFS operations.
 */
@Value
public class SimpleUserInformation implements UserInformation {

  /** The username of the user making the request. */
  String user;

  /** The effective username for the request. */
  String effectiveUser;

  /**
   * Creates a new SimpleUserInformation with the same user and effective user.
   *
   * @param user the username to use for both user and effective user
   * @return a new SimpleUserInformation instance
   */
  public static SimpleUserInformation of(String user) {
    return new SimpleUserInformation(user, user);
  }

  /**
   * Creates a new SimpleUserInformation with different user and effective user.
   *
   * @param user the username of the actual user
   * @param effectiveUser the effective username for the operation
   * @return a new SimpleUserInformation instance
   */
  public static SimpleUserInformation of(String user, String effectiveUser) {
    return new SimpleUserInformation(user, effectiveUser);
  }

  /**
   * Creates a SimpleUserInformation using the current system user.
   *
   * @return a new SimpleUserInformation instance with the current system user
   */
  public static SimpleUserInformation currentUser() {
    String user = System.getProperty("user.name", "hdfs");
    return of(user);
  }

  /**
   * Creates a SimpleUserInformation with 'hdfs' as both user and effective user. This UGI is
   * primarily used for testing.
   *
   * @return a new SimpleUserInformation instance with 'hdfs' user
   */
  public static SimpleUserInformation hdfsUser() {
    return of("hdfs");
  }
}
