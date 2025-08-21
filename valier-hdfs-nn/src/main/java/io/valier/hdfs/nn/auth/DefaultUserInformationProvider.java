package io.valier.hdfs.nn.auth;

/**
 * Default implementation of UserInformationProvider that uses the current system user. This
 * provider creates a SimpleUserInformation instance using the system property "user.name" with
 * "hdfs" as the fallback if the property is not available.
 */
public class DefaultUserInformationProvider implements UserInformationProvider {

  private static final DefaultUserInformationProvider INSTANCE =
      new DefaultUserInformationProvider();

  /**
   * Gets the singleton instance of the default user information provider.
   *
   * @return the default user information provider instance
   */
  public static DefaultUserInformationProvider getInstance() {
    return INSTANCE;
  }

  @Override
  public UserInformation getUserInformation() {
    return SimpleUserInformation.currentUser();
  }
}
