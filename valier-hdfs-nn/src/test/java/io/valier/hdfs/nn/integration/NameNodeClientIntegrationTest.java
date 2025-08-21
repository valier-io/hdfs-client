package io.valier.hdfs.nn.integration;

import static org.junit.Assert.*;

import io.valier.hdfs.nn.DefaultNameNodeClient;
import io.valier.hdfs.nn.HdfsFileSummary;
import io.valier.hdfs.nn.HdfsServerInfo;
import io.valier.hdfs.nn.NameNodeClient;
import io.valier.hdfs.nn.auth.SimpleUserInformation;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for NameNodeClient against a real HDFS NameNode running in Docker.
 *
 * <p>Prerequisites: - Docker and Docker Compose must be installed - Run 'docker-compose -f
 * src/main/docker/docker-compose.yml up -d' in the project directory before running these tests -
 * HDFS cluster should be fully initialized with test data
 *
 * <p>To run these tests: 1. Start HDFS cluster: docker-compose -f
 * src/main/docker/docker-compose.yml up -d 2. Wait for services to be healthy: docker-compose -f
 * src/main/docker/docker-compose.yml ps 3. Run tests: mvn test -Dtest=NameNodeClientIntegrationTest
 * 4. Stop cluster: docker-compose -f src/main/docker/docker-compose.yml down
 */
public class NameNodeClientIntegrationTest {

  private static final String NAMENODE_HOST = "localhost";
  private static final int NAMENODE_PORT = 9000;

  // Test data expectations based on setup-test-data.sh
  private static final String[] EXPECTED_ROOT_DIRS = {"tmp", "user", "test", "integration-test"};

  private static NameNodeClient client;

  @BeforeClass
  public static void setUpClass() throws Exception {
    System.out.println("Setting up integration tests...");

    // Initialize NameNode client
    client =
        DefaultNameNodeClient.builder()
            .nameNodeUri("hdfs://" + NAMENODE_HOST + ":" + NAMENODE_PORT)
            .userInformationProvider(SimpleUserInformation::hdfsUser)
            .build();

    // Wait for HDFS to be ready with retries
    boolean connected = false;
    int maxRetries = 3; // 5 minutes total wait time
    int retryCount = 0;

    while (!connected && retryCount < maxRetries) {
      try {
        System.out.println(
            "Attempting to connect to NameNode (attempt "
                + (retryCount + 1)
                + "/"
                + maxRetries
                + ")...");
        Stream<HdfsFileSummary> testListing = client.list("/");
        List<String> testFileNames =
            testListing.map(HdfsFileSummary::getName).collect(Collectors.toList());
        connected = true;
        System.out.println(
            "Successfully connected to NameNode. Root directory has "
                + testFileNames.size()
                + " entries.");
      } catch (Exception e) {
        e.printStackTrace();
        retryCount++;
        if (retryCount < maxRetries) {
          System.out.println(
              "Connection failed, waiting 10 seconds before retry: " + e.getMessage());
          TimeUnit.SECONDS.sleep(10);
        } else {
          throw new RuntimeException(
              "Failed to connect to NameNode after "
                  + maxRetries
                  + " attempts. "
                  + "Make sure Docker Compose HDFS cluster is running with 'docker-compose -f src/main/docker/docker-compose.yml up -d'",
              e);
        }
      }
    }
  }

  @AfterClass
  public static void tearDownClass() {
    System.out.println("Integration tests completed.");
    System.out.println(
        "To clean up Docker containers, run: docker-compose -f src/main/docker/docker-compose.yml down");
  }

  @Test
  public void testGetRootDirectoryListing() throws IOException {
    System.out.println("Testing root directory listing...");

    List<String> rootListing =
        client.list("/").map(HdfsFileSummary::getName).collect(Collectors.toList());

    assertNotNull("Root directory listing should not be null", rootListing);
    assertFalse("Root directory should not be empty", rootListing.isEmpty());

    System.out.println("Root directory contains " + rootListing.size() + " entries:");
    for (String entry : rootListing) {
      System.out.println("  - " + entry);
    }

    // Verify expected directories are present
    for (String expectedDir : EXPECTED_ROOT_DIRS) {
      assertTrue(
          "Root directory should contain '" + expectedDir + "' directory",
          rootListing.contains(expectedDir));
    }
  }

  @Test
  public void testGetUserDirectoryListing() throws IOException {
    System.out.println("Testing /user directory listing...");

    List<String> userListing =
        client.list("/user").map(HdfsFileSummary::getName).collect(Collectors.toList());

    assertNotNull("User directory listing should not be null", userListing);

    System.out.println("/user directory contains " + userListing.size() + " entries:");
    for (String entry : userListing) {
      System.out.println("  - " + entry);
    }

    // Should contain testuser directory
    assertTrue(
        "User directory should contain 'testuser' directory", userListing.contains("testuser"));
  }

  @Test
  public void testGetIntegrationTestDirectoryListing() throws IOException {
    System.out.println("Testing /integration-test directory listing...");

    List<String> testListing =
        client.list("/integration-test").map(HdfsFileSummary::getName).collect(Collectors.toList());

    assertNotNull("Integration test directory listing should not be null", testListing);
    assertFalse("Integration test directory should not be empty", testListing.isEmpty());

    System.out.println("/integration-test directory contains " + testListing.size() + " entries:");
    for (String entry : testListing) {
      System.out.println("  - " + entry);
    }

    // Should contain test files and nested directory
    assertTrue("Should contain file1.txt", testListing.contains("file1.txt"));
    assertTrue("Should contain file2.txt", testListing.contains("file2.txt"));
    assertTrue("Should contain file3.txt", testListing.contains("file3.txt"));
    assertTrue("Should contain nested directory", testListing.contains("nested"));
  }

  @Test
  public void testGetNestedDirectoryListing() throws IOException {
    System.out.println("Testing nested directory listing...");

    List<String> nestedListing =
        client
            .list("/integration-test/nested")
            .map(HdfsFileSummary::getName)
            .collect(Collectors.toList());

    assertNotNull("Nested directory listing should not be null", nestedListing);
    assertFalse("Nested directory should not be empty", nestedListing.isEmpty());

    System.out.println(
        "/integration-test/nested directory contains " + nestedListing.size() + " entries:");
    for (String entry : nestedListing) {
      System.out.println("  - " + entry);
    }

    assertTrue("Should contain deep directory", nestedListing.contains("deep"));
  }

  @Test
  public void testGetDeepNestedDirectoryListing() throws IOException {
    System.out.println("Testing deep nested directory listing...");

    List<String> deepListing =
        client
            .list("/integration-test/nested/deep/structure")
            .map(HdfsFileSummary::getName)
            .collect(Collectors.toList());

    assertNotNull("Deep nested directory listing should not be null", deepListing);
    assertFalse("Deep nested directory should not be empty", deepListing.isEmpty());

    System.out.println(
        "/integration-test/nested/deep/structure directory contains "
            + deepListing.size()
            + " entries:");
    for (String entry : deepListing) {
      System.out.println("  - " + entry);
    }

    assertTrue("Should contain deep-file.txt", deepListing.contains("deep-file.txt"));
  }

  @Test
  public void testBuilderBasedClientWithSpecificPath() throws IOException {
    System.out.println("Testing builder-based client with specific path...");

    NameNodeClient builderClient =
        DefaultNameNodeClient.builder()
            .nameNodeUri("hdfs://" + NAMENODE_HOST + ":" + NAMENODE_PORT)
            .build();

    List<String> builderPathListing =
        builderClient.list("/user").map(HdfsFileSummary::getName).collect(Collectors.toList());

    assertNotNull("Builder client path listing should not be null", builderPathListing);

    System.out.println(
        "Builder client /user directory contains " + builderPathListing.size() + " entries:");
    for (String entry : builderPathListing) {
      System.out.println("  - " + entry);
    }
  }

  @Test(expected = IOException.class)
  public void testNonExistentDirectoryThrowsException() throws IOException {
    System.out.println("Testing non-existent directory handling...");

    // This should throw an IOException
    client.list("/non/existent/directory");
  }

  @Test
  public void testEmptyDirectoryListing() throws IOException {
    System.out.println("Testing empty directory listing...");

    // /tmp might be empty initially
    List<String> tmpListing =
        client.list("/tmp").map(HdfsFileSummary::getName).collect(Collectors.toList());

    assertNotNull("Tmp directory listing should not be null", tmpListing);

    System.out.println("/tmp directory contains " + tmpListing.size() + " entries:");
    for (String entry : tmpListing) {
      System.out.println("  - " + entry);
    }
  }

  @Test
  public void testMultipleClientInstances() throws IOException {
    System.out.println("Testing multiple client instances...");

    // Create multiple clients and ensure they work independently
    NameNodeClient client1 =
        DefaultNameNodeClient.builder()
            .nameNodeUri("hdfs://" + NAMENODE_HOST + ":" + NAMENODE_PORT)
            .build();
    NameNodeClient client2 =
        DefaultNameNodeClient.builder()
            .nameNodeUri("hdfs://" + NAMENODE_HOST + ":" + NAMENODE_PORT)
            .build();

    List<String> listing1 =
        client1.list("/").map(HdfsFileSummary::getName).collect(Collectors.toList());
    List<String> listing2 =
        client2.list("/").map(HdfsFileSummary::getName).collect(Collectors.toList());

    assertEquals("Both clients should return same results", listing1.size(), listing2.size());

    for (String entry : listing1) {
      assertTrue("Both clients should see the same entries", listing2.contains(entry));
    }

    System.out.println("Multiple client instances test passed");
  }

  @Test
  public void testGetBuildVersion() throws IOException {
    System.out.println("Testing NameNode server information retrieval...");

    HdfsServerInfo serverInfo = client.getBuildVersion();

    assertNotNull("Server info should not be null", serverInfo);
    assertNotNull("Build version should not be null", serverInfo.getBuildVersion());
    assertFalse("Build version should not be empty", serverInfo.getBuildVersion().isEmpty());
    assertNotNull("Block pool ID should not be null", serverInfo.getBlockPoolID());
    assertFalse("Block pool ID should not be empty", serverInfo.getBlockPoolID().isEmpty());
    assertNotNull("Software version should not be null", serverInfo.getSoftwareVersion());
    assertFalse("Software version should not be empty", serverInfo.getSoftwareVersion().isEmpty());

    System.out.println("NameNode server information:");
    System.out.println("  Build version: " + serverInfo.getBuildVersion());
    System.out.println("  Block pool ID: " + serverInfo.getBlockPoolID());
    System.out.println("  Software version: " + serverInfo.getSoftwareVersion());
    System.out.println("  Capabilities: " + serverInfo.getCapabilities());

    // Verify it contains typical Hadoop version information
    assertTrue(
        "Build version should contain version-like information",
        serverInfo.getBuildVersion().contains(".")
            || serverInfo.getBuildVersion().contains("Unknown"));
  }
}
