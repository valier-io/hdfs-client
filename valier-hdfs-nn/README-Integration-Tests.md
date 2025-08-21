# Integration Tests for NameNodeClient

This document describes how to run integration tests for the NameNodeClient against a real Apache HDFS cluster running in Docker containers.

## Prerequisites

- Docker and Docker Compose installed
- Java 11+ and Maven 3.6+
- At least 2GB of free RAM for the HDFS cluster

## Quick Start

1. **Start the HDFS cluster:**
   ```bash
   docker-compose -f src/main/docker/docker-compose.yml up -d
   ```

2. **Wait for services to be healthy:**
   ```bash
   docker-compose -f src/main/docker/docker-compose.yml ps
   ```
   All services should show "healthy" or "running" status.

3. **Run the integration tests:**
   ```bash
   mvn test -Dtest=NameNodeClientIntegrationTest
   ```
   or using the integration test profile:
   ```bash
   mvn verify -Pintegration-test
   ```

4. **Clean up when done:**
   ```bash
   docker-compose -f src/main/docker/docker-compose.yml down
   docker-compose -f src/main/docker/docker-compose.yml down -v  # Remove volumes as well
   ```

## HDFS Cluster Components

The Docker Compose setup includes:

- **NameNode**: Hadoop NameNode on port 9000 (RPC) and 9870 (Web UI)
- **DataNode**: Hadoop DataNode on port 9864 (Web UI)
- **Test Setup**: Initializes test data structure

### Test Data Structure

The integration tests expect the following directory structure in HDFS:

```
/
├── tmp/                    # Standard HDFS temp directory
├── user/                   # Standard HDFS user directory
│   └── testuser/           # Test user directory
│       └── data.txt        # Test file
├── test/                   # Test directory
│   └── sample.txt          # Sample test file
└── integration-test/       # Integration test directory
    ├── file1.txt           # Test file 1
    ├── file2.txt           # Test file 2
    ├── file3.txt           # Test file 3
    └── nested/             # Nested directory structure
        └── deep/
            └── structure/
                └── deep-file.txt
```

## Integration Test Coverage

The integration tests verify:

1. **Basic Functionality:**
   - Root directory listing
   - Specific directory listing
   - Static method calls

2. **Directory Navigation:**
   - User directories
   - Nested directories
   - Deep nested structures

3. **Error Handling:**
   - Non-existent directories
   - Empty directories

4. **Concurrent Access:**
   - Multiple client instances

## Monitoring the HDFS Cluster

### Web UIs
- **NameNode Web UI**: http://localhost:9870
- **DataNode Web UI**: http://localhost:9864

### Command Line Access
```bash
# Access NameNode container
docker exec -it hdfs-namenode-integration bash

# Run HDFS commands
hdfs dfs -ls /
hdfs dfsadmin -report
```

## Troubleshooting

### Services Not Starting
```bash
# Check container logs
docker-compose -f src/main/docker/docker-compose.yml logs namenode
docker-compose -f src/main/docker/docker-compose.yml logs datanode

# Check container status
docker-compose -f src/main/docker/docker-compose.yml ps
```

### Connection Failures
```bash
# Verify NameNode is listening
docker exec hdfs-namenode-integration netstat -tlnp | grep 9000

# Check if test setup completed
docker-compose -f src/main/docker/docker-compose.yml logs test-setup
```

### Port Conflicts
If ports 9000 or 9870 are already in use:
```bash
# Modify src/main/docker/docker-compose.yml to use different ports
# For example, change "9000:9000" to "19000:9000"
```

### Integration Tests Failing
```bash
# Run tests with more verbose output
mvn test -Dtest=NameNodeClientIntegrationTest -X

# Check if HDFS cluster is fully ready
curl -f http://localhost:9870/
```

## Development Tips

### Adding New Integration Tests

1. Create test methods in `NameNodeClientIntegrationTest.java`
2. Use meaningful test data from the setup scripts
3. Include proper assertions and error handling
4. Add logging for debugging

### Modifying Test Data

1. Edit `test-scripts/setup-test-data.sh`
2. Restart the Docker Compose stack:
   ```bash
   docker-compose -f src/main/docker/docker-compose.yml down -v
   docker-compose -f src/main/docker/docker-compose.yml up -d
   ```

### Running Tests Against External HDFS

To run tests against an external HDFS cluster:
```bash
mvn test -Dtest=NameNodeClientIntegrationTest \
  -Dnamenode.host=your-namenode-host \
  -Dnamenode.port=9000
```

## Performance Considerations

- The HDFS cluster takes 1-2 minutes to fully initialize
- Integration tests add ~30 seconds to the build time
- Docker containers use approximately 1-2GB RAM

## CI/CD Integration

For automated testing in CI/CD pipelines:

```bash
# Start cluster
docker-compose -f src/main/docker/docker-compose.yml up -d

# Wait for health check
timeout 300 bash -c 'until docker-compose -f src/main/docker/docker-compose.yml ps | grep healthy; do sleep 10; done'

# Run tests
mvn verify -Pintegration-test

# Cleanup
docker-compose -f src/main/docker/docker-compose.yml down -v
```
