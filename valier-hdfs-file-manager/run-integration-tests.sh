#!/bin/bash

set -e

echo "=== HDFS File Transfer Manager Integration Tests ==="
echo

# Change to the file manager module directory
cd "$(dirname "$0")"

# Function to clean up Docker containers
cleanup() {
    echo "Cleaning up Docker containers..."
    docker-compose -f src/main/docker/docker-compose-test.yml down -v 2>/dev/null || true
    docker rm -f hdfs-namenode-filemanager-test hdfs-datanode-filemanager-test hdfs-test-setup-filemanager 2>/dev/null || true
}

# Trap to ensure cleanup on script exit
trap cleanup EXIT

echo "Starting HDFS Docker containers..."
cleanup

# Make setup script executable
chmod +x src/main/docker/setup-test-data.sh

# Start HDFS cluster
docker-compose -f src/main/docker/docker-compose-test.yml up -d namenode datanode

echo "Waiting for HDFS cluster to start..."
sleep 30

# Check if services are running
if ! docker ps | grep -q hdfs-namenode-filemanager-test; then
    echo "ERROR: NameNode container is not running"
    docker-compose -f src/main/docker/docker-compose-test.yml logs namenode
    exit 1
fi

if ! docker ps | grep -q hdfs-datanode-filemanager-test; then
    echo "ERROR: DataNode container is not running"
    docker-compose -f src/main/docker/docker-compose-test.yml logs datanode
    exit 1
fi

echo "Setting up test data..."
docker run --rm \
    --network hdfs-filemanager-test \
    -e CORE_CONF_fs_defaultFS=hdfs://namenode:9000 \
    -e HDFS_CONF_dfs_replication=1 \
    -v "$(pwd)/src/main/docker/setup-test-data.sh:/setup-test-data.sh:ro" \
    bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 \
    /setup-test-data.sh

echo
echo "Test data setup completed. Running integration tests..."
echo

# Build the project first
echo "Building project..."
cd ..
mvn clean compile -pl valier-hdfs-file-manager -am -q

# Run integration tests
echo "Running HdfsTransferManager integration tests..."
mvn verify -pl valier-hdfs-file-manager -Dit.test=HdfsTransferManagerIntegrationTest

echo
echo "=== Integration Tests Completed Successfully ==="
echo

# Display test results summary
if [ -f "valier-hdfs-file-manager/target/failsafe-reports/TEST-io.valier.hdfs.filemanager.HdfsTransferManagerIntegrationTest.xml" ]; then
    echo "Test Results Summary:"
    grep -o 'tests="[0-9]*"' "valier-hdfs-file-manager/target/failsafe-reports/TEST-io.valier.hdfs.filemanager.HdfsTransferManagerIntegrationTest.xml" | head -1
    grep -o 'failures="[0-9]*"' "valier-hdfs-file-manager/target/failsafe-reports/TEST-io.valier.hdfs.filemanager.HdfsTransferManagerIntegrationTest.xml" | head -1
    grep -o 'errors="[0-9]*"' "valier-hdfs-file-manager/target/failsafe-reports/TEST-io.valier.hdfs.filemanager.HdfsTransferManagerIntegrationTest.xml" | head -1
fi