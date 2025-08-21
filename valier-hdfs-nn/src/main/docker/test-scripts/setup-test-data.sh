#!/bin/bash

# Script to set up test data in HDFS for integration testing

echo "Setting up HDFS test data..."

# Wait for HDFS to be fully ready
echo "Waiting for HDFS to be ready..."
while ! hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes"; do
    echo "Waiting for DataNodes to be ready..."
    sleep 10
done

echo "HDFS is ready, creating test data structure..."

# Create standard HDFS directories
hdfs dfs -mkdir -p /user 2>/dev/null || true
hdfs dfs -mkdir -p /tmp 2>/dev/null || true
hdfs dfs -mkdir -p /test 2>/dev/null || true
hdfs dfs -mkdir -p /user/testuser 2>/dev/null || true
hdfs dfs -mkdir -p /integration-test 2>/dev/null || true

# Create test files with meaningful content
echo "Creating test files..."

# Create a simple text file
echo "This is a sample text file for integration testing." | hdfs dfs -put - /test/sample.txt

# Create a data file
echo "Integration test data file with some content for testing the NameNode client." | hdfs dfs -put - /user/testuser/data.txt

# Create additional files for comprehensive testing
echo "File 1 content" | hdfs dfs -put - /integration-test/file1.txt
echo "File 2 content" | hdfs dfs -put - /integration-test/file2.txt
echo "File 3 content" | hdfs dfs -put - /integration-test/file3.txt

# Create nested directory structure
hdfs dfs -mkdir -p /integration-test/nested/deep/structure
echo "Deep nested file" | hdfs dfs -put - /integration-test/nested/deep/structure/deep-file.txt

# Upload additional test files if they exist in test-data directory
if [ -f /test-data/large-file.txt ]; then
    echo "Uploading large test file..."
    hdfs dfs -put /test-data/large-file.txt /integration-test/large-file.txt
fi

if [ -f /test-data/binary-data.bin ]; then
    echo "Uploading binary test file..."
    hdfs dfs -put /test-data/binary-data.bin /integration-test/binary-data.bin
fi

echo "Verifying test data setup..."

echo "Root directory listing:"
hdfs dfs -ls /

echo "User directory listing:"
hdfs dfs -ls /user

echo "Integration test directory listing:"
hdfs dfs -ls /integration-test

echo "Test data setup completed successfully!"