#!/bin/bash

# Wait for HDFS to be ready
echo "Waiting for HDFS to be ready..."
sleep 30

# Check if NameNode is accessible
while ! hdfs dfsadmin -report > /dev/null 2>&1; do
    echo "Waiting for NameNode to be ready..."
    sleep 5
done

echo "HDFS is ready, setting up test data..."

# Create test directory structure
hdfs dfs -mkdir -p /test-data
hdfs dfs -chmod 777 /test-data

# Create test files with different sizes
echo "Creating test files..."

# Small text files
for i in {1..5}; do
    echo "This is test file $i with some sample content for HdfsTransferManager testing." > /tmp/small-file-$i.txt
    echo "Line 2 of file $i" >> /tmp/small-file-$i.txt
    echo "Line 3 of file $i with timestamp: $(date)" >> /tmp/small-file-$i.txt
    hdfs dfs -put /tmp/small-file-$i.txt /test-data/
done

# Medium files (using base64 to create larger content)
for i in {1..3}; do
    base64 /dev/urandom | head -c 10000 > /tmp/medium-file-$i.txt
    echo "Medium file $i created at $(date)" >> /tmp/medium-file-$i.txt
    hdfs dfs -put /tmp/medium-file-$i.txt /test-data/
done

# Large file
base64 /dev/urandom | head -c 100000 > /tmp/large-file.txt
echo "Large file created at $(date)" >> /tmp/large-file.txt
hdfs dfs -put /tmp/large-file.txt /test-data/

# Binary test file
dd if=/dev/urandom of=/tmp/binary-data.bin bs=1024 count=50 2>/dev/null
hdfs dfs -put /tmp/binary-data.bin /test-data/

# Create a nested directory with files
hdfs dfs -mkdir -p /test-data/nested
for i in {1..3}; do
    echo "Nested file $i content" > /tmp/nested-$i.txt
    hdfs dfs -put /tmp/nested-$i.txt /test-data/nested/
done

# List the created test data
echo "Test data created successfully:"
hdfs dfs -ls -R /test-data/

echo "Test data setup completed!"