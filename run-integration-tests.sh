#!/bin/bash

# Script to run HDFS Client integration tests
# This script starts the HDFS cluster, runs the tests, and cleans up

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$SCRIPT_DIR/valier-hdfs-nn/src/main/docker"

echo "🚀 Starting HDFS Client Integration Tests"
echo "=========================================="

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "🧹 Cleaning up Docker services..."
    cd "$DOCKER_DIR"
    docker-compose down -v --remove-orphans > /dev/null 2>&1 || true
    echo "✅ Cleanup completed"
}

# Trap cleanup on script exit
trap cleanup EXIT

# Step 1: Start HDFS cluster
echo "📦 Starting HDFS cluster..."
cd "$DOCKER_DIR"
docker-compose up -d

echo "⏳ Waiting for HDFS cluster to be ready..."
echo "   This may take 1-2 minutes for the first run..."

# Wait for test-setup to complete
echo "   Waiting for test data setup..."
timeout 300 docker-compose logs -f test-setup | grep -q "Test data setup completed successfully!" || {
    echo "❌ Test setup failed or timed out"
    echo "   Check logs with: docker-compose logs test-setup"
    exit 1
}

echo "✅ HDFS cluster is ready!"

# Step 2: Verify cluster health
echo ""
echo "🔍 Verifying cluster health..."
docker exec hdfs-namenode-integration hdfs dfs -ls / > /dev/null 2>&1 || {
    echo "❌ HDFS cluster is not responding properly"
    exit 1
}
echo "✅ HDFS cluster is healthy!"

# Step 3: Display cluster info
echo ""
echo "📊 HDFS Cluster Information:"
echo "   NameNode: http://localhost:9870"
echo "   HDFS URI: hdfs://localhost:9000"
echo "   DataNode: http://localhost:9864"

# Step 4: Run integration tests
echo ""
echo "🧪 Running integration tests..."
cd "$SCRIPT_DIR"

mvn clean verify \
    -pl valier-hdfs-client \
    -Dit.test="*IntegrationTest" \
    -q

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 All integration tests passed!"
    echo ""
    echo "Test Summary:"
    echo "✅ File copy from /test/sample.txt"
    echo "✅ File copy from /user/testuser/data.txt" 
    echo "✅ File copy with REPLACE_EXISTING option"
    echo "✅ Deep nested file copy"
    echo "✅ Multiple file copy operations"
    echo "✅ Error handling for non-existent files"
    echo "✅ Error handling for directory copy attempts"
    echo "✅ HDFS cluster information retrieval"
else
    echo ""
    echo "❌ Some integration tests failed!"
    echo "   Check the test output above for details"
    exit 1
fi

echo ""
echo "🏁 Integration test run completed successfully!"