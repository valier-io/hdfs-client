#!/bin/bash

# Integration test runner script for NameNodeClient
# This script manages the complete integration testing workflow

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "NameNodeClient Integration Test Runner"
echo "====================================="

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        echo "Error: Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Function to check if Docker Compose is available
check_docker_compose() {
    if ! command -v docker-compose >/dev/null 2>&1; then
        echo "Error: docker-compose is not installed. Please install Docker Compose."
        exit 1
    fi
}

# Function to wait for HDFS to be ready
wait_for_hdfs() {
    echo "Waiting for HDFS cluster to be ready..."
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        echo "Checking HDFS status (attempt $attempt/$max_attempts)..."
        
        if curl -f http://localhost:9870/ >/dev/null 2>&1; then
            echo "NameNode Web UI is accessible"
            
            # Check if DataNode is registered
            if docker exec hdfs-namenode-integration hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes (1):"; then
                echo "HDFS cluster is ready!"
                return 0
            fi
        fi
        
        echo "HDFS not ready yet, waiting 10 seconds..."
        sleep 10
        attempt=$((attempt + 1))
    done
    
    echo "Error: HDFS cluster failed to become ready within timeout"
    echo "Showing container logs for debugging:"
    docker-compose -f src/main/docker/docker-compose.yml logs --tail=20 namenode
    return 1
}

# Function to run integration tests
run_tests() {
    echo "Running integration tests..."
    mvn verify -Dit.test=NameNodeClientIntegrationTest
}

# Function to cleanup
cleanup() {
    if [ "$1" = "volumes" ]; then
        echo "Stopping HDFS cluster and removing volumes..."
        docker-compose -f src/main/docker/docker-compose.yml down -v
    else
        echo "Stopping HDFS cluster..."
        docker-compose -f src/main/docker/docker-compose.yml down
    fi
}

# Parse command line arguments
case "${1:-}" in
    "start")
        echo "Starting HDFS cluster only..."
        check_docker
        check_docker_compose
        docker-compose -f src/main/docker/docker-compose.yml up -d
        wait_for_hdfs
        echo "HDFS cluster is ready. Run './run-integration-tests.sh test' to run tests."
        ;;
    "test")
        echo "Running integration tests against existing cluster..."
        run_tests
        ;;
    "stop")
        echo "Stopping HDFS cluster..."
        cleanup
        ;;
    "clean")
        echo "Stopping HDFS cluster and removing volumes..."
        cleanup volumes
        ;;
    "full")
        echo "Running complete integration test cycle..."
        check_docker
        check_docker_compose
        
        # Start cluster
        echo "Step 1: Starting HDFS cluster..."
        docker-compose -f src/main/docker/docker-compose.yml up -d
        
        # Wait for readiness
        echo "Step 2: Waiting for HDFS to be ready..."
        if wait_for_hdfs; then
            # Run tests
            echo "Step 3: Running integration tests..."
            run_tests
            test_result=$?
            
            # Cleanup
            echo "Step 4: Cleaning up..."
            cleanup
            
            if [ $test_result -eq 0 ]; then
                echo "Integration tests completed successfully!"
                exit 0
            else
                echo "Integration tests failed!"
                exit 1
            fi
        else
            echo "Failed to start HDFS cluster"
            cleanup
            exit 1
        fi
        ;;
    "logs")
        echo "Showing container logs..."
        docker-compose -f src/main/docker/docker-compose.yml logs -f
        ;;
    "status")
        echo "Checking HDFS cluster status..."
        docker-compose -f src/main/docker/docker-compose.yml ps
        echo ""
        if curl -f http://localhost:9870/ >/dev/null 2>&1; then
            echo "✓ NameNode Web UI is accessible at http://localhost:9870/"
        else
            echo "✗ NameNode Web UI is not accessible"
        fi
        
        if docker exec hdfs-namenode-integration hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes"; then
            echo "✓ DataNodes are registered and healthy"
        else
            echo "✗ DataNodes are not ready"
        fi
        ;;
    *)
        echo "Usage: $0 {start|test|stop|clean|full|logs|status}"
        echo ""
        echo "Commands:"
        echo "  start  - Start HDFS cluster only"
        echo "  test   - Run integration tests against running cluster"
        echo "  stop   - Stop HDFS cluster (keep volumes)"
        echo "  clean  - Stop HDFS cluster and remove volumes"
        echo "  full   - Complete cycle: start, test, cleanup"
        echo "  logs   - Show container logs"
        echo "  status - Check cluster status"
        echo ""
        echo "Examples:"
        echo "  $0 full                    # Run complete test cycle"
        echo "  $0 start && $0 test        # Start cluster, then run tests"
        echo "  $0 status                  # Check if cluster is ready"
        exit 1
        ;;
esac