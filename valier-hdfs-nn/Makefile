# Makefile for NameNodeClient Integration Testing

.PHONY: help build test integration-test start-hdfs stop-hdfs clean-hdfs status logs full-test

# Default target
help:
	@echo "NameNodeClient Integration Testing"
	@echo "=================================="
	@echo ""
	@echo "Available targets:"
	@echo "  build           - Build the project"
	@echo "  test            - Run unit tests"
	@echo "  integration-test - Run integration tests (requires running HDFS)"
	@echo "  start-hdfs      - Start HDFS cluster in Docker"
	@echo "  stop-hdfs       - Stop HDFS cluster"
	@echo "  clean-hdfs      - Stop HDFS cluster and remove volumes"
	@echo "  status          - Check HDFS cluster status"
	@echo "  logs            - Show HDFS container logs"
	@echo "  full-test       - Complete test cycle (start HDFS, test, cleanup)"
	@echo ""
	@echo "Example workflows:"
	@echo "  make build && make start-hdfs && make integration-test"
	@echo "  make full-test"

# Build the project
build:
	@echo "Building NameNodeClient..."
	mvn clean compile

# Run unit tests
test:
	@echo "Running unit tests..."
	mvn test

# Run integration tests (requires HDFS to be running)
integration-test:
	@echo "Running integration tests..."
	mvn test -Dtest=NameNodeClientIntegrationTest

# Start HDFS cluster
start-hdfs:
	@echo "Starting HDFS cluster..."
	./run-integration-tests.sh start

# Stop HDFS cluster
stop-hdfs:
	@echo "Stopping HDFS cluster..."
	./run-integration-tests.sh stop

# Clean HDFS cluster (remove volumes)
clean-hdfs:
	@echo "Cleaning HDFS cluster..."
	./run-integration-tests.sh clean

# Check cluster status
status:
	@echo "Checking HDFS cluster status..."
	./run-integration-tests.sh status

# Show container logs
logs:
	@echo "Showing HDFS container logs..."
	./run-integration-tests.sh logs

# Complete test cycle
full-test:
	@echo "Running complete integration test cycle..."
	./run-integration-tests.sh full