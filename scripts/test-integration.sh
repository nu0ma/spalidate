#!/bin/bash
set -e

echo "Starting integration tests..."

# Check if docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Build the application image
echo "Building application image..."
docker-compose build spalidate-test

# Start the services
echo "Starting Spanner emulator..."
docker-compose up -d spanner-emulator

# Wait for emulator to be healthy
echo "Waiting for Spanner emulator to be ready..."
attempts=0
max_attempts=30
while [ $attempts -lt $max_attempts ]; do
    if docker-compose ps | grep -q "spanner-emulator.*healthy"; then
        echo "Spanner emulator is ready!"
        break
    fi
    attempts=$((attempts + 1))
    if [ $attempts -eq $max_attempts ]; then
        echo "Timeout waiting for Spanner emulator"
        docker-compose down
        exit 1
    fi
    sleep 2
done

# Run integration tests
echo "Running integration tests..."
docker-compose run --rm integration-tests

# Store exit code
TEST_EXIT_CODE=$?

# Clean up
echo "Cleaning up..."
docker-compose down

# Exit with test exit code
exit $TEST_EXIT_CODE