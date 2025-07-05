.PHONY: test test-unit test-integration build clean docker-build

# Default target
all: build

# Build the binary
build:
	go build -o spalidate .

# Run unit tests
test-unit:
	go test -v -race ./internal/...

# Run integration tests with Docker
test-integration:
	./scripts/test-integration.sh

# Run all tests
test: test-unit test-integration

# Clean build artifacts
clean:
	rm -f spalidate
	rm -f coverage.out
	docker-compose down -v

# Build Docker image
docker-build:
	docker-compose build spalidate-test

# Run linting
lint:
	go fmt ./...
	go vet ./...

# Install the binary
install:
	go install .

# Run the emulator for local development
emulator-up:
	docker-compose up -d spanner-emulator

# Stop the emulator
emulator-down:
	docker-compose down