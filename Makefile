.PHONY: test test-unit test-integration setup-integration cleanup-integration build

# Default target
test: test-unit test-integration

# Unit tests
test-unit:
	go test ./internal/...

# Integration tests
test-integration: setup-integration
	@echo "Running integration tests..."
	SPANNER_EMULATOR_HOST=localhost:9010 go test -v -tags=integration ./integration_test.go
	@$(MAKE) cleanup-integration

# Setup integration test environment
setup-integration:
	@echo "Stopping any running Spanner emulator..."
	@$(MAKE) cleanup-integration

	@echo "Setting up integration test environment..."
	@echo "Starting Spanner emulator..."
	docker run -d --name spanner-emulator \
		-p 9010:9010 -p 9020:9020 \
		gcr.io/cloud-spanner-emulator/emulator:1.5.6
	@echo "Waiting for Spanner emulator to be ready..."
	@bash -c 'for i in {1..60}; do nc -z localhost 9010 && exit 0 || sleep 1; done; exit 1'
	@echo "Spanner emulator is ready!"
	@echo "Creating database schema..."
	SPANNER_EMULATOR_HOST=localhost:9010 gcloud spanner databases create test-database --instance=test-instance --project=test-project --ddl="CREATE TABLE Users (UserID STRING(36) NOT NULL, Name STRING(100) NOT NULL, Email STRING(255) NOT NULL, Status INT64 NOT NULL, CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (UserID); CREATE TABLE Products (ProductID STRING(36) NOT NULL, Name STRING(200) NOT NULL, Price INT64 NOT NULL, IsActive BOOL NOT NULL, CategoryID STRING(36), CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ProductID); CREATE TABLE Orders (OrderID STRING(36) NOT NULL, UserID STRING(36) NOT NULL, ProductID STRING(36) NOT NULL, Quantity INT64 NOT NULL, OrderDate TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (UserID, ProductID), INTERLEAVE IN PARENT Users ON DELETE CASCADE;" || true
	@echo "Database schema created (fixtures will be loaded by tests)"
	@echo "Setup complete!"

# Cleanup integration test environment
cleanup-integration:
	@echo "Cleaning up integration test environment..."
	docker stop spanner-emulator || true
	docker rm spanner-emulator || true
	@echo "Cleanup complete!"

# Build the application
build:
	go build -o spalidate main.go

# Install dependencies
deps:
	go mod download
	go mod tidy

# Clean build artifacts
clean:
	rm -f spalidate
	go clean

# Help target
help:
	@echo "Available targets:"
	@echo "  test              - Run all tests (unit + integration)"
	@echo "  test-unit         - Run unit tests only"  
	@echo "  test-integration  - Run integration tests with Spanner emulator"
	@echo "  build             - Build the application binary"
	@echo "  setup-integration - Set up Spanner emulator for testing"
	@echo "  cleanup-integration - Clean up Spanner emulator"
	@echo "  deps              - Download and tidy Go dependencies"
	@echo "  clean             - Clean build artifacts"
	@echo "  help              - Show this help message"