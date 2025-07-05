.PHONY: test test-unit test-integration setup-integration cleanup-integration build

# Default target
test: test-unit test-integration

# Unit tests
test-unit:
	go test ./internal/...

# Integration tests
test-integration: setup-integration
	@echo "Running integration tests..."
	go test -v -tags=integration ./integration_test.go
	@$(MAKE) cleanup-integration

# Setup integration test environment
setup-integration:
	@echo "Setting up integration test environment..."
	cd testdata && docker compose up -d
	@echo "Waiting for Spanner emulator to be ready..."
	timeout 300 bash -c 'until nc -z localhost 9010; do sleep 1; done'
	@echo "Waiting additional time for full readiness..."
	sleep 5
	@echo "Configuring gcloud in container..."
	docker exec spanner-emulator gcloud config set auth/disable_credentials true
	docker exec spanner-emulator gcloud config set project test-project
	docker exec spanner-emulator gcloud config set api_endpoint_overrides/spanner http://localhost:9010/
	@echo "Creating Spanner instance..."
	docker exec spanner-emulator gcloud spanner instances create test-instance --config=emulator-config --description="Test Instance" --nodes=1
	@echo "Creating test database..."
	docker exec spanner-emulator gcloud spanner databases create test-database --instance=test-instance --project=test-project
	@echo "Loading schema..."
	docker exec -i spanner-emulator gcloud spanner databases ddl update test-database --instance=test-instance --project=test-project --ddl-file=/dev/stdin < testdata/schema.sql
	@echo "Loading test data..."
	docker exec -i spanner-emulator gcloud spanner databases execute-sql test-database --instance=test-instance --project=test-project --sql="$$(cat testdata/seed.sql)"

# Cleanup integration test environment
cleanup-integration:
	@echo "Cleaning up integration test environment..."
	cd testdata && docker compose down -v
	docker system prune -f

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