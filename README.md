# Spalidate

A command-line tool for validating data in Google Cloud Spanner emulator against YAML configuration files.

## Installation

```bash
go install github.com/nu0ma/spalidate@latest
```

## Usage

```bash
spalidate --project=test-project --instance=test-instance --database=test-database ./validation.yaml
```

### Flags

- `--project`: Spanner project ID (required)
- `--instance`: Spanner instance ID (required)  
- `--database`: Spanner database ID (required)
- `--port`: Spanner emulator port (default: 9010)
- `--version`: Show version information
- `--verbose`: Enable verbose logging
- `--help`: Show help information

### Positional Arguments

- `config-file`: Path to YAML configuration file (required)

## Configuration File Format

Create a YAML file that defines the expected data structure:

```yaml
tables:
  # Example: Users table
  Users:
    count: 1  # Expected number of rows
    columns:
      UserID: "user-001"
      Name: "Test User"
      Email: "test@example.com"
      Status: 1
  
  # Example: Products table
  Products:
    count: 2
    columns:
      ProductID: "prod-001"
      Name: "Test Product"
      Price: 1000
      IsActive: true
```

## What It Validates

- **Table existence**: Ensures all tables defined in the config exist in Spanner
- **Row count**: Verifies the exact number of rows matches the expected count
- **Column values**: Validates that the first row contains the expected column values
- **Data types**: Performs flexible type comparison (e.g., int vs int64, string representations)

## Examples

### Basic validation
```bash
spalidate --project=test-project --instance=test-instance --database=test-database ./validation.yaml
```

### Verbose output
```bash
spalidate --project=test-project --instance=test-instance --database=test-database --verbose ./validation.yaml
```

### Custom emulator port
```bash
spalidate --project=test-project --instance=test-instance --database=test-database --port=9020 ./validation.yaml
```

### Example output (success)
```
✅ All validations passed!
```

### Example output (failure)
```
Validation failed:
  ❌ Table Users: expected 2 rows, got 1
  ❌ Table Products, column Price: expected 1000 (int), got 1200 (int64)
```

## Spanner Connection

The tool connects to Spanner using the database path constructed from your parameters:
```
projects/{project}/instances/{instance}/databases/{database}
```

Make sure your Spanner emulator is running and accessible with the specified project, instance, and database.

## Development

### Building from source
```bash
git clone https://github.com/nu0ma/spalidate.git
cd spalidate
go build -o spalidate main.go
```

### Running tests

#### Unit tests
```bash
go test ./internal/...
```

#### Integration tests
Integration tests require Docker and docker-compose to run a local Spanner emulator.

Using the helper script (recommended):
```bash
./scripts/test-integration.sh
```

Or manually with Docker Compose:
```bash
# Start Spanner emulator
docker-compose up -d spanner-emulator

# Wait for emulator to be healthy
docker-compose ps  # Check status

# Run integration tests  
docker-compose run --rm integration-tests

# Cleanup
docker-compose down
```

Or run tests locally (requires running emulator):
```bash
# Start Spanner emulator
docker-compose up -d spanner-emulator

# Run integration tests with build tag
SPANNER_EMULATOR_HOST=localhost:9010 GOOGLE_CLOUD_PROJECT=test-project \
  go test -v -tags=integration ./integration/...

# Cleanup
docker-compose down
```

The integration tests validate:
- Successful validation scenarios with correct data
- Row count mismatch detection
- Column value mismatch detection  
- Non-existent table handling
- Null value handling
- Type conversion and comparison

Test data is defined in YAML files under `integration/testdata/`.

### Docker setup
The project includes Docker configuration for both the application and integration testing:

- `Dockerfile`: Builds the spalidate binary in a minimal Alpine container
- `docker-compose.yml`: Sets up Spanner emulator for local development and testing

### Environment variables
- `SPANNER_EMULATOR_HOST`: When set, the tool will connect to a Spanner emulator instead of real Spanner. Format: `host:port`

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request