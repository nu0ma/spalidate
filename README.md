# Spanlidate

A command-line tool for validating data in Google Cloud Spanner emulator against YAML configuration files.

## Installation

```bash
go install github.com/ishikawaryoufutoshi/spanlidate@latest
```

## Usage

```bash
spanlidate --project-id=my-project --config=validation.yaml
```

### Flags

- `--project-id`: Google Cloud Project ID (required)
- `--config`: Path to YAML configuration file (required)
- `--version`: Show version information
- `--verbose`: Enable verbose logging
- `--help`: Show help information

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
spanlidate --project-id=test-project --config=./validation.yaml
```

### Verbose output
```bash
spanlidate --project-id=test-project --config=./validation.yaml --verbose
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

The tool connects to Spanner using the default database path:
```
projects/{project-id}/instances/test-instance/databases/test-db
```

Make sure your Spanner emulator is running and accessible with the specified project ID.

## Development

### Building from source
```bash
git clone https://github.com/ishikawaryoufutoshi/spanlidate.git
cd spanlidate
go build -o spanlidate main.go
```

### Running tests
```bash
go test ./internal/...
```

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request