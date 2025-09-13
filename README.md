# Spalidate

A command-line tool for validating data in Google Cloud Spanner emulator against YAML configuration files.

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/nu0ma/spalidate)

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
- `--verbose`: Enable verbose logging (equivalent to `--log-level=debug`)
- `--log-level`: Log level (`debug`, `info`, `warn`, `error`) default: `info`
- `--log-format`: Log format (`console`, `json`) default: `console`
- `--color`: Color mode (`auto`, `always`, `never`) default: `auto`
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

### JSON logs
```bash
spalidate \
  --project=test-project --instance=test-instance --database=test-database \
  --log-level=info --log-format=json ./validation.yaml
```

### 強制的にカラーを有効化（CI/テストなど非TTY）
```bash
spalidate \
  --project=test-project --instance=test-instance --database=test-database \
  --log-level=debug --log-format=console --color=always ./validation.yaml
```
環境変数での制御も可能（例: `SPALIDATE_COLOR=always` に対応予定）。

## Logging

Spalidate は zap を用いてログを一元化しています。

- `--log-level` と `--log-format` で出力を制御できます（`--verbose` は `--log-level=debug` のショートカット）。
- 標準ライブラリの `log` 出力も zap に取り込まれるため、外部ライブラリのログも一箇所に集約されます。

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
