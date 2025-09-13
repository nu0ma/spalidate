# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spalidate is a Go CLI tool for validating Google Cloud Spanner database data against YAML configuration files. It connects to Spanner emulator instances and performs comprehensive data validation with flexible type comparison.

## Essential Development Commands

### Building and Testing
```bash
# Build the application
go build -o spalidate main.go

# Run unit tests
go test ./internal/...

# Run all tests (unit + integration)
make test

# Run only integration tests (includes emulator setup)
make test-integration

# Manual emulator management
make setup-integration    # Start Spanner emulator in Docker
make cleanup-integration  # Stop and remove emulator
```

### Usage
```bash
# Basic validation
./spalidate --project=test-project --instance=test-instance --database=test-database ./validation.yaml

# With verbose output
./spalidate --verbose --project=test-project --instance=test-instance --database=test-database ./validation.yaml
```

## Core Architecture

### Package Structure
- **`internal/config/`**: YAML configuration parsing and validation
  - Supports both single-row (`columns`) and multi-row (`rows`) validation modes
  - Validates configuration structure and required fields
- **`internal/spanner/`**: Spanner client wrapper with type-safe row handling
  - Abstracts Spanner connection and query operations
  - Handles emulator connection via `SPANNER_EMULATOR_HOST`
- **`internal/validator/`**: Core validation engine with comprehensive type comparison
  - Implements flexible validation strategies and type conversion logic
- **`internal/testutil/`**: Test fixture management using go-testfixtures

## Detailed Validation Logic

### Validation Flow
1. **Table Existence Check**: Verifies table exists in Spanner database
2. **Row Count Validation**: Compares expected vs actual row counts exactly
3. **Column Data Validation**: Validates actual data against expected values

### Row Comparison Strategies

#### Order-based Comparison (Default)
- Compares rows in sequence: `rows[0]` vs `expected[0]`, `rows[1]` vs `expected[1]`, etc.
- Requires consistent ordering via `order_by` field in configuration
- Best for simple validation scenarios

#### Primary Key-based Comparison
- Uses `primary_key_columns` to match rows regardless of order
- Builds composite keys from specified columns (e.g., `"col1|col2|col3"`)
- Detects and reports:
  - Missing rows (in expected but not actual)
  - Unexpected rows (in actual but not expected)  
  - Value mismatches in matching rows
- Handles unordered data correctly

### Type Comparison System

The validator implements sophisticated type comparison logic:

#### Spanner-Specific Types
- **`big.Rat`** (NUMERIC/DECIMAL): Compares with optional floating-point tolerance
- **`time.Time`** (TIMESTAMP): Supports multiple time formats with optional truncation
- **`[]byte`** (BYTES): Handles base64 encoding/decoding automatically
- **`map[string]any`** (JSON): Deep comparison with configurable key order independence

#### Numeric Type Flexibility
- Handles int ↔ int64 ↔ float64 conversions common in Spanner
- Applies tolerance-based comparison for floating-point values
- Falls back to string representation for unknown numeric types

#### Comparison Options
```go
type ComparisonOptions struct {
    FloatTolerance      float64       // Default: 1e-9
    TimestampTruncateTo time.Duration // Default: 0 (no truncation)
    IgnoreJSONKeyOrder  bool          // Default: true
    AllowUnorderedRows  bool          // Deprecated: use primary_key_columns
}
```

## Configuration Format

### Multi-row Validation (Recommended)
```yaml
tables:
  Users:
    count: 3
    order_by: "UserID"                    # Ensures consistent ordering
    primary_key_columns: ["UserID"]      # Enables unordered comparison
    rows:
      - UserID: "user-001"
        Name: "Alice Johnson"
        Email: "alice@example.com"
        Status: 1
      - UserID: "user-002"
        Name: "Bob Smith"
        Status: 2
```

### Legacy Single-row Validation
```yaml
tables:
  Products:
    count: 1
    columns:                            # Validates only first row
      ProductID: "prod-001"
      Name: "Test Product"
      Price: 1000
      IsActive: true
```

### JSON Data Validation
```yaml
tables:
  JsonTable:
    count: 1
    rows:
      - ID: "json-001"
        Data: '{"name": "John", "age": 30}'    # String representation
        Metadata: '{"active": true}'
```

## Development Environment

### Requirements
- **Go 1.24.3+**
- **Docker** for Spanner emulator integration tests
- **Network access** to pull `gcr.io/cloud-spanner-emulator/emulator:1.5.6`

### Integration Testing
- Uses Docker-based Spanner emulator on ports 9010 (gRPC) and 9020 (HTTP)
- Automatically creates test database schema via `scripts/setup-emulator.go`
- Loads test fixtures from `testdata/fixtures/` using go-testfixtures
- Environment variable: `SPANNER_EMULATOR_HOST=localhost:9010`

### Test Data Management
- **Fixtures**: `testdata/fixtures/*.yml` - Test data in go-testfixtures format
- **Validation configs**: `testdata/validation.yaml` - Example validation configuration
- **Schema setup**: `scripts/setup-emulator.go` - Database schema creation for tests

## Project-Specific Notes

- **Recent rename**: Project was renamed from "spanlidate" to "spalidate"
- **Version management**: Uses release-please for automated releases
- **CLI framework**: Uses Go's standard `flag` package for argument parsing
- **Error reporting**: Comprehensive error messages with type information and value formatting
- **Verbose mode**: Provides detailed success messages for debugging and verification