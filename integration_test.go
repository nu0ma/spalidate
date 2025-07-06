//go:build integration
// +build integration

package main

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/nu0ma/spalidate/internal/testutil"
)

const (
	spannerEmulatorHost = "localhost:9010"
	testProject         = "test-project"
	testInstance        = "test-instance"
	testDatabase        = "test-database"
	validationFile      = "testdata/validation.yaml"
)

func TestIntegrationSpalidate(t *testing.T) {
	// Set environment variable for Spanner emulator
	os.Setenv("SPANNER_EMULATOR_HOST", spannerEmulatorHost)

	// Wait for Spanner emulator to be ready
	if err := waitForSpannerEmulator(); err != nil {
		t.Fatalf("Spanner emulator is not ready: %v", err)
	}

	// Setup test database with fixtures
	ctx := context.Background()
	if err := setupTestDatabase(ctx, t); err != nil {
		t.Fatalf("Failed to setup test database: %v", err)
	}

	// Build the spalidate binary
	buildCmd := exec.Command("go", "build", "-o", "spalidate-test", "main.go")
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build spalidate: %v", err)
	}
	defer os.Remove("spalidate-test")

	// Test successful validation
	t.Run("SuccessfulValidation", func(t *testing.T) {
		cmd := exec.Command("./spalidate-test",
			"--project", testProject,
			"--instance", testInstance,
			"--database", testDatabase,
			"--port", "9010",
			"--verbose",
			validationFile,
		)

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("spalidate command failed: %v\nOutput: %s", err, string(output))
		}

		outputStr := string(output)
		if !contains(outputStr, "✅ All validations passed!") {
			t.Errorf("Expected successful validation message, got: %s", outputStr)
		}

		// Check that all tables were validated
		expectedMessages := []string{
			"Table Users: row count matches",
			"Table Products: row count matches",
			"Table Orders: row count matches",
		}

		for _, msg := range expectedMessages {
			if !contains(outputStr, msg) {
				t.Errorf("Expected message '%s' not found in output: %s", msg, outputStr)
			}
		}
	})

	// Test failure case - wrong count validation
	t.Run("FailureValidation", func(t *testing.T) {
		// Create a validation file with wrong count
		wrongValidationContent := `tables:
  Users:
    count: 5  # Wrong count (actual is 3)
    order_by: "UserID"
    columns:
      UserID: "user-001"
      Name: "Alice Johnson"
`

		wrongValidationFile := "testdata/validation_wrong.yaml"
		if err := os.WriteFile(wrongValidationFile, []byte(wrongValidationContent), 0644); err != nil {
			t.Fatalf("Failed to create wrong validation file: %v", err)
		}
		defer os.Remove(wrongValidationFile)

		cmd := exec.Command("./spalidate-test",
			"--project", testProject,
			"--instance", testInstance,
			"--database", testDatabase,
			"--port", "9010",
			wrongValidationFile,
		)

		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("Expected spalidate to fail with wrong count, but it succeeded")
		}

		outputStr := string(output)
		if !contains(outputStr, "Validation failed:") {
			t.Errorf("Expected validation failure message, got: %s", outputStr)
		}

		if !contains(outputStr, "expected 5 rows, got 3") {
			t.Errorf("Expected count mismatch message, got: %s", outputStr)
		}
	})

	// Test failure case - wrong column values
	t.Run("FailureValidationWrongColumns", func(t *testing.T) {
		// Create a validation file with correct count but wrong column values
		wrongColumnsContent := `tables:
  Users:
    count: 3
    order_by: "UserID"
    rows:
      - UserID: "user-001"
        Name: "Wrong Name"  # Should be "Alice Johnson"
        Email: "wrong@example.com"  # Should be "alice@example.com"
        Status: 2  # Should be 1
`

		wrongColumnsFile := "testdata/validation_wrong_columns.yaml"
		if err := os.WriteFile(wrongColumnsFile, []byte(wrongColumnsContent), 0644); err != nil {
			t.Fatalf("Failed to create wrong columns validation file: %v", err)
		}
		defer os.Remove(wrongColumnsFile)

		cmd := exec.Command("./spalidate-test",
			"--project", testProject,
			"--instance", testInstance,
			"--database", testDatabase,
			"--port", "9010",
			wrongColumnsFile,
		)

		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("Expected spalidate to fail with wrong column values, but it succeeded")
		}

		outputStr := string(output)
		if !contains(outputStr, "Validation failed:") {
			t.Errorf("Expected validation failure message, got: %s", outputStr)
		}

		// Check for column validation error messages
		if !contains(outputStr, "column") || !contains(outputStr, "expected") {
			t.Errorf("Expected column validation error in output: %s", outputStr)
		}
	})

	// Test primary key-based comparison
	t.Run("PrimaryKeyBasedComparison", func(t *testing.T) {
		// Create a validation file that uses primary key columns for order-independent comparison
		primaryKeyContent := `tables:
  Users:
    count: 3
    primary_key_columns: ["UserID"]
    rows:
      - UserID: "user-003"  # Different order than database
        Name: "Charlie Brown"
        Email: "charlie@example.com"
        Status: 1
      - UserID: "user-001"
        Name: "Alice Johnson"
        Email: "alice@example.com"
        Status: 1
      - UserID: "user-002"
        Name: "Bob Smith"
        Email: "bob@example.com"
        Status: 2
`

		primaryKeyFile := "testdata/validation_primary_key.yaml"
		if err := os.WriteFile(primaryKeyFile, []byte(primaryKeyContent), 0644); err != nil {
			t.Fatalf("Failed to create primary key validation file: %v", err)
		}
		defer os.Remove(primaryKeyFile)

		cmd := exec.Command("./spalidate-test",
			"--project", testProject,
			"--instance", testInstance,
			"--database", testDatabase,
			"--port", "9010",
			"--verbose",
			primaryKeyFile,
		)

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("spalidate command failed: %v\nOutput: %s", err, string(output))
		}

		outputStr := string(output)
		if !contains(outputStr, "✅ All validations passed!") {
			t.Errorf("Expected successful validation message, got: %s", outputStr)
		}
	})

	// Test numeric tolerance feature
	t.Run("NumericToleranceComparison", func(t *testing.T) {
		// Create a validation file that tests numeric tolerance
		toleranceContent := `tables:
  Products:
    count: 3
    order_by: "ProductID"
    rows:
      - ProductID: "prod-001"
        Name: "Laptop Computer"
        Price: 150000  # Exact match
        IsActive: true
      - ProductID: "prod-002"
        Name: "Wireless Mouse"
        Price: 3000  # Exact match
        IsActive: true
      - ProductID: "prod-003"
        Name: "Coffee Mug"
        Price: 1200  # Exact match
        IsActive: false
`

		toleranceFile := "testdata/validation_tolerance.yaml"
		if err := os.WriteFile(toleranceFile, []byte(toleranceContent), 0644); err != nil {
			t.Fatalf("Failed to create tolerance validation file: %v", err)
		}
		defer os.Remove(toleranceFile)

		cmd := exec.Command("./spalidate-test",
			"--project", testProject,
			"--instance", testInstance,
			"--database", testDatabase,
			"--port", "9010",
			"--verbose",
			toleranceFile,
		)

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("spalidate command failed: %v\nOutput: %s", err, string(output))
		}

		outputStr := string(output)
		if !contains(outputStr, "✅ All validations passed!") {
			t.Errorf("Expected successful validation message, got: %s", outputStr)
		}
	})

	// Test composite primary key
	t.Run("CompositePrimaryKey", func(t *testing.T) {
		// Create a validation file that uses composite primary key
		compositeKeyContent := `tables:
  Orders:
    count: 3
    primary_key_columns: ["UserID", "ProductID"]
    rows:
      - UserID: "user-001"  # Different order than database
        ProductID: "prod-001"
        Quantity: 1
      - UserID: "user-001"
        ProductID: "prod-003"
        Quantity: 1
      - UserID: "user-002"
        ProductID: "prod-002"
        Quantity: 2
`

		compositeKeyFile := "testdata/validation_composite_key.yaml"
		if err := os.WriteFile(compositeKeyFile, []byte(compositeKeyContent), 0644); err != nil {
			t.Fatalf("Failed to create composite key validation file: %v", err)
		}
		defer os.Remove(compositeKeyFile)

		cmd := exec.Command("./spalidate-test",
			"--project", testProject,
			"--instance", testInstance,
			"--database", testDatabase,
			"--port", "9010",
			"--verbose",
			compositeKeyFile,
		)

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("spalidate command failed: %v\nOutput: %s", err, string(output))
		}

		outputStr := string(output)
		if !contains(outputStr, "✅ All validations passed!") {
			t.Errorf("Expected successful validation message, got: %s", outputStr)
		}
	})

	// Test error case - non-existent table
	t.Run("NonExistentTable", func(t *testing.T) {
		nonExistentValidationContent := `tables:
  NonExistentTable:
    count: 1
    columns:
      ID: "test"
`

		nonExistentValidationFile := "testdata/validation_nonexistent.yaml"
		if err := os.WriteFile(nonExistentValidationFile, []byte(nonExistentValidationContent), 0644); err != nil {
			t.Fatalf("Failed to create non-existent validation file: %v", err)
		}
		defer os.Remove(nonExistentValidationFile)

		cmd := exec.Command("./spalidate-test",
			"--project", testProject,
			"--instance", testInstance,
			"--database", testDatabase,
			"--port", "9010",
			nonExistentValidationFile,
		)

		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("Expected spalidate to fail with non-existent table, but it succeeded")
		}

		outputStr := string(output)
		if !contains(outputStr, "does not exist") {
			t.Errorf("Expected table not found message, got: %s", outputStr)
		}
	})
}

func waitForSpannerEmulator() error {
	// Try to connect to the emulator for up to 30 seconds
	for i := 0; i < 30; i++ {
		cmd := exec.Command("nc", "-z", "localhost", "9010")
		if err := cmd.Run(); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return exec.Command("nc", "-z", "localhost", "9010").Run()
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// setupTestDatabase creates the test database schema and loads fixtures
func setupTestDatabase(ctx context.Context, t *testing.T) error {
	// Create Spanner client
	client, err := spanner.NewClient(ctx, "projects/"+testProject+"/instances/"+testInstance+"/databases/"+testDatabase)
	if err != nil {
		return err
	}
	defer client.Close()

	// Create fixture helper
	fixtureHelper := testutil.NewFixtureHelper(client, testDatabase)

	// Load fixtures
	if err := fixtureHelper.LoadFixtures(ctx); err != nil {
		return err
	}

	return nil
}
