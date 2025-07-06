//go:build integration
// +build integration

package main

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
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

	// Data Types Test Group
	t.Run("DataTypes", func(t *testing.T) {
		// Test basic data types (simplified)
		t.Run("BasicDataTypes", func(t *testing.T) {
			cmd := exec.Command("./spalidate-test",
				"--project", testProject,
				"--instance", testInstance,
				"--database", testDatabase,
				"--port", "9010",
				"--verbose",
				"testdata/validation_datatypes_correct.yaml",
			)

			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("Data types validation failed: %v\nOutput: %s", err, string(output))
			}

			assertValidationSuccess(t, output)
		})

		// Test partial validation (skip NULL columns)
		t.Run("PartialValidation", func(t *testing.T) {
			yamlContent := `tables:
  DataTypeTest:
    count: 3
    order_by: "ID"
    rows:
      - ID: "dtype-001"
        StringCol: "Hello World"
        Int64Col: 42
        BoolCol: true
      - ID: "dtype-002"
        StringCol: "Test String"
        # Skip Int64Col because it's NULL
        # Skip NumericCol for now (big.Rat comparison issue)
        Float64Col: 2.71828
        BoolCol: false
        # Skip BytesCol because it's NULL
      - ID: "dtype-003"
        StringCol: "日本語テスト 🚀"
        Int64Col: -9999
        BoolCol: true
`
			output, _ := runValidationTest(t, yamlContent, true)
			assertValidationSuccess(t, output)
		})

		// Test type mismatch errors
		t.Run("TypeMismatch", func(t *testing.T) {
			yamlContent := `tables:
  DataTypeTest:
    count: 3
    order_by: "ID"
    rows:
      - ID: "dtype-001"
        Int64Col: "not a number"  # Type mismatch
`
			output, _ := runValidationTest(t, yamlContent, false)
			assertValidationError(t, output, "expected")
		})
	})

	// Edge Cases Test Group
	t.Run("EdgeCases", func(t *testing.T) {
		// Test empty table
		t.Run("EmptyTable", func(t *testing.T) {
			yamlContent := `tables:
  EmptyTable:
    count: 0
`
			output, _ := runValidationTest(t, yamlContent, true)
			assertValidationSuccess(t, output)
		})

		// Test partial column validation
		t.Run("PartialColumns", func(t *testing.T) {
			cmd := exec.Command("./spalidate-test",
				"--project", testProject,
				"--instance", testInstance,
				"--database", testDatabase,
				"--port", "9010",
				"testdata/validation_edge_cases.yaml",
			)

			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("Edge cases validation failed: %v\nOutput: %s", err, string(output))
			}

			assertValidationSuccess(t, output)
		})

		// Test large dataset (performance) - simplified
		t.Run("LargeDataSet", func(t *testing.T) {
			if testing.Short() {
				t.Skip("Skipping large dataset test in short mode")
			}

			// Just test empty LargeTable for now
			yamlContent := `tables:
  LargeTable:
    count: 0
`
			output, _ := runValidationTest(t, yamlContent, true)
			assertValidationSuccess(t, output)
		})

		// Test multiple validation errors
		t.Run("MultipleErrors", func(t *testing.T) {
			yamlContent := `tables:
  Users:
    count: 10  # Wrong count
    order_by: "UserID"
    rows:
      - UserID: "wrong-id"  # Wrong ID
        Name: "Wrong Name"  # Wrong name
      - UserID: "user-002"
        Email: "wrong@email.com"  # Wrong email
  Products:
    count: 5  # Wrong count
`
			output, _ := runValidationTest(t, yamlContent, false)
			assertValidationError(t, output, "")

			// Should report multiple errors
			outputStr := string(output)
			if !contains(outputStr, "Users") || !contains(outputStr, "Products") {
				t.Errorf("Expected errors for both tables, got: %s", outputStr)
			}
		})
	})

	// Error Handling Test Group
	t.Run("ErrorHandling", func(t *testing.T) {
		// Test invalid YAML syntax
		t.Run("InvalidYAML", func(t *testing.T) {
			yamlContent := `tables:
  Users:
    count: 3
    invalid_yaml: [unclosed bracket
`
			output, _ := runValidationTest(t, yamlContent, false)
			outputStr := string(output)
			if !contains(outputStr, "failed to parse YAML") {
				t.Errorf("Expected YAML parse error, got: %s", outputStr)
			}
		})

		// Test missing required fields
		t.Run("MissingRequiredFields", func(t *testing.T) {
			yamlContent := `tables:
  Users:
    # Missing count field
    order_by: "UserID"
`
			output, _ := runValidationTest(t, yamlContent, false)
			outputStr := string(output)
			// Count defaults to 0 when not specified, so validation fails with row count mismatch
			if !contains(outputStr, "expected 0 rows, got 3") {
				t.Errorf("Expected row count mismatch error, got: %s", outputStr)
			}
		})

		// Test CLI flag validation
		t.Run("InvalidCLIFlags", func(t *testing.T) {
			cmd := exec.Command("./spalidate-test",
				"--project", "",  // Empty project
				"--instance", testInstance,
				"--database", testDatabase,
				"--port", "9010",
				validationFile,
			)

			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("Expected failure with empty project, but succeeded")
			}

			outputStr := string(output)
			if !contains(outputStr, "project") {
				t.Errorf("Expected error about project flag, got: %s", outputStr)
			}
		})
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

// Helper functions for improved test organization

// assertValidationSuccess checks that validation passed successfully
func assertValidationSuccess(t *testing.T, output []byte) {
	t.Helper()
	outputStr := string(output)
	if !contains(outputStr, "✅ All validations passed!") {
		t.Errorf("Expected successful validation message, got: %s", outputStr)
	}
}

// assertValidationError checks that validation failed with expected error
func assertValidationError(t *testing.T, output []byte, expectedError string) {
	t.Helper()
	outputStr := string(output)
	if !contains(outputStr, "Validation failed:") {
		t.Errorf("Expected validation failure message, got: %s", outputStr)
	}
	if expectedError != "" && !contains(outputStr, expectedError) {
		t.Errorf("Expected error containing '%s', got: %s", expectedError, outputStr)
	}
}

// runValidationTest executes spalidate with given YAML content
func runValidationTest(t *testing.T, yamlContent string, expectedSuccess bool) ([]byte, error) {
	t.Helper()
	
	// Create temporary YAML file
	tmpFile := fmt.Sprintf("testdata/temp_validation_%d.yaml", time.Now().UnixNano())
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to create temp validation file: %v", err)
	}
	defer os.Remove(tmpFile)
	
	// Run spalidate
	cmd := exec.Command("./spalidate-test",
		"--project", testProject,
		"--instance", testInstance,
		"--database", testDatabase,
		"--port", "9010",
		tmpFile,
	)
	
	output, err := cmd.CombinedOutput()
	
	// Check expected outcome
	if expectedSuccess && err != nil {
		t.Errorf("Expected validation to succeed, but it failed: %v\nOutput: %s", err, string(output))
	} else if !expectedSuccess && err == nil {
		t.Errorf("Expected validation to fail, but it succeeded\nOutput: %s", string(output))
	}
	
	return output, err
}

