//go:build integration
// +build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/nu0ma/spalidate/internal/testutil"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	spannerEmulatorHost = "localhost:9010"
	testProject         = "test-project"
	testInstance        = "test-instance"
	testDatabase        = "test-database"
	validationFile      = "testdata/validation.yaml"
)

func TestMain(m *testing.M) {
	// Set environment variable for Spanner emulator
	os.Setenv("SPANNER_EMULATOR_HOST", spannerEmulatorHost)

	// Wait for Spanner emulator to be ready
	if err := waitForSpannerEmulator(); err != nil {
		panic("Spanner emulator is not ready: " + err.Error())
	}

	// Create instance and database
	if err := setupSpannerInstance(); err != nil {
		panic("Failed to setup Spanner instance: " + err.Error())
	}

	// Initialize fixtures
	if err := testutil.InitFixtures(testProject, testInstance, testDatabase); err != nil {
		panic("Failed to initialize fixtures: " + err.Error())
	}

	// Run tests
	code := m.Run()
	os.Exit(code)
}

func prepareTestDatabase() error {
	return testutil.LoadFixtures()
}

func TestIntegrationSpalidate(t *testing.T) {

	// Build the spalidate binary
	buildCmd := exec.Command("go", "build", "-o", "spalidate-test", "main.go")
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build spalidate: %v", err)
	}
	defer os.Remove("spalidate-test")

	// Test successful validation
	t.Run("SuccessfulValidation", func(t *testing.T) {
		// Load test fixtures
		if err := prepareTestDatabase(); err != nil {
			t.Fatalf("Failed to prepare test database: %v", err)
		}
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
		// Load test fixtures
		if err := prepareTestDatabase(); err != nil {
			t.Fatalf("Failed to prepare test database: %v", err)
		}
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
		// Load test fixtures
		if err := prepareTestDatabase(); err != nil {
			t.Fatalf("Failed to prepare test database: %v", err)
		}
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
		// Load test fixtures
		if err := prepareTestDatabase(); err != nil {
			t.Fatalf("Failed to prepare test database: %v", err)
		}
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
		// Load test fixtures
		if err := prepareTestDatabase(); err != nil {
			t.Fatalf("Failed to prepare test database: %v", err)
		}
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
		// Load test fixtures
		if err := prepareTestDatabase(); err != nil {
			t.Fatalf("Failed to prepare test database: %v", err)
		}
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
		// Load test fixtures
		if err := prepareTestDatabase(); err != nil {
			t.Fatalf("Failed to prepare test database: %v", err)
		}
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

func setupSpannerInstance() error {
	ctx := context.Background()

	// Set up clients for emulator
	opts := []option.ClientOption{
		option.WithEndpoint(spannerEmulatorHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	// Create instance admin client
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to create instance admin client: %w", err)
	}
	defer instanceAdminClient.Close()

	// Create instance
	instanceReq := &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + testProject,
		InstanceId: testInstance,
		Instance: &instancepb.Instance{
			DisplayName: "Test Instance",
			Config:      "projects/" + testProject + "/instanceConfigs/emulator-config",
			NodeCount:   1,
		},
	}

	instanceOp, err := instanceAdminClient.CreateInstance(ctx, instanceReq)
	if err != nil {
		// Instance might already exist, continue
		fmt.Printf("Instance creation failed (may already exist): %v\n", err)
	} else {
		_, err = instanceOp.Wait(ctx)
		if err != nil {
			fmt.Printf("Failed to wait for instance creation: %v\n", err)
		}
	}

	// Create database admin client
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to create database admin client: %w", err)
	}
	defer databaseAdminClient.Close()

	// Create database
	databaseReq := &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", testDatabase),
		ExtraStatements: []string{
			"CREATE TABLE Users (UserID STRING(36) NOT NULL, Name STRING(100) NOT NULL, Email STRING(255) NOT NULL, Status INT64 NOT NULL, CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (UserID)",
			"CREATE TABLE Products (ProductID STRING(36) NOT NULL, Name STRING(200) NOT NULL, Price INT64 NOT NULL, IsActive BOOL NOT NULL, CategoryID STRING(36), CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ProductID)",
			"CREATE TABLE Orders (OrderID STRING(36) NOT NULL, UserID STRING(36) NOT NULL, ProductID STRING(36) NOT NULL, Quantity INT64 NOT NULL, OrderDate TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (UserID, ProductID), INTERLEAVE IN PARENT Users ON DELETE CASCADE",
		},
	}

	databaseOp, err := databaseAdminClient.CreateDatabase(ctx, databaseReq)
	if err != nil {
		// Database might already exist, continue
		fmt.Printf("Database creation failed (may already exist): %v\n", err)
		return nil
	}

	_, err = databaseOp.Wait(ctx)
	if err != nil {
		fmt.Printf("Failed to wait for database creation: %v\n", err)
	}

	return nil
}
