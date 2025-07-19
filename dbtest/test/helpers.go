//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/nu0ma/spalidate/internal/testutil"
	_ "github.com/googleapis/go-sql-spanner"
)

const (
	testProject  = "test-project"
	testInstance = "test-instance"
	testDatabase = "test-database"
)

// TestConfig holds configuration for running spalidate tests
type TestConfig struct {
	Project     string
	Instance    string
	Database    string
	Port        string
	Verbose     bool
	BinaryPath  string
	ValidationFile string
}

// DefaultTestConfig returns a default test configuration
func DefaultTestConfig() *TestConfig {
	return &TestConfig{
		Project:    testProject,
		Instance:   testInstance,
		Database:   testDatabase,
		Port:       "9010",
		Verbose:    false,
		BinaryPath: "spalidate-test",
	}
}

// WithVerbose enables verbose output
func (tc *TestConfig) WithVerbose() *TestConfig {
	tc.Verbose = true
	return tc
}

// WithValidationFile sets the validation file path
func (tc *TestConfig) WithValidationFile(path string) *TestConfig {
	tc.ValidationFile = path
	return tc
}

// RunSpalidate executes spalidate with the given configuration
func RunSpalidate(t *testing.T, config *TestConfig) ([]byte, error) {
	t.Helper()

	// Prepare test database
	if err := prepareTestDatabase(t); err != nil {
		t.Fatalf("Failed to prepare test database: %v", err)
	}

	// Build command arguments
	args := []string{
		"--project", config.Project,
		"--instance", config.Instance,
		"--database", config.Database,
		"--port", config.Port,
	}

	if config.Verbose {
		args = append(args, "--verbose")
	}

	if config.ValidationFile != "" {
		args = append(args, config.ValidationFile)
	}

	// Execute command
	cmd := exec.Command(config.BinaryPath, args...)
	return cmd.CombinedOutput()
}

// prepareTestDatabase prepares the test database with fixtures
func prepareTestDatabase(t *testing.T) error {
	// Open database connection
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProject, testInstance, testDatabase)
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Load test data from SQL file
	sqlFilePath := filepath.Join("..", "fixtures", "data.sql")
	testutil.LoadSQLInBatchesBySplitter(t, db, sqlFilePath, []byte(";\n"))
	
	return nil
}

// BuildSpalidate builds the spalidate binary for testing
func BuildSpalidate(t *testing.T) string {
	t.Helper()

	binaryPath := "./spalidate-test"
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "../../main.go")
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build spalidate: %v", err)
	}

	// Clean up binary after test
	t.Cleanup(func() {
		os.Remove(binaryPath)
	})

	return binaryPath
}

// AssertContains checks if the output contains the expected substring
func AssertContains(t *testing.T, output, expected string) {
	t.Helper()
	if !contains(output, expected) {
		t.Errorf("Expected output to contain '%s', but got: %s", expected, output)
	}
}

// AssertNotContains checks if the output does not contain the substring
func AssertNotContains(t *testing.T, output, unexpected string) {
	t.Helper()
	if contains(output, unexpected) {
		t.Errorf("Expected output not to contain '%s', but got: %s", unexpected, output)
	}
}

// AssertCommandSuccess checks if command execution was successful
func AssertCommandSuccess(t *testing.T, output []byte, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("spalidate command failed: %v\nOutput: %s", err, string(output))
	}
}

// AssertCommandFailure checks if command execution failed as expected
func AssertCommandFailure(t *testing.T, output []byte, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected spalidate to fail, but it succeeded. Output: %s", string(output))
	}
}

// GetTestDataPath returns the path to a test data file
func GetTestDataPath(filename string) string {
	return filepath.Join("testdata", filename)
}

// CreateTempValidationFile creates a temporary validation file with the given content
func CreateTempValidationFile(t *testing.T, content, filename string) string {
	t.Helper()

	filepath := GetTestDataPath(filename)
	if err := os.WriteFile(filepath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp validation file: %v", err)
	}

	// Clean up file after test
	t.Cleanup(func() {
		os.Remove(filepath)
	})

	return filepath
}

// contains checks if a string contains a substring (helper from original code)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			findSubstring(s, substr)))
}

// findSubstring finds a substring in a string (helper from original code)
func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}