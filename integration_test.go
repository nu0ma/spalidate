package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanemuboost"
)

var (
	testProject   = "test-project"
	testInstance  = "test-instance"
	testDatabase  = "test-database"
	emulatorHost  string
	spannerClient *spanner.Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Read schema
	schemaPath := filepath.Join("dbtest", "fixtures", "schema.sql")
	schemaContent, err := os.ReadFile(schemaPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to read schema file: %v", err))
	}

	ddls := parseSchemaStatements(string(schemaContent))

	// Add Books table schema for test_validation.yaml
	booksTableDDL := `CREATE TABLE Books (
		BookID STRING(36) NOT NULL,
		Title STRING(200) NOT NULL,
		Author STRING(100) NOT NULL,
		PublishedYear INT64 NOT NULL,
		JSONData STRING(MAX)
	) PRIMARY KEY (BookID)`
	ddls = append(ddls, booksTableDDL)

	// Start emulator with schema
	emulator, clients, teardown, err := spanemuboost.NewEmulatorWithClients(ctx,
		spanemuboost.WithProjectID(testProject),
		spanemuboost.WithInstanceID(testInstance),
		spanemuboost.WithDatabaseID(testDatabase),
		spanemuboost.WithSetupDDLs(ddls),
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create emulator: %v", err))
	}
	defer teardown()

	// Get emulator host
	host, err := emulator.Host(ctx)
	if err != nil {
		panic(fmt.Sprintf("Failed to get emulator host: %v", err))
	}
	port, err := emulator.MappedPort(ctx, "9010/tcp")
	if err != nil {
		panic(fmt.Sprintf("Failed to get emulator port: %v", err))
	}
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	os.Setenv("SPANNER_EMULATOR_HOST", emulatorHost)

	// Use the Spanner client from emulator
	spannerClient = clients.Client

	// Run tests
	code := m.Run()
	os.Exit(code)
}

func parseSchemaStatements(schema string) []string {
	// Remove comments and split by semicolon
	lines := strings.Split(schema, "\n")
	var cleanLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "--") {
			cleanLines = append(cleanLines, line)
		}
	}
	cleanContent := strings.Join(cleanLines, "\n")

	// Split by semicolon to get statements
	statements := strings.Split(cleanContent, ";")
	var result []string
	for _, stmt := range statements {
		trimmed := strings.TrimSpace(stmt)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}

// Helper functions

func runSpalidate(configPath string, verbose bool) (string, error) {
	// Build the spalidate binary first
	buildCmd := exec.Command("go", "build", "-o", "spalidate", "main.go")
	if err := buildCmd.Run(); err != nil {
		return "", fmt.Errorf("failed to build spalidate: %w", err)
	}

	// Run spalidate with the test configuration
	args := []string{
		"--project", testProject,
		"--instance", testInstance,
		"--database", testDatabase,
		configPath,
	}
	if verbose {
		args = append([]string{"--verbose"}, args...)
	}

	cmd := exec.Command("./spalidate", args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("SPANNER_EMULATOR_HOST=%s", emulatorHost))

	output, err := cmd.CombinedOutput()
	return string(output), err
}

func runSpalidateWithFile(filePath string, verbose bool) (string, error) {
	// Build the spalidate binary first
	buildCmd := exec.Command("go", "build", "-o", "spalidate", "main.go")
	if err := buildCmd.Run(); err != nil {
		return "", fmt.Errorf("failed to build spalidate: %w", err)
	}

	// Run spalidate with the file path
	args := []string{
		"--project", testProject,
		"--instance", testInstance,
		"--database", testDatabase,
		filePath,
	}
	if verbose {
		args = append([]string{"--verbose"}, args...)
	}

	cmd := exec.Command("./spalidate", args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("SPANNER_EMULATOR_HOST=%s", emulatorHost))

	output, err := cmd.CombinedOutput()
	return string(output), err
}

func cleanupTestData(ctx context.Context, tables ...string) error {
	for _, table := range tables {
		_, err := spannerClient.Apply(ctx, []*spanner.Mutation{
			spanner.Delete(table, spanner.AllKeys()),
		})
		if err != nil {
			return fmt.Errorf("failed to cleanup table %s: %w", table, err)
		}
	}
	return nil
}

func insertTestData(ctx context.Context, mutations []*spanner.Mutation) error {
	_, err := spannerClient.Apply(ctx, mutations)
	return err
}

// Test Cases

func TestCLIValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("TestWithExistingValidationFile", func(t *testing.T) {
		// Clean up first
		if err := cleanupTestData(ctx, "Users", "Products", "Books"); err != nil {
			t.Fatal(err)
		}

		// Insert test data for Users
		userMutations := []*spanner.Mutation{
			spanner.Insert("Users",
				[]string{"UserID", "Name", "Email", "Status", "CreatedAt"},
				[]interface{}{"user-001", "Alice Johnson", "alice@example.com", int64(1), spanner.CommitTimestamp}),
			spanner.Insert("Users",
				[]string{"UserID", "Name", "Email", "Status", "CreatedAt"},
				[]interface{}{"user-002", "Bob Smith", "bob@example.com", int64(2), spanner.CommitTimestamp}),
			spanner.Insert("Users",
				[]string{"UserID", "Name", "Email", "Status", "CreatedAt"},
				[]interface{}{"user-003", "Charlie Brown", "charlie@example.com", int64(1), spanner.CommitTimestamp}),
		}
		if err := insertTestData(ctx, userMutations); err != nil {
			t.Fatal(err)
		}

		// Insert test data for Products
		productMutations := []*spanner.Mutation{
			spanner.Insert("Products",
				[]string{"ProductID", "Name", "Price", "IsActive", "CategoryID", "CreatedAt"},
				[]interface{}{"prod-001", "Laptop Computer", int64(150000), true, "cat-electronics", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}),
			spanner.Insert("Products",
				[]string{"ProductID", "Name", "Price", "IsActive", "CategoryID", "CreatedAt"},
				[]interface{}{"prod-002", "Wireless Mouse", int64(3000), true, "cat-electronics", spanner.CommitTimestamp}),
			spanner.Insert("Products",
				[]string{"ProductID", "Name", "Price", "IsActive", "CategoryID", "CreatedAt"},
				[]interface{}{"prod-003", "Coffee Mug", int64(1200), false, "cat-kitchen", spanner.CommitTimestamp}),
		}
		if err := insertTestData(ctx, productMutations); err != nil {
			t.Fatal(err)
		}

		// Insert test data for Books
		bookMutations := []*spanner.Mutation{
			spanner.Insert("Books",
				[]string{"BookID", "Title", "Author", "PublishedYear", "JSONData"},
				[]interface{}{"book-001", "The Great Gatsby", "F. Scott Fitzgerald", int64(1925), `{"genre": "Fiction", "rating": 4.5}`}),
			spanner.Insert("Books",
				[]string{"BookID", "Title", "Author", "PublishedYear", "JSONData"},
				[]interface{}{"book-002", "To Kill a Mockingbird", "Harper Lee", int64(1960), `{"genre": "Fiction", "rating": 4.8}`}),
			spanner.Insert("Books",
				[]string{"BookID", "Title", "Author", "PublishedYear", "JSONData"},
				[]interface{}{"book-003", "1984", "George Orwell", int64(1949), `{"genre": "Dystopian", "rating": 4.6}`}),
		}
		if err := insertTestData(ctx, bookMutations); err != nil {
			t.Fatal(err)
		}

		// Run validation with test_validation.yaml
		output, err := runSpalidateWithFile("test_validation.yaml", true)
		if err != nil {
			t.Fatalf("Validation failed: %v\nOutput: %s", err, output)
		}

		// Check output
		if !strings.Contains(output, "Validation passed for all tables") {
			t.Errorf("Expected success message, got: %s", output)
		}
	})

}
