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

// === Test data (DML) centralized here ===
var fixedTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

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

func initializeTestData(ctx context.Context) error {
	if err := insertTestData(ctx, []*spanner.Mutation{
		spanner.Insert("Books",
			[]string{"BookID", "Title", "Author", "PublishedYear", "JSONData"},
			[]any{"book-001", "The Great Gatsby", "F. Scott Fitzgerald", int64(1925), `{"genre": "Fiction", "rating": 4.5}`}),
		spanner.Insert("Books",
			[]string{"BookID", "Title", "Author", "PublishedYear", "JSONData"},
			[]any{"book-002", "To Kill a Mockingbird", "Harper Lee", int64(1960), `{"genre": "Fiction", "rating": 4.8}`}),
		spanner.Insert("Books",
			[]string{"BookID", "Title", "Author", "PublishedYear", "JSONData"},
			[]any{"book-003", "1984", "George Orwell", int64(1949), `{"genre": "Dystopian", "rating": 4.6}`}),
	}); err != nil {
		return fmt.Errorf("failed to insert test users: %w", err)
	}
	if err := insertTestData(ctx, []*spanner.Mutation{
		spanner.Insert("Products",
			[]string{"ProductID", "Name", "Price", "IsActive", "CategoryID", "CreatedAt"},
			[]any{"prod-001", "Laptop Computer", int64(150000), true, "cat-electronics", fixedTime}),
		spanner.Insert("Products",
			[]string{"ProductID", "Name", "Price", "IsActive", "CategoryID", "CreatedAt"},
			[]any{"prod-002", "Wireless Mouse", int64(3000), true, "cat-electronics", fixedTime}),
		spanner.Insert("Products",
			[]string{"ProductID", "Name", "Price", "IsActive", "CategoryID", "CreatedAt"},
			[]any{"prod-003", "Coffee Mug", int64(1200), false, "cat-kitchen", fixedTime}),
	}); err != nil {
		return fmt.Errorf("failed to insert test products: %w", err)
	}
	if err := insertTestData(ctx, []*spanner.Mutation{
		spanner.Insert("Users",
			[]string{"UserID", "Name", "Email", "Status", "CreatedAt"},
			[]any{"user-001", "Alice Johnson", "alice@example.com", int64(1), fixedTime}),
		spanner.Insert("Users",
			[]string{"UserID", "Name", "Email", "Status", "CreatedAt"},
			[]any{"user-002", "Bob Smith", "bob@example.com", int64(2), fixedTime}),
		spanner.Insert("Users",
			[]string{"UserID", "Name", "Email", "Status", "CreatedAt"},
			[]any{"user-003", "Charlie Brown", "charlie@example.com", int64(1), fixedTime}),
	}); err != nil {
		return fmt.Errorf("failed to insert test books: %w", err)
	}
	return nil
}

func cleanupTestData(ctx context.Context) error {
	stmt := spanner.Statement{SQL: `
        SELECT t.table_name
        FROM information_schema.tables AS t
        WHERE t.table_catalog = '' AND t.table_schema = ''
    `}

	iter := spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var tableNames []string
	err := iter.Do(func(row *spanner.Row) error {
		var name string
		if err := row.Columns(&name); err != nil {
			return err
		}
		tableNames = append(tableNames, name)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}

	for _, tbl := range tableNames {
		stmtDel := spanner.Statement{SQL: fmt.Sprintf("DELETE FROM %s WHERE TRUE", tbl)}
		if _, err := spannerClient.PartitionedUpdate(ctx, stmtDel); err != nil {
			return fmt.Errorf("failed to cleanup table %s: %w", tbl, err)
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
		if err := cleanupTestData(ctx); err != nil {
			t.Fatal(err)
		}

		if err := initializeTestData(ctx); err != nil {
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

	t.Run("TestMultipleColumnsMatching_Success", func(t *testing.T) {
		if err := cleanupTestData(ctx); err != nil {
			t.Fatal(err)
		}

		if err := initializeTestData(ctx); err != nil {
			t.Fatal(err)
		}

		output, err := runSpalidateWithFile("test_multi_columns.yaml", true)
		if err != nil {
			t.Fatalf("Validation should succeed, got error: %v\nOutput: %s", err, output)
		}
		if !strings.Contains(output, "Validation passed for all tables") {
			t.Errorf("Expected success message, got: %s", output)
		}
	})

	t.Run("TestMultipleColumnsMatching_Failure", func(t *testing.T) {
		if err := cleanupTestData(ctx); err != nil {
			t.Fatal(err)
		}
		if err := initializeTestData(ctx); err != nil {
			t.Fatal(err)
		}

		output, err := runSpalidateWithFile("test_multi_columns_fail.yaml", true)
		if err == nil {
			t.Fatalf("Validation should fail but succeeded. Output: %s", output)
		}
		if !(strings.Contains(output, "no row strictly matched spec") || strings.Contains(output, "no row matched expected spec")) {
			t.Errorf("Expected failure reason about unmatched spec. Output: %s", output)
		}
	})
}
