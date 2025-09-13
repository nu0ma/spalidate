package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanemuboost"
	tcspanner "github.com/testcontainers/testcontainers-go/modules/gcloud/spanner"
)

var (
	emulatorHost string
	emulator     *tcspanner.Container
)

var schema = `
CREATE TABLE Users (
	UserID STRING(36) NOT NULL,
	Name STRING(100) NOT NULL,
	Email STRING(255) NOT NULL,
	Status INT64 NOT NULL,
	CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (UserID);

CREATE TABLE Products (
	ProductID STRING(36) NOT NULL,
	Name STRING(200) NOT NULL,
	Price INT64 NOT NULL,
	IsActive BOOL NOT NULL,
	CategoryID STRING(36),
	CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (ProductID);

CREATE TABLE Books (
	BookID STRING(36) NOT NULL,
	Title STRING(200) NOT NULL,
	Author STRING(100) NOT NULL,
	PublishedYear INT64 NOT NULL,
	JSONData STRING(MAX)
) PRIMARY KEY (BookID);
`

var ddls = parseSchemaStatements(string(schema))

func TestMain(m *testing.M) {
	ctx := context.Background()

	var teardown func()
	var err error
	emulator, teardown, err = spanemuboost.NewEmulator(ctx,
		spanemuboost.EnableInstanceAutoConfigOnly(),
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create emulator: %v", err))
	}

	host, err := emulator.Host(ctx)
	if err != nil {
		panic(fmt.Sprintf("Failed to get emulator host: %v", err))
	}
	port, err := emulator.MappedPort(ctx, "9010/tcp")
	if err != nil {
		panic(fmt.Sprintf("Failed to get emulator port: %v", err))
	}
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	if err := os.Setenv("SPANNER_EMULATOR_HOST", emulatorHost); err != nil {
		panic(fmt.Sprintf("Failed to set SPANNER_EMULATOR_HOST: %v", err))
	}

	code := m.Run()
	teardown()
	os.Exit(code)
}

func parseSchemaStatements(schema string) []string {
	lines := strings.Split(schema, "\n")
	var cleanLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "--") {
			cleanLines = append(cleanLines, line)
		}
	}
	cleanContent := strings.Join(cleanLines, "\n")

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

var fixedTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

func runSpalidateWithFile(filePath string, verbose bool, project, instance, database string) (string, error) {
	buildCmd := exec.Command("go", "build", "-o", "spalidate", "main.go")
	if err := buildCmd.Run(); err != nil {
		return "", fmt.Errorf("failed to build spalidate: %w", err)
	}

	// Run spalidate with the file path
	args := []string{
		"--project", project,
		"--instance", instance,
		"--database", database,
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

func initializeTestData(ctx context.Context, client *spanner.Client) error {
	if err := insertTestData(ctx, client, []*spanner.Mutation{
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
	if err := insertTestData(ctx, client, []*spanner.Mutation{
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
	if err := insertTestData(ctx, client, []*spanner.Mutation{
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

func insertTestData(ctx context.Context, client *spanner.Client, mutations []*spanner.Mutation) error {
	_, err := client.Apply(ctx, mutations)
	return err
}

// Test Cases

func TestCLIValidation(t *testing.T) {
	ctx := context.Background()
	t.Parallel()

	t.Run("TestWithExistingValidationFile", func(t *testing.T) {
		t.Parallel()
		clients, clientsTeardown, err := spanemuboost.NewClients(ctx, emulator,
			spanemuboost.EnableDatabaseAutoConfigOnly(),
			spanemuboost.WithRandomDatabaseID(),
			spanemuboost.WithSetupDDLs(ddls),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer clientsTeardown()

		if err := initializeTestData(ctx, clients.Client); err != nil {
			t.Fatal(err)
		}

		// Run validation with test_validation.yaml
		output, err := runSpalidateWithFile("test_validation.yaml", true, clients.ProjectID, clients.InstanceID, clients.DatabaseID)
		if err != nil {
			t.Fatalf("Validation failed: %v\nOutput: %s", err, output)
		}

		// Check output
		if !strings.Contains(output, "Validation passed for all tables") {
			t.Errorf("Expected success message, got: %s", output)
		}
	})

	t.Run("TestMultipleColumnsMatching_Success", func(t *testing.T) {
		t.Parallel()
		clients, clientsTeardown, err := spanemuboost.NewClients(ctx, emulator,
			spanemuboost.EnableDatabaseAutoConfigOnly(),
			spanemuboost.WithRandomDatabaseID(),
			spanemuboost.WithSetupDDLs(ddls),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer clientsTeardown()

		if err := initializeTestData(ctx, clients.Client); err != nil {
			t.Fatal(err)
		}

		output, err := runSpalidateWithFile("test_multi_columns.yaml", true, clients.ProjectID, clients.InstanceID, clients.DatabaseID)
		if err != nil {
			t.Fatalf("Validation should succeed, got error: %v\nOutput: %s", err, output)
		}
		if !strings.Contains(output, "Validation passed for all tables") {
			t.Errorf("Expected success message, got: %s", output)
		}
	})

	t.Run("TestMultipleColumnsMatching_Failure", func(t *testing.T) {
		t.Parallel()
		clients, clientsTeardown, err := spanemuboost.NewClients(ctx, emulator,
			spanemuboost.EnableDatabaseAutoConfigOnly(),
			spanemuboost.WithRandomDatabaseID(),
			spanemuboost.WithSetupDDLs(ddls),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer clientsTeardown()

		if err := initializeTestData(ctx, clients.Client); err != nil {
			t.Fatal(err)
		}

		output, err := runSpalidateWithFile("test_multi_columns_fail.yaml", true, clients.ProjectID, clients.InstanceID, clients.DatabaseID)
		if err == nil {
			t.Fatalf("Validation should fail but succeeded. Output: %s", output)
		}
		if !strings.Contains(output, "no row strictly matched spec") && !strings.Contains(output, "no row matched expected spec") {
			t.Errorf("Expected failure reason about unmatched spec. Output: %s", output)
		}
	})
}
