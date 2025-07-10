package testutil

import (
	"database/sql"
	"fmt"

	"github.com/go-testfixtures/testfixtures/v3"
	_ "github.com/googleapis/go-sql-spanner"
)

var (
	fixtures *testfixtures.Loader
)

// InitFixtures initializes the test fixtures loader for Spanner
func InitFixtures(project, instance, database string) error {
	// Create database connection using Spanner SQL driver
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Create testfixtures loader
	// For Spanner, we need to specify files in order (parents before children)
	// because of INTERLEAVED table support
	fixtures, err = testfixtures.New(
		testfixtures.Database(db),
		testfixtures.Dialect("spanner"),
		testfixtures.DangerousSkipTestDatabaseCheck(), // Skip database check for emulator
		testfixtures.Files(
			"testdata/fixtures/users.yml",    // Parent table first
			"testdata/fixtures/products.yml", // Independent table
			"testdata/fixtures/orders.yml",   // Child table (interleaved with Users)
			"testdata/fixtures/json.yaml",   // JSONTestTable table
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create fixtures loader: %w", err)
	}

	return nil
}

// LoadFixtures loads all fixture files into the database
func LoadFixtures() error {
	if fixtures == nil {
		return fmt.Errorf("fixtures not initialized, call InitFixtures() first")
	}

	if err := fixtures.Load(); err != nil {
		return fmt.Errorf("failed to load fixtures: %w", err)
	}

	return nil
}

// ResetSequences resets database sequences (if supported by Spanner)
func ResetSequences() error {
	if fixtures == nil {
		return fmt.Errorf("fixtures not initialized, call InitFixtures() first")
	}

	// Note: Spanner may not support sequence resetting
	// This is a placeholder for potential future support
	return nil
}
