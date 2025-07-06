package testutil

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"cloud.google.com/go/spanner"
)

// FixtureHelper manages test fixtures for Spanner integration tests
type FixtureHelper struct {
	client     *spanner.Client
	database   string
	fixturesDir string
}

// NewFixtureHelper creates a new fixture helper
func NewFixtureHelper(client *spanner.Client, database string) *FixtureHelper {
	return &FixtureHelper{
		client:     client,
		database:   database,
		fixturesDir: "testdata/fixtures",
	}
}

// LoadFixtures loads all fixture files into the database
func (f *FixtureHelper) LoadFixtures(ctx context.Context) error {
	// Clean existing data first
	if err := f.CleanDatabase(ctx); err != nil {
		return fmt.Errorf("failed to clean database: %w", err)
	}

	// Load fixtures using a custom loader for Spanner
	return f.loadSpannerFixtures(ctx)
}

// CleanDatabase removes all data from test tables
func (f *FixtureHelper) CleanDatabase(ctx context.Context) error {
	// Delete in reverse order to respect foreign key constraints
	tables := []string{"Orders", "Products", "Users"}
	
	for _, table := range tables {
		stmt := spanner.Statement{
			SQL: fmt.Sprintf("DELETE FROM %s WHERE TRUE", table),
		}
		
		_, err := f.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			_, err := txn.Update(ctx, stmt)
			return err
		})
		
		if err != nil {
			return fmt.Errorf("failed to clean table %s: %w", table, err)
		}
	}
	
	return nil
}

// loadSpannerFixtures loads fixtures specifically for Spanner
func (f *FixtureHelper) loadSpannerFixtures(ctx context.Context) error {
	// Load fixtures in dependency order
	fixtures := []struct {
		table string
		file  string
	}{
		{"Users", "users.yml"},
		{"Products", "products.yml"},
		{"Orders", "orders.yml"},
	}
	
	for _, fixture := range fixtures {
		if err := f.loadFixtureFile(ctx, fixture.table, fixture.file); err != nil {
			return fmt.Errorf("failed to load fixture %s: %w", fixture.file, err)
		}
	}
	
	return nil
}

// loadFixtureFile loads a single fixture file
func (f *FixtureHelper) loadFixtureFile(ctx context.Context, table, filename string) error {
	// For now, we'll implement a simple YAML loader
	// This could be extended to use go-testfixtures with a custom driver
	
	// Create a simple SQL loader that reads YAML and converts to Spanner inserts
	filePath := filepath.Join(f.fixturesDir, filename)
	
	// Use testfixtures with a custom approach for Spanner
	// Since testfixtures doesn't directly support Spanner, we'll use a hybrid approach
	
	// This is a simplified implementation - in production you might want to
	// implement a full testfixtures driver for Spanner
	
	return f.loadYAMLFixture(ctx, table, filePath)
}

// loadYAMLFixture loads YAML fixture data into Spanner
func (f *FixtureHelper) loadYAMLFixture(ctx context.Context, table, filePath string) error {
	// This would parse YAML and insert into Spanner
	// For now, we'll implement basic inserts based on the table structure
	
	switch table {
	case "Users":
		return f.loadUsersFixture(ctx)
	case "Products":
		return f.loadProductsFixture(ctx)
	case "Orders":
		return f.loadOrdersFixture(ctx)
	default:
		return fmt.Errorf("unknown table: %s", table)
	}
}

// loadUsersFixture loads user test data
func (f *FixtureHelper) loadUsersFixture(ctx context.Context) error {
	users := []map[string]interface{}{
		{
			"UserID":    "user-001",
			"Name":      "Alice Johnson",
			"Email":     "alice@example.com",
			"Status":    1,
			"CreatedAt": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"UserID":    "user-002",
			"Name":      "Bob Smith",
			"Email":     "bob@example.com",
			"Status":    2,
			"CreatedAt": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"UserID":    "user-003",
			"Name":      "Charlie Brown",
			"Email":     "charlie@example.com",
			"Status":    1,
			"CreatedAt": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	
	return f.insertRecords(ctx, "Users", users)
}

// loadProductsFixture loads product test data
func (f *FixtureHelper) loadProductsFixture(ctx context.Context) error {
	products := []map[string]interface{}{
		{
			"ProductID":  "prod-001",
			"Name":       "Laptop Computer",
			"Price":      150000,
			"IsActive":   true,
			"CategoryID": "cat-electronics",
			"CreatedAt":  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"ProductID":  "prod-002",
			"Name":       "Wireless Mouse",
			"Price":      3000,
			"IsActive":   true,
			"CategoryID": "cat-electronics",
			"CreatedAt":  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"ProductID":  "prod-003",
			"Name":       "Coffee Mug",
			"Price":      1200,
			"IsActive":   false,
			"CategoryID": "cat-kitchen",
			"CreatedAt":  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	
	return f.insertRecords(ctx, "Products", products)
}

// loadOrdersFixture loads order test data
func (f *FixtureHelper) loadOrdersFixture(ctx context.Context) error {
	orders := []map[string]interface{}{
		{
			"OrderID":   "order-001",
			"UserID":    "user-001",
			"ProductID": "prod-001",
			"Quantity":  1,
			"OrderDate": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"OrderID":   "order-002",
			"UserID":    "user-002",
			"ProductID": "prod-002",
			"Quantity":  2,
			"OrderDate": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"OrderID":   "order-003",
			"UserID":    "user-001",
			"ProductID": "prod-003",
			"Quantity":  1,
			"OrderDate": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	
	return f.insertRecords(ctx, "Orders", orders)
}

// insertRecords inserts multiple records into a table
func (f *FixtureHelper) insertRecords(ctx context.Context, table string, records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}
	
	// Build mutations for batch insert
	var mutations []*spanner.Mutation
	
	for _, record := range records {
		mutation := spanner.InsertMap(table, record)
		mutations = append(mutations, mutation)
	}
	
	// Execute batch insert
	_, err := f.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite(mutations)
	})
	
	return err
}