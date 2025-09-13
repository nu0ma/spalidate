package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	yamlContent := `
tables:
  Users:
    columns:
      - UserID: "user-001"
        Name: "Test User"
        Email: "test@example.com"
        Status: 1
  Products:
    columns:
      - ProductID: "prod-001"
        Name: "Test Product"
        Price: 1000
        IsActive: true
`
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-config.yaml")

	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	config, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if len(config.Tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(config.Tables))
	}

	usersTable, exists := config.Tables["Users"]
	if !exists {
		t.Error("Users table not found")
	}

	if usersTable.Columns[0]["UserID"] != "user-001" {
		t.Errorf("Expected UserID 'user-001', got %v", usersTable.Columns[0]["UserID"])
	}

	if usersTable.Columns[0]["Name"] != "Test User" {
		t.Errorf("Expected Name 'Test User', got %v", usersTable.Columns[0]["Name"])
	}

	if usersTable.Columns[0]["Email"] != "test@example.com" {
		t.Errorf("Expected Email 'test@example.com', got %v", usersTable.Columns[0]["Email"])
	}

	if usersTable.Columns[0]["Status"] != 1 {
		t.Errorf("Expected Status 1, got %v", usersTable.Columns[0]["Status"])
	}

}
