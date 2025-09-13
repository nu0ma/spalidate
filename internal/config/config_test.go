package config

import (
	"os"
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

	tmpFile, err := os.CreateTemp("", "test-config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(yamlContent); err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	config, err := LoadConfig(tmpFile.Name())
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

}
