package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	yamlContent := `
tables:
  Users:
    count: 1
    columns:
      UserID: "user-001"
      Name: "Test User"
      Email: "test@example.com"
      Status: 1
  Products:
    count: 2
    columns:
      ProductID: "prod-001"
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

	if usersTable.Count != 1 {
		t.Errorf("Expected Users count 1, got %d", usersTable.Count)
	}

	if usersTable.Columns["UserID"] != "user-001" {
		t.Errorf("Expected UserID 'user-001', got %v", usersTable.Columns["UserID"])
	}

	productsTable := config.Tables["Products"]
	if productsTable.Count != 2 {
		t.Errorf("Expected Products count 2, got %d", productsTable.Count)
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr bool
	}{
		{
			name: "valid config",
			yaml: `
tables:
  Users:
    count: 1
    columns:
      ID: "test"
`,
			wantErr: false,
		},
		{
			name: "empty tables",
			yaml: `tables: {}`,
			wantErr: true,
		},
		{
			name: "negative count",
			yaml: `
tables:
  Users:
    count: -1
    columns:
      ID: "test"
`,
			wantErr: true,
		},
		{
			name: "count > 0 but no columns",
			yaml: `
tables:
  Users:
    count: 1
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "test-validate-*.yaml")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpFile.Name())

			if _, err := tmpFile.WriteString(tt.yaml); err != nil {
				t.Fatal(err)
			}
			tmpFile.Close()

			_, err = LoadConfig(tmpFile.Name())
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetTableNames(t *testing.T) {
	config := &Config{
		Tables: map[string]TableConfig{
			"Users":    {},
			"Products": {},
		},
	}

	names := config.GetTableNames()
	if len(names) != 2 {
		t.Errorf("Expected 2 table names, got %d", len(names))
	}

	found := make(map[string]bool)
	for _, name := range names {
		found[name] = true
	}

	if !found["Users"] || !found["Products"] {
		t.Error("Missing expected table names")
	}
}
