package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Tables map[string]TableConfig `yaml:"tables"`
}

type TableConfig struct {
	Count   int                      `yaml:"count"`
	OrderBy string                   `yaml:"order_by,omitempty"`
	Columns map[string]interface{}   `yaml:"columns,omitempty"`
	Rows    []map[string]interface{} `yaml:"rows,omitempty"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &config, nil
}

func validateConfig(config *Config) error {
	if len(config.Tables) == 0 {
		return fmt.Errorf("no tables defined in config")
	}

	for tableName, tableConfig := range config.Tables {
		if tableName == "" {
			return fmt.Errorf("table name cannot be empty")
		}

		if tableConfig.Count < 0 {
			return fmt.Errorf("table %s: count cannot be negative", tableName)
		}

		// Allow count-only validation (no columns or rows defined)
		// This is valid for scenarios where we only want to check row count
	}

	return nil
}

func (c *Config) GetTableNames() []string {
	names := make([]string, 0, len(c.Tables))
	for name := range c.Tables {
		names = append(names, name)
	}
	return names
}

func (t *TableConfig) GetColumnNames() []string {
	nameSet := make(map[string]bool)
	
	if len(t.Columns) > 0 {
		for name := range t.Columns {
			nameSet[name] = true
		}
	} else if len(t.Rows) > 0 {
		// Collect column names from all rows, not just the first one
		for _, row := range t.Rows {
			for name := range row {
				nameSet[name] = true
			}
		}
	}
	
	// Convert map keys to slice
	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	return names
}
