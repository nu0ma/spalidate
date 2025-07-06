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
	Count             int                      `yaml:"count"`
	OrderBy           string                   `yaml:"order_by,omitempty"`
	PrimaryKeyColumns []string                 `yaml:"primary_key_columns,omitempty"`
	Columns           map[string]interface{}   `yaml:"columns,omitempty"`
	Rows              []map[string]interface{} `yaml:"rows,omitempty"`
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

		if len(tableConfig.Columns) == 0 && len(tableConfig.Rows) == 0 && tableConfig.Count > 0 {
			return fmt.Errorf("table %s: expected %d rows but no columns or rows defined", tableName, tableConfig.Count)
		}
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
	var names []string
	if len(t.Columns) > 0 {
		names = make([]string, 0, len(t.Columns))
		for name := range t.Columns {
			names = append(names, name)
		}
	} else if len(t.Rows) > 0 {
		names = make([]string, 0, len(t.Rows[0]))
		for name := range t.Rows[0] {
			names = append(names, name)
		}
	}
	return names
}
