package validator

import (
	"fmt"
	"reflect"

	"github.com/nu0ma/spalidate/internal/config"
	"github.com/nu0ma/spalidate/internal/spanner"
)

type Validator struct {
	client *spanner.Client
}

type ValidationResult struct {
	Errors   []string
	Messages []string
}

func (r *ValidationResult) HasErrors() bool {
	return len(r.Errors) > 0
}

func (r *ValidationResult) AddError(err string) {
	r.Errors = append(r.Errors, err)
}

func (r *ValidationResult) AddMessage(msg string) {
	r.Messages = append(r.Messages, msg)
}

func New(client *spanner.Client) *Validator {
	return &Validator{
		client: client,
	}
}

func (v *Validator) Validate(cfg *config.Config) (*ValidationResult, error) {
	result := &ValidationResult{}

	for tableName, tableConfig := range cfg.Tables {
		if err := v.validateTable(tableName, tableConfig, result); err != nil {
			return nil, fmt.Errorf("validation failed for table %s: %w", tableName, err)
		}
	}

	return result, nil
}

func (v *Validator) validateTable(tableName string, tableConfig config.TableConfig, result *ValidationResult) error {
	exists, err := v.client.TableExists(tableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		result.AddError(fmt.Sprintf("Table %s does not exist", tableName))
		return nil
	}

	actualCount, err := v.client.CountRows(tableName)
	if err != nil {
		return fmt.Errorf("failed to count rows: %w", err)
	}

	if actualCount != tableConfig.Count {
		result.AddError(fmt.Sprintf("Table %s: expected %d rows, got %d", tableName, tableConfig.Count, actualCount))
	} else {
		result.AddMessage(fmt.Sprintf("Table %s: row count matches (%d)", tableName, actualCount))
	}

	if tableConfig.Count > 0 && len(tableConfig.Columns) > 0 {
		if err := v.validateColumns(tableName, tableConfig, result); err != nil {
			return fmt.Errorf("failed to validate columns: %w", err)
		}
	}

	return nil
}

func (v *Validator) validateColumns(tableName string, tableConfig config.TableConfig, result *ValidationResult) error {
	columnNames := tableConfig.GetColumnNames()
	rows, err := v.client.QueryRows(tableName, columnNames)
	if err != nil {
		return fmt.Errorf("failed to query rows: %w", err)
	}

	if len(rows) == 0 {
		if tableConfig.Count > 0 {
			result.AddError(fmt.Sprintf("Table %s: expected data but no rows found", tableName))
		}
		return nil
	}

	expectedRow := tableConfig.Columns
	actualRow := rows[0]

	for columnName, expectedValue := range expectedRow {
		actualValue, exists := actualRow[columnName]
		if !exists {
			result.AddError(fmt.Sprintf("Table %s: column %s not found", tableName, columnName))
			continue
		}

		if !v.compareValues(expectedValue, actualValue) {
			result.AddError(fmt.Sprintf("Table %s, column %s: expected %v (%T), got %v (%T)",
				tableName, columnName, expectedValue, expectedValue, actualValue, actualValue))
		} else {
			result.AddMessage(fmt.Sprintf("Table %s, column %s: value matches", tableName, columnName))
		}
	}

	return nil
}

func (v *Validator) compareValues(expected, actual interface{}) bool {
	if expected == nil && actual == nil {
		return true
	}
	if expected == nil || actual == nil {
		return false
	}

	expectedType := reflect.TypeOf(expected)
	actualType := reflect.TypeOf(actual)

	if expectedType == actualType {
		return reflect.DeepEqual(expected, actual)
	}

	expectedStr := fmt.Sprintf("%v", expected)
	actualStr := fmt.Sprintf("%v", actual)

	if expectedStr == actualStr {
		return true
	}

	switch expectedType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if actualType.Kind() >= reflect.Int && actualType.Kind() <= reflect.Int64 {
			return reflect.ValueOf(expected).Int() == reflect.ValueOf(actual).Int()
		}
	case reflect.Float32, reflect.Float64:
		if actualType.Kind() >= reflect.Float32 && actualType.Kind() <= reflect.Float64 {
			return reflect.ValueOf(expected).Float() == reflect.ValueOf(actual).Float()
		}
	case reflect.String:
		if actualType.Kind() == reflect.String {
			return reflect.ValueOf(expected).String() == reflect.ValueOf(actual).String()
		}
	case reflect.Bool:
		if actualType.Kind() == reflect.Bool {
			return reflect.ValueOf(expected).Bool() == reflect.ValueOf(actual).Bool()
		}
	}

	return false
}
