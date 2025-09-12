package validator

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/nu0ma/spalidate/internal/config"
	"google.golang.org/protobuf/types/known/structpb"
)

type Validator struct {
	spannerClient *spannerClient
	options       ComparisonOptions
}

func NewValidator(project, instance, database string, port int) (*Validator, error) {
	client, err := newSpannerClient(project, instance, database, port)
	if err != nil {
		return nil, err
	}

	return &Validator{
		spannerClient: client,
		options:       DefaultComparisonOptions(),
	}, nil
}

func NewValidatorWithOptions(project, instance, database string, port int, options ComparisonOptions) (*Validator, error) {
	client, err := newSpannerClient(project, instance, database, port)
	if err != nil {
		return nil, err
	}

	return &Validator{
		spannerClient: client,
		options:       options,
	}, nil
}

func New(client *spanner.Client) *Validator {
	spannerClient := &spannerClient{
		client:   client,
		database: "",
	}
	return &Validator{
		spannerClient: spannerClient,
		options:       DefaultComparisonOptions(),
	}
}

func NewWithOptions(client *spanner.Client, options ComparisonOptions) *Validator {
	spannerClient := &spannerClient{
		client:   client,
		database: "",
	}
	return &Validator{
		spannerClient: spannerClient,
		options:       options,
	}
}

func (v *Validator) Close() {
	if v.spannerClient != nil && v.spannerClient.client != nil {
		v.spannerClient.client.Close()
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
	exists, err := v.spannerClient.tableExists(tableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		result.AddError(fmt.Sprintf("Table %s does not exist", tableName))
		return nil
	}

	actualCount, err := v.spannerClient.countRows(tableName)
	if err != nil {
		return fmt.Errorf("failed to count rows: %w", err)
	}

	if actualCount != tableConfig.Count {
		result.AddError(fmt.Sprintf("Table %s: expected %d rows, got %d", tableName, tableConfig.Count, actualCount))
	} else {
		result.AddMessage(fmt.Sprintf("Table %s: row count matches (%d)", tableName, actualCount))
	}

	if tableConfig.Count > 0 && (len(tableConfig.Columns) > 0 || len(tableConfig.Rows) > 0) {
		if err := v.validateColumns(tableName, tableConfig, result); err != nil {
			return fmt.Errorf("failed to validate columns: %w", err)
		}
	}

	return nil
}

func (v *Validator) validateColumns(tableName string, tableConfig config.TableConfig, result *ValidationResult) error {
	columnNames := tableConfig.GetColumnNames()
	rows, err := v.spannerClient.queryRowsWithOrder(tableName, columnNames, tableConfig.OrderBy)
	if err != nil {
		return fmt.Errorf("failed to query rows: %w", err)
	}

	if len(rows) == 0 {
		if tableConfig.Count > 0 {
			result.AddError(fmt.Sprintf("Table %s: expected data but no rows found", tableName))
		}
		return nil
	}

	if len(tableConfig.Rows) > 0 {
		return v.validateMultipleRows(tableName, tableConfig, rows, result)
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

func (v *Validator) validateMultipleRows(tableName string, tableConfig config.TableConfig, rows []Row, result *ValidationResult) error {
	if len(tableConfig.PrimaryKeyColumns) > 0 {
		return v.validateRowsByPrimaryKey(tableName, tableConfig, rows, result)
	}

	return v.validateRowsByOrder(tableName, tableConfig, rows, result)
}

func (v *Validator) validateRowsByPrimaryKey(tableName string, tableConfig config.TableConfig, rows []Row, result *ValidationResult) error {
	expectedRowMap := make(map[string]map[string]interface{})
	for _, expectedRow := range tableConfig.Rows {
		primaryKey := v.buildPrimaryKey(expectedRow, tableConfig.PrimaryKeyColumns)
		if primaryKey == "" {
			result.AddError(fmt.Sprintf("Table %s: could not build primary key for expected row %v", tableName, expectedRow))
			continue
		}
		expectedRowMap[primaryKey] = expectedRow
	}

	actualRowMap := make(map[string]Row)
	for _, actualRow := range rows {
		primaryKey := v.buildPrimaryKey(actualRow, tableConfig.PrimaryKeyColumns)
		if primaryKey == "" {
			result.AddError(fmt.Sprintf("Table %s: could not build primary key for actual row %v", tableName, actualRow))
			continue
		}
		actualRowMap[primaryKey] = actualRow
	}

	for primaryKey, expectedRow := range expectedRowMap {
		if _, exists := actualRowMap[primaryKey]; !exists {
			result.AddError(fmt.Sprintf("Table %s: missing row with primary key %s: %v", tableName, primaryKey, expectedRow))
		}
	}

	for primaryKey, actualRow := range actualRowMap {
		if _, exists := expectedRowMap[primaryKey]; !exists {
			result.AddError(fmt.Sprintf("Table %s: unexpected row with primary key %s: %v", tableName, primaryKey, actualRow))
		}
	}

	for primaryKey, expectedRow := range expectedRowMap {
		if actualRow, exists := actualRowMap[primaryKey]; exists {
			v.compareRowValues(tableName, fmt.Sprintf("primary key %s", primaryKey), expectedRow, actualRow, result)
		}
	}

	return nil
}

func (v *Validator) validateRowsByOrder(tableName string, tableConfig config.TableConfig, rows []Row, result *ValidationResult) error {
	for i, expectedRow := range tableConfig.Rows {
		if i >= len(rows) {
			result.AddError(fmt.Sprintf("Table %s: expected row %d but only %d rows found", tableName, i+1, len(rows)))
			continue
		}

		actualRow := rows[i]
		v.compareRowValues(tableName, fmt.Sprintf("row %d", i+1), expectedRow, actualRow, result)
	}

	return nil
}

func (v *Validator) buildPrimaryKey(row map[string]interface{}, primaryKeyColumns []string) string {
	var keyParts []string
	for _, columnName := range primaryKeyColumns {
		value, exists := row[columnName]
		if !exists {
			return ""
		}
		keyParts = append(keyParts, fmt.Sprintf("%v", value))
	}
	return strings.Join(keyParts, "|")
}

func (v *Validator) compareRowValues(tableName, rowIdentifier string, expectedRow map[string]interface{}, actualRow Row, result *ValidationResult) {
	for columnName, expectedValue := range expectedRow {
		actualValue, exists := actualRow[columnName]
		if !exists {
			result.AddError(fmt.Sprintf("Table %s %s: column %s not found", tableName, rowIdentifier, columnName))
			continue
		}

		if !v.compareValues(expectedValue, actualValue) {
			expectedStr := v.formatValueForError(expectedValue)
			actualStr := v.formatValueForError(actualValue)
			result.AddError(fmt.Sprintf("Table %s %s, column %s: expected %s (%T), got %s (%T)",
				tableName, rowIdentifier, columnName, expectedStr, expectedValue, actualStr, actualValue))
		} else {
			result.AddMessage(fmt.Sprintf("Table %s %s, column %s: value matches", tableName, rowIdentifier, columnName))
		}
	}
}

func (v *Validator) formatValueForError(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch v := value.(type) {
	case *big.Rat:
		return v.FloatString(6)
	case big.Rat:
		return v.FloatString(6)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	case string:
		if len(v) > MaxErrorMessageLength {
			return fmt.Sprintf("%.100s...", v)
		}
		return fmt.Sprintf("%q", v)
	case *structpb.Value:
		return fmt.Sprintf("%q", v.GetStringValue())
	case []interface{}:
		if len(v) > 10 {
			return fmt.Sprintf("[%d items]", len(v))
		}
		var parts []string
		for _, item := range v {
			parts = append(parts, fmt.Sprintf("%v", item))
		}
		return fmt.Sprintf("[%s]", strings.Join(parts, ", "))
	default:
		str := fmt.Sprintf("%v", value)
		if len(str) > MaxErrorMessageLength {
			return fmt.Sprintf("%.100s...", str)
		}
		return str
	}
}