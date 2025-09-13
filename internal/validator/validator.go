package validator

import (
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/nu0ma/spalidate/internal/config"
)

type Validator struct {
	spannerClient  *spannerClient
	options        ComparisonOptions
	typeRegistry   *TypeRegistry
	errorFormatter *ErrorFormatter
}

func NewValidator(project, instance, database string, port int) (*Validator, error) {
	return NewValidatorWithOptions(project, instance, database, port, DefaultComparisonOptions())
}

func NewValidatorWithOptions(project, instance, database string, port int, options ComparisonOptions) (*Validator, error) {
	client, err := newSpannerClient(project, instance, database, port)
	if err != nil {
		return nil, err
	}

	return &Validator{
		spannerClient:  client,
		options:        options,
		typeRegistry:   NewTypeRegistry(),
		errorFormatter: NewErrorFormatter(),
	}, nil
}

func New(client *spanner.Client) *Validator {
	return NewWithOptions(client, DefaultComparisonOptions())
}

func NewWithOptions(client *spanner.Client, options ComparisonOptions) *Validator {
	spannerClient := &spannerClient{
		client:   client,
		database: "",
	}
	return &Validator{
		spannerClient:  spannerClient,
		options:        options,
		typeRegistry:   NewTypeRegistry(),
		errorFormatter: NewErrorFormatter(),
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
		result.AddError(v.errorFormatter.TableNotFound(tableName))
		return nil
	}

	actualCount, err := v.spannerClient.countRows(tableName)
	if err != nil {
		return fmt.Errorf("failed to count rows: %w", err)
	}

	if actualCount != tableConfig.Count {
		result.AddError(v.errorFormatter.RowCountMismatch(tableName, tableConfig.Count, actualCount))
	} else {
		result.AddMessage(v.errorFormatter.RowCountMatch(tableName, actualCount))
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
			result.AddError(v.errorFormatter.NoRowsFound(tableName))
		}
		return nil
	}

	// Handle multi-row validation
	if len(tableConfig.Rows) > 0 {
		if len(tableConfig.PrimaryKeyColumns) > 0 {
			v.validateRowsByPrimaryKey(tableName, tableConfig.Rows, rows, tableConfig.PrimaryKeyColumns, result)
		} else {
			v.validateRowsByOrder(tableName, tableConfig.Rows, rows, result)
		}
		return nil
	}

	// Handle single-row validation (legacy)
	expectedRow := tableConfig.Columns
	actualRow := rows[0]
	v.compareRow(tableName, "", expectedRow, actualRow, result)

	return nil
}

// compareRow compares a single row's values
func (v *Validator) compareRow(tableName, rowIdentifier string, expectedRow map[string]interface{}, actualRow Row, result *ValidationResult) {
	for columnName, expectedValue := range expectedRow {
		actualValue, exists := actualRow[columnName]
		if !exists {
			result.AddError(v.errorFormatter.ColumnNotFound(tableName, rowIdentifier, columnName))
			continue
		}

		if !v.compareValues(expectedValue, actualValue) {
			result.AddError(v.errorFormatter.ValueMismatch(tableName, rowIdentifier, columnName, expectedValue, actualValue))
		} else {
			result.AddMessage(v.errorFormatter.ValueMatch(tableName, rowIdentifier, columnName))
		}
	}
}

// compareValues compares two values using the type registry
func (v *Validator) compareValues(expected, actual interface{}) bool {
	return v.typeRegistry.Compare(expected, actual, v.options)
}

// validateRowsByOrder compares rows in sequence
func (v *Validator) validateRowsByOrder(tableName string, expectedRows []map[string]interface{}, actualRows []Row, result *ValidationResult) {
	for i, expectedRow := range expectedRows {
		if i >= len(actualRows) {
			result.AddError(fmt.Sprintf("Table %s: expected row %d but only %d rows found", tableName, i+1, len(actualRows)))
			continue
		}
		
		actualRow := actualRows[i]
		rowIdentifier := fmt.Sprintf("row %d", i+1)
		v.compareRow(tableName, rowIdentifier, expectedRow, actualRow, result)
	}
}

// validateRowsByPrimaryKey matches rows by primary key values
func (v *Validator) validateRowsByPrimaryKey(tableName string, expectedRows []map[string]interface{}, actualRows []Row, primaryKeyColumns []string, result *ValidationResult) {
	// Build expected row map
	expectedRowMap := make(map[string]map[string]interface{})
	for _, expectedRow := range expectedRows {
		primaryKey := v.buildPrimaryKey(expectedRow, primaryKeyColumns)
		if primaryKey == "" {
			result.AddError(fmt.Sprintf("Table %s: could not build primary key for expected row %v", tableName, expectedRow))
			continue
		}
		expectedRowMap[primaryKey] = expectedRow
	}
	
	// Build actual row map
	actualRowMap := make(map[string]Row)
	for _, actualRow := range actualRows {
		primaryKey := v.buildPrimaryKey(actualRow, primaryKeyColumns)
		if primaryKey == "" {
			result.AddError(fmt.Sprintf("Table %s: could not build primary key for actual row %v", tableName, actualRow))
			continue
		}
		actualRowMap[primaryKey] = actualRow
	}
	
	// Check for missing rows
	for primaryKey, expectedRow := range expectedRowMap {
		if _, exists := actualRowMap[primaryKey]; !exists {
			result.AddError(fmt.Sprintf("Table %s: missing row with primary key %s: %v", tableName, primaryKey, expectedRow))
		}
	}
	
	// Check for unexpected rows
	for primaryKey, actualRow := range actualRowMap {
		if _, exists := expectedRowMap[primaryKey]; !exists {
			result.AddError(fmt.Sprintf("Table %s: unexpected row with primary key %s: %v", tableName, primaryKey, actualRow))
		}
	}
	
	// Compare matching rows
	for primaryKey, expectedRow := range expectedRowMap {
		if actualRow, exists := actualRowMap[primaryKey]; exists {
			rowIdentifier := fmt.Sprintf("primary key %s", primaryKey)
			v.compareRow(tableName, rowIdentifier, expectedRow, actualRow, result)
		}
	}
}

// buildPrimaryKey creates a composite key from primary key columns
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

// Types and options from types.go

const (
	DefaultFloatTolerance = 1e-9
	DefaultPort          = 9010
	MaxErrorMessageLength = 100
)

type Row map[string]interface{}

type ComparisonOptions struct {
	FloatTolerance      float64
	TimestampTruncateTo time.Duration
	IgnoreJSONKeyOrder  bool
	AllowUnorderedRows  bool
}

func DefaultComparisonOptions() ComparisonOptions {
	return ComparisonOptions{
		FloatTolerance:      DefaultFloatTolerance,
		TimestampTruncateTo: 0,
		IgnoreJSONKeyOrder:  true,
		AllowUnorderedRows:  false,
	}
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

// Error formatting methods from error_formatter.go

// ErrorFormatter provides consistent error message formatting
type ErrorFormatter struct {
	typeRegistry *TypeRegistry
}

// NewErrorFormatter creates a new error formatter
func NewErrorFormatter() *ErrorFormatter {
	return &ErrorFormatter{
		typeRegistry: NewTypeRegistry(),
	}
}

// TableNotFound formats table not found error
func (f *ErrorFormatter) TableNotFound(tableName string) string {
	return fmt.Sprintf("Table %s does not exist", tableName)
}

// RowCountMismatch formats row count mismatch error
func (f *ErrorFormatter) RowCountMismatch(tableName string, expected, actual int) string {
	return fmt.Sprintf("Table %s: expected %d rows, got %d", tableName, expected, actual)
}

// RowCountMatch formats row count match message
func (f *ErrorFormatter) RowCountMatch(tableName string, count int) string {
	return fmt.Sprintf("Table %s: row count matches (%d)", tableName, count)
}

// NoRowsFound formats no rows found error
func (f *ErrorFormatter) NoRowsFound(tableName string) string {
	return fmt.Sprintf("Table %s: expected data but no rows found", tableName)
}

// ColumnNotFound formats column not found error
func (f *ErrorFormatter) ColumnNotFound(tableName, rowIdentifier, columnName string) string {
	if rowIdentifier == "" {
		return fmt.Sprintf("Table %s: column %s not found", tableName, columnName)
	}
	return fmt.Sprintf("Table %s %s: column %s not found", tableName, rowIdentifier, columnName)
}

// ValueMismatch formats value mismatch error
func (f *ErrorFormatter) ValueMismatch(tableName, rowIdentifier, columnName string, expected, actual interface{}) string {
	expectedStr := f.typeRegistry.Format(expected)
	actualStr := f.typeRegistry.Format(actual)
	
	if rowIdentifier == "" {
		return fmt.Sprintf("Table %s, column %s: expected %s (%T), got %s (%T)",
			tableName, columnName, expectedStr, expected, actualStr, actual)
	}
	return fmt.Sprintf("Table %s %s, column %s: expected %s (%T), got %s (%T)",
		tableName, rowIdentifier, columnName, expectedStr, expected, actualStr, actual)
}

// ValueMatch formats value match message
func (f *ErrorFormatter) ValueMatch(tableName, rowIdentifier, columnName string) string {
	if rowIdentifier == "" {
		return fmt.Sprintf("Table %s, column %s: value matches", tableName, columnName)
	}
	return fmt.Sprintf("Table %s %s, column %s: value matches", tableName, rowIdentifier, columnName)
}

// MissingRow formats missing row error
func (f *ErrorFormatter) MissingRow(tableName string, rowNumber int, totalRows int) string {
	return fmt.Sprintf("Table %s: expected row %d but only %d rows found", tableName, rowNumber, totalRows)
}

// BuildPrimaryKeyError formats primary key build error
func (f *ErrorFormatter) BuildPrimaryKeyError(tableName string, row map[string]interface{}, isExpected bool) string {
	rowType := "actual"
	if isExpected {
		rowType = "expected"
	}
	return fmt.Sprintf("Table %s: could not build primary key for %s row %v", tableName, rowType, row)
}

// MissingRowWithKey formats missing row with primary key error
func (f *ErrorFormatter) MissingRowWithKey(tableName, primaryKey string, row map[string]interface{}) string {
	return fmt.Sprintf("Table %s: missing row with primary key %s: %v", tableName, primaryKey, row)
}

// UnexpectedRowWithKey formats unexpected row with primary key error
func (f *ErrorFormatter) UnexpectedRowWithKey(tableName, primaryKey string, row Row) string {
	return fmt.Sprintf("Table %s: unexpected row with primary key %s: %v", tableName, primaryKey, row)
}

// Test helper methods from test_helpers.go

// validateMultipleRows is exposed for testing
func (v *Validator) validateMultipleRows(tableName string, tableConfig config.TableConfig, rows []Row, result *ValidationResult) error {
	if len(tableConfig.PrimaryKeyColumns) > 0 {
		v.validateRowsByPrimaryKey(tableName, tableConfig.Rows, rows, tableConfig.PrimaryKeyColumns, result)
	} else {
		v.validateRowsByOrder(tableName, tableConfig.Rows, rows, result)
	}
	return nil
}

// compareNumericConversions is exposed for testing
func (v *Validator) compareNumericConversions(expected, actual interface{}) bool {
	return compareNumericValues(expected, actual, v.options.FloatTolerance)
}

// compareSlices is exposed for testing
func (v *Validator) compareSlices(expected, actual interface{}) bool {
	handler := &SliceHandler{}
	return handler.Compare(expected, actual, v.options)
}

// formatValueForError is exposed for testing
func (v *Validator) formatValueForError(value interface{}) string {
	return v.typeRegistry.Format(value)
}
