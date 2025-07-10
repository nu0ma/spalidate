package validator

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/nu0ma/spalidate/internal/config"
	"github.com/nu0ma/spalidate/internal/spanner"
)

// ComparisonOptions defines options for value comparison
type ComparisonOptions struct {
	FloatTolerance      float64       // Tolerance for float comparisons
	TimestampTruncateTo time.Duration // Truncate timestamps to this precision
	IgnoreJSONKeyOrder  bool          // Ignore JSON key order when comparing
	AllowUnorderedRows  bool          // Allow unordered row comparison (deprecated - use PrimaryKeyColumns)
}

// DefaultComparisonOptions returns default comparison options
func DefaultComparisonOptions() ComparisonOptions {
	return ComparisonOptions{
		FloatTolerance:      1e-9,
		TimestampTruncateTo: 0, // No truncation by default
		IgnoreJSONKeyOrder:  true,
		AllowUnorderedRows:  false,
	}
}

type Validator struct {
	client  *spanner.Client
	options ComparisonOptions
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
		client:  client,
		options: DefaultComparisonOptions(),
	}
}

func NewWithOptions(client *spanner.Client, options ComparisonOptions) *Validator {
	return &Validator{
		client:  client,
		options: options,
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

	if tableConfig.Count > 0 && (len(tableConfig.Columns) > 0 || len(tableConfig.Rows) > 0) {
		if err := v.validateColumns(tableName, tableConfig, result); err != nil {
			return fmt.Errorf("failed to validate columns: %w", err)
		}
	}

	return nil
}

func (v *Validator) validateColumns(tableName string, tableConfig config.TableConfig, result *ValidationResult) error {
	columnNames := tableConfig.GetColumnNames()
	rows, err := v.client.QueryRowsWithOrder(tableName, columnNames, tableConfig.OrderBy)
	if err != nil {
		return fmt.Errorf("failed to query rows: %w", err)
	}

	if len(rows) == 0 {
		if tableConfig.Count > 0 {
			result.AddError(fmt.Sprintf("Table %s: expected data but no rows found", tableName))
		}
		return nil
	}

	// Handle multi-row validation
	if len(tableConfig.Rows) > 0 {
		return v.validateMultipleRows(tableName, tableConfig, rows, result)
	}

	// Legacy single-row validation
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

func (v *Validator) validateMultipleRows(tableName string, tableConfig config.TableConfig, rows []spanner.Row, result *ValidationResult) error {
	// If primary key columns are specified, use primary key-based comparison
	if len(tableConfig.PrimaryKeyColumns) > 0 {
		return v.validateRowsByPrimaryKey(tableName, tableConfig, rows, result)
	}

	// Fall back to order-based comparison (legacy behavior)
	return v.validateRowsByOrder(tableName, tableConfig, rows, result)
}

// validateRowsByPrimaryKey implements primary key-based row comparison
func (v *Validator) validateRowsByPrimaryKey(tableName string, tableConfig config.TableConfig, rows []spanner.Row, result *ValidationResult) error {
	// Build map of expected rows keyed by primary key
	expectedRowMap := make(map[string]map[string]interface{})
	for _, expectedRow := range tableConfig.Rows {
		primaryKey := v.buildPrimaryKey(expectedRow, tableConfig.PrimaryKeyColumns)
		if primaryKey == "" {
			result.AddError(fmt.Sprintf("Table %s: could not build primary key for expected row %v", tableName, expectedRow))
			continue
		}
		expectedRowMap[primaryKey] = expectedRow
	}

	// Build map of actual rows keyed by primary key
	actualRowMap := make(map[string]spanner.Row)
	for _, actualRow := range rows {
		primaryKey := v.buildPrimaryKey(actualRow, tableConfig.PrimaryKeyColumns)
		if primaryKey == "" {
			result.AddError(fmt.Sprintf("Table %s: could not build primary key for actual row %v", tableName, actualRow))
			continue
		}
		actualRowMap[primaryKey] = actualRow
	}

	// Find missing rows (in expected but not in actual)
	for primaryKey, expectedRow := range expectedRowMap {
		if _, exists := actualRowMap[primaryKey]; !exists {
			result.AddError(fmt.Sprintf("Table %s: missing row with primary key %s: %v", tableName, primaryKey, expectedRow))
		}
	}

	// Find unexpected rows (in actual but not in expected)
	for primaryKey, actualRow := range actualRowMap {
		if _, exists := expectedRowMap[primaryKey]; !exists {
			result.AddError(fmt.Sprintf("Table %s: unexpected row with primary key %s: %v", tableName, primaryKey, actualRow))
		}
	}

	// Compare matching rows
	for primaryKey, expectedRow := range expectedRowMap {
		if actualRow, exists := actualRowMap[primaryKey]; exists {
			v.compareRowValues(tableName, fmt.Sprintf("primary key %s", primaryKey), expectedRow, actualRow, result)
		}
	}

	return nil
}

// validateRowsByOrder implements the original order-based row comparison
func (v *Validator) validateRowsByOrder(tableName string, tableConfig config.TableConfig, rows []spanner.Row, result *ValidationResult) error {
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

// buildPrimaryKey creates a primary key string from the specified columns
func (v *Validator) buildPrimaryKey(row map[string]interface{}, primaryKeyColumns []string) string {
	var keyParts []string
	for _, columnName := range primaryKeyColumns {
		value, exists := row[columnName]
		if !exists {
			return "" // Cannot build primary key if any column is missing
		}
		keyParts = append(keyParts, fmt.Sprintf("%v", value))
	}
	return strings.Join(keyParts, "|")
}

// compareRowValues compares individual column values between expected and actual rows
func (v *Validator) compareRowValues(tableName, rowIdentifier string, expectedRow map[string]interface{}, actualRow spanner.Row, result *ValidationResult) {
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

func (v *Validator) compareValues(expected, actual interface{}) bool {
	if expected == nil && actual == nil {
		return true
	}
	if expected == nil || actual == nil {
		return false
	}

	expectedType := reflect.TypeOf(expected)
	actualType := reflect.TypeOf(actual)

	// Handle special Spanner types first
	switch actualType.String() {
	case "*big.Rat", "big.Rat":
		return v.compareBigRat(expected, actual)
	case "time.Time":
		return v.compareTimestamp(expected, actual)
	case "[]uint8":
		return v.compareBytes(expected, actual)
	case "map[string]interface{}", "map[string]interface {}":
		return v.compareJSON(expected, actual)
	case "string":
		// Check if this is a JSON string from Spanner
		if v.isJSONString(actual.(string)) {
			return v.compareJSON(expected, actual)
		}
	}

	// Handle arrays/slices
	if expectedType.Kind() == reflect.Slice && actualType.Kind() == reflect.Slice {
		return v.compareSlices(expected, actual)
	}

	// Handle numeric conversions with tolerance
	if v.isNumericType(expectedType) && v.isNumericType(actualType) {
		return v.compareNumericConversions(expected, actual)
	}

	// Handle int vs int64 specifically (common with Spanner)
	if v.isIntegerType(expectedType) && v.isIntegerType(actualType) {
		return v.compareIntegerConversions(expected, actual)
	}

	// Exact type match
	if expectedType == actualType {
		return reflect.DeepEqual(expected, actual)
	}

	// String representation comparison as fallback
	expectedStr := fmt.Sprintf("%v", expected)
	actualStr := fmt.Sprintf("%v", actual)
	return expectedStr == actualStr
}

// formatValueForError formats values for error messages with proper type-specific formatting
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
		if len(v) > 100 {
			return fmt.Sprintf("%.100s...", v)
		}
		return fmt.Sprintf("%q", v)
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
		if len(str) > 100 {
			return fmt.Sprintf("%.100s...", str)
		}
		return str
	}
}

// compareBigRat compares NUMERIC/DECIMAL values with optional tolerance
func (v *Validator) compareBigRat(expected, actual interface{}) bool {
	var actualRat *big.Rat
	if rat, ok := actual.(*big.Rat); ok {
		actualRat = rat
	} else if rat, ok := actual.(big.Rat); ok {
		actualRat = &rat
	} else {
		return false
	}

	var expectedRat *big.Rat
	switch exp := expected.(type) {
	case string:
		var ok bool
		expectedRat, ok = new(big.Rat).SetString(exp)
		if !ok {
			return false
		}
	case float64:
		expectedRat = new(big.Rat).SetFloat64(exp)
	case int64:
		expectedRat = new(big.Rat).SetInt64(exp)
	case int:
		expectedRat = new(big.Rat).SetInt64(int64(exp))
	case *big.Rat:
		expectedRat = exp
	case big.Rat:
		expectedRat = &exp
	default:
		expectedStr := fmt.Sprintf("%v", exp)
		var ok bool
		expectedRat, ok = new(big.Rat).SetString(expectedStr)
		if !ok {
			return false
		}
	}

	// Direct comparison first
	if actualRat.Cmp(expectedRat) == 0 {
		return true
	}

	// If tolerance is set, check difference
	if v.options.FloatTolerance > 0 {
		diff := new(big.Rat).Sub(actualRat, expectedRat)
		diff.Abs(diff)
		tolerance := new(big.Rat).SetFloat64(v.options.FloatTolerance)
		return diff.Cmp(tolerance) < 0
	}

	return false
}

// compareTimestamp compares timestamp values with optional truncation
func (v *Validator) compareTimestamp(expected, actual interface{}) bool {
	actualTime, ok := actual.(time.Time)
	if !ok {
		return false
	}

	expectedStr := fmt.Sprintf("%v", expected)

	// Try to parse expected as time
	var expectedTime time.Time
	var err error

	// Try various time formats
	timeFormats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range timeFormats {
		if expectedTime, err = time.Parse(format, expectedStr); err == nil {
			break
		}
	}

	if err != nil {
		return false
	}

	// Apply truncation if specified
	if v.options.TimestampTruncateTo > 0 {
		actualTime = actualTime.Truncate(v.options.TimestampTruncateTo)
		expectedTime = expectedTime.Truncate(v.options.TimestampTruncateTo)
	}

	return actualTime.Equal(expectedTime)
}

// compareBytes compares byte arrays, handling base64 encoding
func (v *Validator) compareBytes(expected, actual interface{}) bool {
	actualBytes, ok := actual.([]byte)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case []byte:
		return reflect.DeepEqual(actualBytes, exp)
	case string:
		if exp == "" && len(actualBytes) == 0 {
			return true
		}
		// Try to decode as base64
		if expectedBytes, err := base64.StdEncoding.DecodeString(exp); err == nil {
			return reflect.DeepEqual(actualBytes, expectedBytes)
		}
		// Compare base64 encoded strings
		actualStr := base64.StdEncoding.EncodeToString(actualBytes)
		return exp == actualStr
	default:
		return false
	}
}

// compareJSON compares JSON values with optional key order independence
func (v *Validator) compareJSON(expected, actual interface{}) bool {
	// Parse actual value to JSON
	var actualParsed interface{}
	switch a := actual.(type) {
	case string:
		// JSON string from Spanner
		if err := json.Unmarshal([]byte(a), &actualParsed); err != nil {
			return false
		}
	case map[string]interface{}:
		actualParsed = a
	case []interface{}:
		actualParsed = a
	default:
		return false
	}

	// Parse expected value to JSON
	var expectedParsed interface{}
	switch e := expected.(type) {
	case string:
		if err := json.Unmarshal([]byte(e), &expectedParsed); err != nil {
			return false
		}
	case map[string]interface{}:
		expectedParsed = e
	case []interface{}:
		expectedParsed = e
	default:
		// Try to convert to JSON string first
		expectedBytes, err := json.Marshal(expected)
		if err != nil {
			return false
		}
		if err := json.Unmarshal(expectedBytes, &expectedParsed); err != nil {
			return false
		}
	}

	// Handle null values
	if actualParsed == nil && expectedParsed == nil {
		return true
	}
	if actualParsed == nil || expectedParsed == nil {
		return false
	}

	// Use DeepEqual for comparison
	return reflect.DeepEqual(expectedParsed, actualParsed)
}

// compareSlices compares array/slice values recursively
func (v *Validator) compareSlices(expected, actual interface{}) bool {
	expectedSlice := reflect.ValueOf(expected)
	actualSlice := reflect.ValueOf(actual)

	if expectedSlice.Len() != actualSlice.Len() {
		return false
	}

	for i := 0; i < expectedSlice.Len(); i++ {
		if !v.compareValues(expectedSlice.Index(i).Interface(), actualSlice.Index(i).Interface()) {
			return false
		}
	}
	return true
}

// compareNumericConversions handles numeric type conversions with tolerance
func (v *Validator) compareNumericConversions(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	// Convert both to float64 for comparison
	var expectedFloat, actualFloat float64

	switch expectedVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		expectedFloat = float64(expectedVal.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		expectedFloat = float64(expectedVal.Uint())
	case reflect.Float32, reflect.Float64:
		expectedFloat = expectedVal.Float()
	default:
		return false
	}

	switch actualVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		actualFloat = float64(actualVal.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		actualFloat = float64(actualVal.Uint())
	case reflect.Float32, reflect.Float64:
		actualFloat = actualVal.Float()
	default:
		return false
	}

	// Use tolerance for float comparison
	if v.options.FloatTolerance > 0 {
		return math.Abs(expectedFloat-actualFloat) <= v.options.FloatTolerance
	}

	return expectedFloat == actualFloat
}

// isNumericType checks if a type is numeric
func (v *Validator) isNumericType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// isIntegerType checks if a type is an integer type
func (v *Validator) isIntegerType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

// compareIntegerConversions handles integer type conversions (int vs int64, etc.)
func (v *Validator) compareIntegerConversions(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	// Convert both to int64 for comparison
	var expectedInt, actualInt int64

	switch expectedVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		expectedInt = expectedVal.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		expectedInt = int64(expectedVal.Uint())
	default:
		return false
	}

	switch actualVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		actualInt = actualVal.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		actualInt = int64(actualVal.Uint())
	default:
		return false
	}

	return expectedInt == actualInt
}

// isJSONString checks if a string appears to be JSON
func (v *Validator) isJSONString(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return false
	}
	// Check if it starts with JSON object or array markers
	return (s[0] == '{' && s[len(s)-1] == '}') || (s[0] == '[' && s[len(s)-1] == ']') || s == "null" || s == "true" || s == "false" || (len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"')
}
