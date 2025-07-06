package validator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/nu0ma/spalidate/internal/config"
	spannerClient "github.com/nu0ma/spalidate/internal/spanner"
)

type Validator struct {
	client *spannerClient.Client
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

func New(client *spannerClient.Client) *Validator {
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
			// For debugging: show actual value details
			actualStr := fmt.Sprintf("%v", actualValue)
			if actualType := reflect.TypeOf(actualValue); actualType != nil {
				switch actualType.String() {
				case "big.Rat":
					if rat, ok := actualValue.(*big.Rat); ok {
						actualStr = rat.FloatString(6)
					}
				case "time.Time":
					if t, ok := actualValue.(time.Time); ok {
						actualStr = t.Format(time.RFC3339Nano)
					}
				}
			}
			result.AddError(fmt.Sprintf("Table %s, column %s: expected %v (%T), got %s (%T)",
				tableName, columnName, expectedValue, expectedValue, actualStr, actualValue))
		} else {
			result.AddMessage(fmt.Sprintf("Table %s, column %s: value matches", tableName, columnName))
		}
	}

	return nil
}

func (v *Validator) validateMultipleRows(tableName string, tableConfig config.TableConfig, rows []spannerClient.Row, result *ValidationResult) error {
	for i, expectedRow := range tableConfig.Rows {
		if i >= len(rows) {
			result.AddError(fmt.Sprintf("Table %s: expected row %d but only %d rows found", tableName, i+1, len(rows)))
			continue
		}

		actualRow := rows[i]
		for columnName, expectedValue := range expectedRow {
			actualValue, exists := actualRow[columnName]
			if !exists {
				result.AddError(fmt.Sprintf("Table %s row %d: column %s not found", tableName, i+1, columnName))
				continue
			}

			if !v.compareValues(expectedValue, actualValue) {
				// For debugging: show actual value details
				actualStr := fmt.Sprintf("%v", actualValue)
				if actualType := reflect.TypeOf(actualValue); actualType != nil {
					switch actualType.String() {
					case "big.Rat":
						if rat, ok := actualValue.(*big.Rat); ok {
							actualStr = rat.FloatString(6)
						}
					case "time.Time":
						if t, ok := actualValue.(time.Time); ok {
							actualStr = t.Format(time.RFC3339Nano)
						}
					case "[]uint8":
						if bytes, ok := actualValue.([]byte); ok {
							actualStr = base64.StdEncoding.EncodeToString(bytes)
						}
					}
				}
				result.AddError(fmt.Sprintf("Table %s row %d, column %s: expected %v (%T), got %s (%T)",
					tableName, i+1, columnName, expectedValue, expectedValue, actualStr, actualValue))
			} else {
				result.AddMessage(fmt.Sprintf("Table %s row %d, column %s: value matches", tableName, i+1, columnName))
			}
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

	// Handle special Spanner types
	switch actualType.String() {
	case "big.Rat":
		// NUMERIC type - handle both pointer and value types
		var actualRat *big.Rat
		if rat, ok := actual.(*big.Rat); ok {
			actualRat = rat
		} else if rat, ok := actual.(big.Rat); ok {
			actualRat = &rat
		} else {
			return false
		}
		
		// Create expected Rat from various input types
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
		default:
			// Try converting to string then to Rat
			expectedStr := fmt.Sprintf("%v", exp)
			var ok bool
			expectedRat, ok = new(big.Rat).SetString(expectedStr)
			if !ok {
				return false
			}
		}
		
		// Compare the Rat values directly
		return actualRat.Cmp(expectedRat) == 0
	case "time.Time":
		// TIMESTAMP type
		actualTime, ok := actual.(time.Time)
		if !ok {
			return false
		}
		expectedStr := fmt.Sprintf("%v", expected)
		
		// Try to parse expected as time first
		if parsedTime, err := time.Parse(time.RFC3339Nano, expectedStr); err == nil {
			return actualTime.Equal(parsedTime)
		}
		if parsedTime, err := time.Parse(time.RFC3339, expectedStr); err == nil {
			return actualTime.Equal(parsedTime)
		}
		
		// Fallback to string comparison with various formats
		actualStr := actualTime.Format(time.RFC3339Nano)
		if expectedStr == actualStr {
			return true
		}
		actualStr = actualTime.Format(time.RFC3339)
		return expectedStr == actualStr
	case "[]uint8":
		// BYTES type
		actualBytes, ok := actual.([]byte)
		if !ok {
			return false
		}
		expectedStr := fmt.Sprintf("%v", expected)
		
		// Try to decode expected as base64 first
		if expectedBytes, err := base64.StdEncoding.DecodeString(expectedStr); err == nil {
			return bytes.Equal(actualBytes, expectedBytes)
		}
		
		// Fallback to base64 encoding comparison
		actualStr := base64.StdEncoding.EncodeToString(actualBytes)
		return expectedStr == actualStr
	case "map[string]interface {}":
		// JSON type
		actualMap, ok := actual.(map[string]interface{})
		if !ok {
			return false
		}
		expectedStr := fmt.Sprintf("%v", expected)
		
		// Try to parse expected as JSON first
		var expectedMap map[string]interface{}
		if err := json.Unmarshal([]byte(expectedStr), &expectedMap); err == nil {
			return reflect.DeepEqual(actualMap, expectedMap)
		}
		
		// Fallback to marshaling actual and comparing strings
		actualJSON, err := json.Marshal(actualMap)
		if err != nil {
			return false
		}
		return expectedStr == string(actualJSON)
	}

	// Handle arrays/slices
	if expectedType.Kind() == reflect.Slice && actualType.Kind() == reflect.Slice {
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

	if expectedType == actualType {
		return reflect.DeepEqual(expected, actual)
	}

	// Try string representation comparison
	expectedStr := fmt.Sprintf("%v", expected)
	actualStr := fmt.Sprintf("%v", actual)

	if expectedStr == actualStr {
		return true
	}

	// Handle numeric type conversions
	switch expectedType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if actualType.Kind() >= reflect.Int && actualType.Kind() <= reflect.Int64 {
			return reflect.ValueOf(expected).Int() == reflect.ValueOf(actual).Int()
		}
		// Try to parse actual as int if it's a string
		if actualType.Kind() == reflect.String {
			if actualInt, err := strconv.ParseInt(actualStr, 10, 64); err == nil {
				return reflect.ValueOf(expected).Int() == actualInt
			}
		}
	case reflect.Float32, reflect.Float64:
		if actualType.Kind() >= reflect.Float32 && actualType.Kind() <= reflect.Float64 {
			return reflect.ValueOf(expected).Float() == reflect.ValueOf(actual).Float()
		}
		// Try to parse actual as float if it's a string
		if actualType.Kind() == reflect.String {
			if actualFloat, err := strconv.ParseFloat(actualStr, 64); err == nil {
				return reflect.ValueOf(expected).Float() == actualFloat
			}
		}
	case reflect.String:
		if actualType.Kind() == reflect.String {
			return reflect.ValueOf(expected).String() == reflect.ValueOf(actual).String()
		}
		// Try to convert actual to string for comparison
		return expectedStr == actualStr
	case reflect.Bool:
		if actualType.Kind() == reflect.Bool {
			return reflect.ValueOf(expected).Bool() == reflect.ValueOf(actual).Bool()
		}
	}

	return false
}
