package validator

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"strings"
	"time"
)

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