package validator

import (
	"encoding/base64"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/nu0ma/spalidate/internal/config"
	"github.com/nu0ma/spalidate/internal/spanner"
)

func TestValidationResult(t *testing.T) {
	result := &ValidationResult{}

	if result.HasErrors() {
		t.Error("New result should not have errors")
	}

	result.AddError("test error")
	if !result.HasErrors() {
		t.Error("Result should have errors after adding one")
	}

	if len(result.Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(result.Errors))
	}

	result.AddMessage("test message")
	if len(result.Messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(result.Messages))
	}
}

func TestCompareValues(t *testing.T) {
	v := &Validator{}

	tests := []struct {
		name     string
		expected any
		actual   any
		want     bool
	}{
		{
			name:     "equal strings",
			expected: "test",
			actual:   "test",
			want:     true,
		},
		{
			name:     "different strings",
			expected: "test1",
			actual:   "test2",
			want:     false,
		},
		{
			name:     "equal integers",
			expected: 42,
			actual:   42,
			want:     true,
		},
		{
			name:     "different integers",
			expected: 42,
			actual:   43,
			want:     false,
		},
		{
			name:     "equal booleans",
			expected: true,
			actual:   true,
			want:     true,
		},
		{
			name:     "different booleans",
			expected: true,
			actual:   false,
			want:     false,
		},
		{
			name:     "both nil",
			expected: nil,
			actual:   nil,
			want:     true,
		},
		{
			name:     "one nil",
			expected: nil,
			actual:   "test",
			want:     false,
		},
		{
			name:     "int types compatibility",
			expected: int(42),
			actual:   int64(42),
			want:     true,
		},
		{
			name:     "string representation match",
			expected: 42,
			actual:   "42",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := v.compareValues(tt.expected, tt.actual)
			if got != tt.want {
				t.Errorf("compareValues(%v, %v) = %v, want %v", tt.expected, tt.actual, got, tt.want)
			}
		})
	}
}

func TestValidateMultipleRowsByPrimaryKey(t *testing.T) {
	tests := []struct {
		name        string
		tableConfig config.TableConfig
		rows        []spanner.Row
		wantErrors  bool
		errorCount  int
	}{
		{
			name: "primary key based comparison - exact match",
			tableConfig: config.TableConfig{
				PrimaryKeyColumns: []string{"ID"},
				Rows: []map[string]interface{}{
					{"ID": "1", "Name": "Alice"},
					{"ID": "2", "Name": "Bob"},
				},
			},
			rows: []spanner.Row{
				{"ID": "2", "Name": "Bob"},
				{"ID": "1", "Name": "Alice"},
			},
			wantErrors: false,
		},
		{
			name: "primary key based comparison - missing row",
			tableConfig: config.TableConfig{
				PrimaryKeyColumns: []string{"ID"},
				Rows: []map[string]interface{}{
					{"ID": "1", "Name": "Alice"},
					{"ID": "2", "Name": "Bob"},
				},
			},
			rows: []spanner.Row{
				{"ID": "1", "Name": "Alice"},
			},
			wantErrors: true,
			errorCount: 1, // One missing row
		},
		{
			name: "primary key based comparison - unexpected row",
			tableConfig: config.TableConfig{
				PrimaryKeyColumns: []string{"ID"},
				Rows: []map[string]interface{}{
					{"ID": "1", "Name": "Alice"},
				},
			},
			rows: []spanner.Row{
				{"ID": "1", "Name": "Alice"},
				{"ID": "2", "Name": "Bob"},
			},
			wantErrors: true,
			errorCount: 1, // One unexpected row
		},
		{
			name: "primary key based comparison - value mismatch",
			tableConfig: config.TableConfig{
				PrimaryKeyColumns: []string{"ID"},
				Rows: []map[string]interface{}{
					{"ID": "1", "Name": "Alice"},
				},
			},
			rows: []spanner.Row{
				{"ID": "1", "Name": "Bob"}, // Wrong name
			},
			wantErrors: true,
			errorCount: 1, // One column mismatch
		},
		{
			name: "composite primary key",
			tableConfig: config.TableConfig{
				PrimaryKeyColumns: []string{"UserID", "OrderID"},
				Rows: []map[string]interface{}{
					{"UserID": "1", "OrderID": "A", "Amount": 100},
					{"UserID": "1", "OrderID": "B", "Amount": 200},
				},
			},
			rows: []spanner.Row{
				{"UserID": "1", "OrderID": "B", "Amount": 200},
				{"UserID": "1", "OrderID": "A", "Amount": 100},
			},
			wantErrors: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &Validator{}
			result := &ValidationResult{}

			err := v.validateMultipleRows("TestTable", tt.tableConfig, tt.rows, result)
			if err != nil {
				t.Errorf("validateMultipleRows() error = %v", err)
				return
			}

			hasErrors := result.HasErrors()
			if hasErrors != tt.wantErrors {
				t.Errorf("validateMultipleRows() hasErrors = %v, want %v, errors: %v", hasErrors, tt.wantErrors, result.Errors)
			}

			if tt.wantErrors && len(result.Errors) != tt.errorCount {
				t.Errorf("validateMultipleRows() error count = %d, want %d, errors: %v", len(result.Errors), tt.errorCount, result.Errors)
			}
		})
	}
}

func TestBuildPrimaryKey(t *testing.T) {
	v := &Validator{}

	tests := []struct {
		name        string
		row         map[string]interface{}
		primaryKeys []string
		expected    string
	}{
		{
			name:        "single column primary key",
			row:         map[string]interface{}{"ID": "123", "Name": "Alice"},
			primaryKeys: []string{"ID"},
			expected:    "123",
		},
		{
			name:        "composite primary key",
			row:         map[string]interface{}{"UserID": "1", "OrderID": "A", "Amount": 100},
			primaryKeys: []string{"UserID", "OrderID"},
			expected:    "1|A",
		},
		{
			name:        "missing primary key column",
			row:         map[string]interface{}{"Name": "Alice"},
			primaryKeys: []string{"ID"},
			expected:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := v.buildPrimaryKey(tt.row, tt.primaryKeys)
			if result != tt.expected {
				t.Errorf("buildPrimaryKey() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCompareBigRat(t *testing.T) {
	tests := []struct {
		name     string
		options  ComparisonOptions
		expected interface{}
		actual   interface{}
		want     bool
	}{
		{
			name:     "exact big.Rat match",
			options:  DefaultComparisonOptions(),
			expected: "123.456",
			actual:   mustParseBigRat("123.456"),
			want:     true,
		},
		{
			name:     "big.Rat with tolerance - within range",
			options:  ComparisonOptions{FloatTolerance: 0.01},
			expected: "123.45",
			actual:   mustParseBigRat("123.451"),
			want:     true,
		},
		{
			name:     "big.Rat with tolerance - outside range",
			options:  ComparisonOptions{FloatTolerance: 0.001},
			expected: "123.45",
			actual:   mustParseBigRat("123.46"),
			want:     false,
		},
		{
			name:     "int to big.Rat conversion",
			options:  DefaultComparisonOptions(),
			expected: 123,
			actual:   mustParseBigRat("123"),
			want:     true,
		},
		{
			name:     "float to big.Rat conversion",
			options:  DefaultComparisonOptions(),
			expected: 123.0,
			actual:   mustParseBigRat("123"),
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewWithOptions(nil, tt.options)
			result := v.compareBigRat(tt.expected, tt.actual)
			if result != tt.want {
				t.Errorf("compareBigRat() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestCompareTimestamp(t *testing.T) {
	baseTime := time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)

	tests := []struct {
		name     string
		options  ComparisonOptions
		expected interface{}
		actual   interface{}
		want     bool
	}{
		{
			name:     "exact timestamp match RFC3339Nano",
			options:  DefaultComparisonOptions(),
			expected: "2024-01-15T10:30:45.123456789Z",
			actual:   baseTime,
			want:     true,
		},
		{
			name:     "timestamp match RFC3339",
			options:  DefaultComparisonOptions(),
			expected: "2024-01-15T10:30:45Z",
			actual:   baseTime.Truncate(time.Second),
			want:     true,
		},
		{
			name:     "timestamp with truncation",
			options:  ComparisonOptions{TimestampTruncateTo: time.Second},
			expected: "2024-01-15T10:30:45Z",
			actual:   baseTime,
			want:     true,
		},
		{
			name:     "timestamp mismatch",
			options:  DefaultComparisonOptions(),
			expected: "2024-01-15T10:30:46Z",
			actual:   baseTime,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewWithOptions(nil, tt.options)
			result := v.compareTimestamp(tt.expected, tt.actual)
			if result != tt.want {
				t.Errorf("compareTimestamp() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestCompareBytes(t *testing.T) {
	testData := []byte("test data")
	testDataBase64 := base64.StdEncoding.EncodeToString(testData)

	tests := []struct {
		name     string
		expected interface{}
		actual   interface{}
		want     bool
	}{
		{
			name:     "exact bytes match",
			expected: testData,
			actual:   testData,
			want:     true,
		},
		{
			name:     "base64 string to bytes",
			expected: testDataBase64,
			actual:   testData,
			want:     true,
		},
		{
			name:     "empty bytes",
			expected: "",
			actual:   []byte{},
			want:     true,
		},
		{
			name:     "bytes mismatch",
			expected: "different",
			actual:   testData,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &Validator{}
			result := v.compareBytes(tt.expected, tt.actual)
			if result != tt.want {
				t.Errorf("compareBytes() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestCompareJSON(t *testing.T) {
	testMap := map[string]interface{}{
		"key":    "value",
		"number": float64(123),
		"bool":   true,
	}

	tests := []struct {
		name     string
		options  ComparisonOptions
		expected interface{}
		actual   interface{}
		want     bool
	}{
		{
			name:     "exact JSON match",
			options:  DefaultComparisonOptions(),
			expected: `{"key":"value","number":123,"bool":true}`,
			actual:   testMap,
			want:     true,
		},
		{
			name:     "JSON different key order - ignore order",
			options:  ComparisonOptions{IgnoreJSONKeyOrder: true},
			expected: `{"bool":true,"key":"value","number":123}`,
			actual:   testMap,
			want:     true,
		},
		{
			name:     "JSON different key order - strict order",
			options:  ComparisonOptions{IgnoreJSONKeyOrder: false},
			expected: `{"bool":true,"key":"value","number":123}`,
			actual:   testMap,
			want:     true, // Go map iteration order is not guaranteed, so DeepEqual is always used
		},
		{
			name:     "JSON map to map",
			options:  DefaultComparisonOptions(),
			expected: testMap,
			actual:   testMap,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewWithOptions(nil, tt.options)
			result := v.compareJSON(tt.expected, tt.actual)
			if result != tt.want {
				t.Errorf("compareJSON() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestCompareNumericConversions(t *testing.T) {
	tests := []struct {
		name     string
		options  ComparisonOptions
		expected interface{}
		actual   interface{}
		want     bool
	}{
		{
			name:     "int to int64",
			options:  DefaultComparisonOptions(),
			expected: 42,
			actual:   int64(42),
			want:     true,
		},
		{
			name:     "float with tolerance - within",
			options:  ComparisonOptions{FloatTolerance: 0.01},
			expected: 3.14,
			actual:   3.141,
			want:     true,
		},
		{
			name:     "float with tolerance - outside",
			options:  ComparisonOptions{FloatTolerance: 0.001},
			expected: 3.14,
			actual:   3.15,
			want:     false,
		},
		{
			name:     "exact float match",
			options:  DefaultComparisonOptions(),
			expected: 3.14159,
			actual:   3.14159,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewWithOptions(nil, tt.options)
			result := v.compareNumericConversions(tt.expected, tt.actual)
			if result != tt.want {
				t.Errorf("compareNumericConversions() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestCompareSlices(t *testing.T) {
	tests := []struct {
		name     string
		expected interface{}
		actual   interface{}
		want     bool
	}{
		{
			name:     "string slices match",
			expected: []string{"a", "b", "c"},
			actual:   []string{"a", "b", "c"},
			want:     true,
		},
		{
			name:     "int slices match",
			expected: []int{1, 2, 3},
			actual:   []int{1, 2, 3},
			want:     true,
		},
		{
			name:     "slices different lengths",
			expected: []string{"a", "b"},
			actual:   []string{"a", "b", "c"},
			want:     false,
		},
		{
			name:     "slices different content",
			expected: []string{"a", "b"},
			actual:   []string{"a", "c"},
			want:     false,
		},
		{
			name:     "empty slices",
			expected: []string{},
			actual:   []string{},
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &Validator{}
			result := v.compareSlices(tt.expected, tt.actual)
			if result != tt.want {
				t.Errorf("compareSlices() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestFormatValueForError(t *testing.T) {
	v := &Validator{}
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	testBytes := []byte("test")

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "nil value",
			value:    nil,
			expected: "null",
		},
		{
			name:     "string value",
			value:    "test",
			expected: `"test"`,
		},
		{
			name:     "long string",
			value:    strings.Repeat("a", 150),
			expected: strings.Repeat("a", 100) + "...",
		},
		{
			name:     "time value",
			value:    testTime,
			expected: "2024-01-15T10:30:00Z",
		},
		{
			name:     "bytes value",
			value:    testBytes,
			expected: base64.StdEncoding.EncodeToString(testBytes),
		},
		{
			name:     "big.Rat value",
			value:    mustParseBigRat("123.456"),
			expected: "123.456000",
		},
		{
			name:     "slice value",
			value:    []interface{}{"a", "b", "c"},
			expected: "[a, b, c]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := v.formatValueForError(tt.value)
			if result != tt.expected {
				t.Errorf("formatValueForError() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComparisonOptions(t *testing.T) {
	defaultOpts := DefaultComparisonOptions()

	if defaultOpts.FloatTolerance != 1e-9 {
		t.Errorf("Expected FloatTolerance 1e-9, got %v", defaultOpts.FloatTolerance)
	}

	if !defaultOpts.IgnoreJSONKeyOrder {
		t.Error("Expected IgnoreJSONKeyOrder to be true")
	}

	if defaultOpts.TimestampTruncateTo != 0 {
		t.Errorf("Expected TimestampTruncateTo 0, got %v", defaultOpts.TimestampTruncateTo)
	}

	if defaultOpts.AllowUnorderedRows {
		t.Error("Expected AllowUnorderedRows to be false")
	}
}

func TestCompareTimestampWithMultipleFormats(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		options  ComparisonOptions
		expected interface{}
		actual   interface{}
		want     bool
	}{
		{
			name:     "RFC3339 with Z timezone",
			options:  DefaultComparisonOptions(),
			expected: "2024-01-01T00:00:00Z",
			actual:   baseTime,
			want:     true,
		},
		{
			name:     "alternative format without T separator",
			options:  DefaultComparisonOptions(),
			expected: "2024-01-01 00:00:00",
			actual:   baseTime,
			want:     true,
		},
		{
			name:     "without timezone suffix",
			options:  DefaultComparisonOptions(),
			expected: "2024-01-01T00:00:00",
			actual:   baseTime,
			want:     true,
		},
		{
			name:     "date only format",
			options:  DefaultComparisonOptions(),
			expected: "2024-01-01",
			actual:   baseTime,
			want:     true,
		},
		{
			name:     "timestamp with nanoseconds",
			options:  DefaultComparisonOptions(),
			expected: "2024-01-01T00:00:00.123456789Z",
			actual:   time.Date(2024, 1, 1, 0, 0, 0, 123456789, time.UTC),
			want:     true,
		},
		{
			name:     "timestamp truncation to seconds",
			options:  ComparisonOptions{TimestampTruncateTo: time.Second},
			expected: "2024-01-01T00:00:00Z",
			actual:   time.Date(2024, 1, 1, 0, 0, 0, 999999999, time.UTC),
			want:     true,
		},
		{
			name:     "timestamp truncation to minutes",
			options:  ComparisonOptions{TimestampTruncateTo: time.Minute},
			expected: "2024-01-01T00:00:00Z",
			actual:   time.Date(2024, 1, 1, 0, 0, 59, 0, time.UTC),
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewWithOptions(nil, tt.options)
			result := v.compareTimestamp(tt.expected, tt.actual)
			if result != tt.want {
				t.Errorf("compareTimestamp() = %v, want %v", result, tt.want)
			}
		})
	}
}

func mustParseBigRat(s string) *big.Rat {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		panic("failed to parse big.Rat from: " + s)
	}
	return r
}

func TestNew(t *testing.T) {
	v := New(nil)
	if v == nil {
		t.Error("New() should return a non-nil validator")
		return
	}
	if v.client != nil {
		t.Error("Validator client should be nil when passed nil")
	}

	// Test default options
	defaultOpts := DefaultComparisonOptions()
	if v.options.FloatTolerance != defaultOpts.FloatTolerance {
		t.Errorf("Expected default FloatTolerance, got %v", v.options.FloatTolerance)
	}
}
