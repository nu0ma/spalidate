package validator

import (
	"reflect"
	"testing"
)

func TestCompareJSON(t *testing.T) {
	validator := &Validator{
		options: DefaultComparisonOptions(),
	}

	tests := []struct {
		name     string
		expected interface{}
		actual   interface{}
		want     bool
	}{
		{
			name:     "JSON string to object comparison",
			expected: `{"name": "John", "age": 30}`,
			actual:   `{"name": "John", "age": 30}`,
			want:     true,
		},
		{
			name:     "JSON string to object comparison with different order",
			expected: `{"name": "John", "age": 30}`,
			actual:   `{"age": 30, "name": "John"}`,
			want:     true,
		},
		{
			name:     "JSON object to string comparison",
			expected: map[string]interface{}{"name": "John", "age": float64(30)},
			actual:   `{"name": "John", "age": 30}`,
			want:     true,
		},
		{
			name:     "JSON array comparison",
			expected: `[1, 2, 3]`,
			actual:   `[1, 2, 3]`,
			want:     true,
		},
		{
			name:     "JSON array different order should fail",
			expected: `[1, 2, 3]`,
			actual:   `[3, 2, 1]`,
			want:     false,
		},
		{
			name:     "JSON null comparison",
			expected: `null`,
			actual:   `null`,
			want:     true,
		},
		{
			name:     "JSON boolean true",
			expected: `true`,
			actual:   `true`,
			want:     true,
		},
		{
			name:     "JSON boolean false",
			expected: `false`,
			actual:   `false`,
			want:     true,
		},
		{
			name:     "JSON string value",
			expected: `"hello"`,
			actual:   `"hello"`,
			want:     true,
		},
		{
			name:     "Nested JSON object",
			expected: `{"user": {"name": "John", "profile": {"age": 30}}}`,
			actual:   `{"user": {"profile": {"age": 30}, "name": "John"}}`,
			want:     true,
		},
		{
			name:     "JSON with empty object",
			expected: `{}`,
			actual:   `{}`,
			want:     true,
		},
		{
			name:     "JSON with empty array",
			expected: `[]`,
			actual:   `[]`,
			want:     true,
		},
		{
			name:     "Different JSON values",
			expected: `{"name": "John"}`,
			actual:   `{"name": "Jane"}`,
			want:     false,
		},
		{
			name:     "JSON number comparison",
			expected: `123`,
			actual:   `123`,
			want:     true,
		},
		{
			name:     "JSON number vs string",
			expected: `123`,
			actual:   `"123"`,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validator.compareJSON(tt.expected, tt.actual)
			if got != tt.want {
				t.Errorf("compareJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsJSONString(t *testing.T) {
	validator := &Validator{
		options: DefaultComparisonOptions(),
	}

	tests := []struct {
		name string
		str  string
		want bool
	}{
		{
			name: "JSON object",
			str:  `{"name": "John"}`,
			want: true,
		},
		{
			name: "JSON array",
			str:  `[1, 2, 3]`,
			want: true,
		},
		{
			name: "JSON null",
			str:  `null`,
			want: true,
		},
		{
			name: "JSON boolean true",
			str:  `true`,
			want: true,
		},
		{
			name: "JSON boolean false",
			str:  `false`,
			want: true,
		},
		{
			name: "JSON string",
			str:  `"hello"`,
			want: true,
		},
		{
			name: "Regular string",
			str:  `hello`,
			want: false,
		},
		{
			name: "Empty string",
			str:  ``,
			want: false,
		},
		{
			name: "Malformed JSON",
			str:  `{name: "John"}`,
			want: false, // Now properly validates JSON syntax
		},
		{
			name: "JSON with whitespace",
			str:  ` {"name": "John"} `,
			want: true,
		},
		{
			name: "Invalid JSON - missing quotes",
			str:  `{name: John}`,
			want: false,
		},
		{
			name: "Invalid JSON - trailing comma",
			str:  `{"name": "John",}`,
			want: false,
		},
		{
			name: "Invalid JSON - unmatched brackets",
			str:  `{"name": "John"`,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validator.isJSONString(tt.str)
			if got != tt.want {
				t.Errorf("isJSONString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompareValues_JSON(t *testing.T) {
	validator := &Validator{
		options: DefaultComparisonOptions(),
	}

	tests := []struct {
		name     string
		expected interface{}
		actual   interface{}
		want     bool
	}{
		{
			name:     "JSON string detected and compared",
			expected: `{"name": "John", "age": 30}`,
			actual:   `{"age": 30, "name": "John"}`,
			want:     true,
		},
		{
			name:     "JSON array detected and compared",
			expected: `[1, 2, 3]`,
			actual:   `[1, 2, 3]`,
			want:     true,
		},
		{
			name:     "Non-JSON string comparison",
			expected: "hello",
			actual:   "hello",
			want:     true,
		},
		{
			name:     "Mixed JSON and map comparison",
			expected: map[string]interface{}{"name": "John", "age": float64(30)},
			actual:   `{"name": "John", "age": 30}`,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validator.compareValues(tt.expected, tt.actual)
			if got != tt.want {
				t.Errorf("compareValues() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeJSONNumbers(t *testing.T) {
	validator := &Validator{
		options: DefaultComparisonOptions(),
	}

	tests := []struct {
		name string
		data interface{}
		want interface{}
	}{
		{
			name: "Float64 that is actually an integer",
			data: float64(30),
			want: int64(30),
		},
		{
			name: "Float64 with decimal",
			data: float64(30.5),
			want: float64(30.5),
		},
		{
			name: "Int converted to int64",
			data: int(30),
			want: int64(30),
		},
		{
			name: "Int32 converted to int64",
			data: int32(30),
			want: int64(30),
		},
		{
			name: "Map with mixed numbers",
			data: map[string]interface{}{
				"age":   float64(30),
				"score": float64(95.5),
				"rank":  int(1),
			},
			want: map[string]interface{}{
				"age":   int64(30),
				"score": float64(95.5),
				"rank":  int64(1),
			},
		},
		{
			name: "Array with mixed numbers",
			data: []interface{}{float64(1), float64(2.5), int(3)},
			want: []interface{}{int64(1), float64(2.5), int64(3)},
		},
		{
			name: "String unchanged",
			data: "hello",
			want: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validator.normalizeJSONNumbers(tt.data)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("normalizeJSONNumbers() = %v (type %T), want %v (type %T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestCompareJSON_NumberNormalization(t *testing.T) {
	validator := &Validator{
		options: DefaultComparisonOptions(),
	}

	tests := []struct {
		name     string
		expected interface{}
		actual   interface{}
		want     bool
	}{
		{
			name:     "Integer vs Float64 with same value",
			expected: `{"age": 30}`,
			actual:   `{"age": 30.0}`,
			want:     true,
		},
		{
			name:     "Different float values",
			expected: `{"score": 95.5}`,
			actual:   `{"score": 95.6}`,
			want:     false,
		},
		{
			name:     "Mixed types in array",
			expected: `[1, 2.5, 3]`,
			actual:   `[1.0, 2.5, 3.0]`,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validator.compareJSON(tt.expected, tt.actual)
			if got != tt.want {
				t.Errorf("compareJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}