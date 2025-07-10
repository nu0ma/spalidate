package validator

import (
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
			want: true, // Still looks like JSON based on brackets
		},
		{
			name: "JSON with whitespace",
			str:  ` {"name": "John"} `,
			want: true,
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