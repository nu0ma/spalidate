package validator

import (
	"testing"
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

func TestNew(t *testing.T) {
	v := New(nil)
	if v == nil {
		t.Error("New() should return a non-nil validator")
		return
	}
	if v.client != nil {
		t.Error("Validator client should be nil when passed nil")
	}
}
