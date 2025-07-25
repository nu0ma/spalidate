package test

import (
	"testing"
)

func TestNullTypesValidation(t *testing.T) {
	testCases := []ValidationTestCase{
		{
			name:         "null types validation",
			configPath:   "expected/null_types.yaml",
			expectErrors: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runValidationTest(t, tc)
		})
	}
}

func TestArrayTypesValidation(t *testing.T) {
	testCases := []ValidationTestCase{
		{
			name:         "array types validation",
			configPath:   "expected/array_types.yaml",
			expectErrors: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runValidationTest(t, tc)
		})
	}
}

func TestComplexTypesValidation(t *testing.T) {
	testCases := []ValidationTestCase{
		{
			name:         "complex types validation",
			configPath:   "expected/complex_types.yaml",
			expectErrors: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runValidationTest(t, tc)
		})
	}
}

func TestAllSpannerTypesValidation(t *testing.T) {
	// Test that validates all new Spanner types together
	testCases := []ValidationTestCase{
		{
			name:         "null types validation",
			configPath:   "expected/null_types.yaml",
			expectErrors: false,
		},
		{
			name:         "array types validation", 
			configPath:   "expected/array_types.yaml",
			expectErrors: false,
		},
		{
			name:         "complex types validation",
			configPath:   "expected/complex_types.yaml",
			expectErrors: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runValidationTest(t, tc)
		})
	}
}