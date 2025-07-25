//go:build integration
// +build integration

package integration

import (
	"testing"
)

func TestNullTypesValidation(t *testing.T) {
	// Build spalidate binary for testing
	binaryPath := BuildSpalidate(t)

	// Test null types validation
	config := DefaultTestConfig().
		WithValidationFile(GetTestDataPath("null_types.yaml")).
		WithVerbose()
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandSuccess(t, output, err)

	// Verify successful validation output
	AssertContains(t, string(output), "All validations passed!")
	AssertContains(t, string(output), "NullTypes")
}

func TestArrayTypesValidation(t *testing.T) {
	// Build spalidate binary for testing
	binaryPath := BuildSpalidate(t)

	// Test array types validation
	config := DefaultTestConfig().
		WithValidationFile(GetTestDataPath("array_types.yaml")).
		WithVerbose()
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandSuccess(t, output, err)

	// Verify successful validation output
	AssertContains(t, string(output), "All validations passed!")
	AssertContains(t, string(output), "ArrayTypes")
}

func TestComplexTypesValidation(t *testing.T) {
	// Build spalidate binary for testing
	binaryPath := BuildSpalidate(t)

	// Test complex types validation
	config := DefaultTestConfig().
		WithValidationFile(GetTestDataPath("complex_types.yaml")).
		WithVerbose()
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandSuccess(t, output, err)

	// Verify successful validation output
	AssertContains(t, string(output), "All validations passed!")
	AssertContains(t, string(output), "ComplexTypes")
}

func TestAllSpannerTypesValidation(t *testing.T) {
	// Build spalidate binary for testing
	binaryPath := BuildSpalidate(t)

	// Test all new Spanner types together
	testCases := []struct {
		name           string
		validationFile string
		expectedTable  string
	}{
		{
			name:           "null types validation",
			validationFile: "null_types.yaml",
			expectedTable:  "NullTypes",
		},
		{
			name:           "array types validation",
			validationFile: "array_types.yaml",
			expectedTable:  "ArrayTypes",
		},
		{
			name:           "complex types validation",
			validationFile: "complex_types.yaml",
			expectedTable:  "ComplexTypes",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultTestConfig().
				WithValidationFile(GetTestDataPath(tc.validationFile)).
				WithVerbose()
			config.BinaryPath = binaryPath

			output, err := RunSpalidate(t, config)
			AssertCommandSuccess(t, output, err)

			// Verify successful validation output
			AssertContains(t, string(output), "All validations passed!")
			AssertContains(t, string(output), tc.expectedTable)
		})
	}
}