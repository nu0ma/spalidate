//go:build integration
// +build integration

package integration

import (
	"testing"
)

func TestColumnValidationFailure(t *testing.T) {
	binaryPath := BuildSpalidate(t)
	
	config := DefaultTestConfig().
		WithValidationFile(GetTestDataPath("wrong_columns.yaml"))
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandFailure(t, output, err)

	outputStr := string(output)
	AssertContains(t, outputStr, "Validation failed:")
	
	// Check for column validation error messages
	if !contains(outputStr, "column") || !contains(outputStr, "expected") {
		t.Errorf("Expected column validation error in output: %s", outputStr)
	}
}