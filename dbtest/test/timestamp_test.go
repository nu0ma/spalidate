//go:build integration
// +build integration

package integration

import (
	"testing"
)

func TestTimestampValidation(t *testing.T) {
	binaryPath := BuildSpalidate(t)
	
	config := DefaultTestConfig().
		WithVerbose().
		WithValidationFile(GetTestDataPath("timestamp.yaml"))
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandSuccess(t, output, err)

	outputStr := string(output)
	AssertContains(t, outputStr, "✅ All validations passed!")
	
	// Check that timestamp fields were validated
	AssertContains(t, outputStr, "CreatedAt: value matches")
}

func TestAdvancedTimestampValidation(t *testing.T) {
	binaryPath := BuildSpalidate(t)
	
	config := DefaultTestConfig().
		WithVerbose().
		WithValidationFile(GetTestDataPath("timestamp_advanced.yaml"))
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandSuccess(t, output, err)

	outputStr := string(output)
	AssertContains(t, outputStr, "✅ All validations passed!")
	
	// Check that timestamp fields were validated
	if !contains(outputStr, "CreatedAt: value matches") || !contains(outputStr, "OrderDate: value matches") {
		t.Errorf("Expected timestamp validation messages, got: %s", outputStr)
	}
}