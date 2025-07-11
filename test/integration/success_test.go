//go:build integration
// +build integration

package integration

import (
	"testing"
)

func TestSuccessfulValidation(t *testing.T) {
	binaryPath := BuildSpalidate(t)
	
	config := DefaultTestConfig().
		WithVerbose().
		WithValidationFile(GetTestDataPath("success.yaml"))
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandSuccess(t, output, err)

	outputStr := string(output)
	AssertContains(t, outputStr, "✅ All validations passed!")

	// Check that all tables were validated
	expectedMessages := []string{
		"Table Users: row count matches",
		"Table Products: row count matches", 
		"Table Orders: row count matches",
	}

	for _, msg := range expectedMessages {
		AssertContains(t, outputStr, msg)
	}
}