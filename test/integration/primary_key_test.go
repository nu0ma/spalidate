//go:build integration
// +build integration

package integration

import (
	"testing"
)

func TestPrimaryKeyBasedComparison(t *testing.T) {
	binaryPath := BuildSpalidate(t)
	
	config := DefaultTestConfig().
		WithVerbose().
		WithValidationFile(GetTestDataPath("primary_key.yaml"))
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandSuccess(t, output, err)

	outputStr := string(output)
	AssertContains(t, outputStr, "✅ All validations passed!")
}

func TestCompositePrimaryKey(t *testing.T) {
	binaryPath := BuildSpalidate(t)
	
	config := DefaultTestConfig().
		WithVerbose().
		WithValidationFile(GetTestDataPath("composite_key.yaml"))
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandSuccess(t, output, err)

	outputStr := string(output)
	AssertContains(t, outputStr, "✅ All validations passed!")
}