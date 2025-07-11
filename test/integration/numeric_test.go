//go:build integration
// +build integration

package integration

import (
	"testing"
)

func TestNumericToleranceComparison(t *testing.T) {
	binaryPath := BuildSpalidate(t)
	
	config := DefaultTestConfig().
		WithVerbose().
		WithValidationFile(GetTestDataPath("numeric_tolerance.yaml"))
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandSuccess(t, output, err)

	outputStr := string(output)
	AssertContains(t, outputStr, "✅ All validations passed!")
}