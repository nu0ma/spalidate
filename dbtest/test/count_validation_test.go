//go:build integration
// +build integration

package integration

import (
	"testing"
)

func TestCountValidationFailure(t *testing.T) {
	binaryPath := BuildSpalidate(t)
	
	config := DefaultTestConfig().
		WithValidationFile(GetTestDataPath("wrong_count.yaml"))
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandFailure(t, output, err)

	outputStr := string(output)
	AssertContains(t, outputStr, "Validation failed:")
	AssertContains(t, outputStr, "expected 5 rows, got 3")
}