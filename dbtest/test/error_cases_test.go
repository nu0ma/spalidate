//go:build integration
// +build integration

package integration

import (
	"testing"
)

func TestNonExistentTable(t *testing.T) {
	binaryPath := BuildSpalidate(t)
	
	config := DefaultTestConfig().
		WithValidationFile(GetTestDataPath("nonexistent_table.yaml"))
	config.BinaryPath = binaryPath

	output, err := RunSpalidate(t, config)
	AssertCommandFailure(t, output, err)

	outputStr := string(output)
	AssertContains(t, outputStr, "does not exist")
}