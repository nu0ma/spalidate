//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	spannerEmulatorHost = "localhost:9010"
)

func loadSchemaStatements() ([]string, error) {
	schemaPath := filepath.Join("..", "fixtures", "schema.sql")
	content, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	// Remove comments first
	lines := strings.Split(string(content), "\n")
	var cleanLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "--") {
			cleanLines = append(cleanLines, line)
		}
	}
	cleanContent := strings.Join(cleanLines, "\n")

	// Split by semicolon to get statements
	statements := strings.Split(cleanContent, ";")
	var result []string
	for _, stmt := range statements {
		trimmed := strings.TrimSpace(stmt)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	
	return result, nil
}

func TestMain(m *testing.M) {
	// Set environment variable for Spanner emulator
	os.Setenv("SPANNER_EMULATOR_HOST", spannerEmulatorHost)

	// Wait for Spanner emulator to be ready
	if err := waitForSpannerEmulator(); err != nil {
		panic("Spanner emulator is not ready: " + err.Error())
	}

	// Create instance and database
	if err := setupSpannerInstance(); err != nil {
		panic("Failed to setup Spanner instance: " + err.Error())
	}


	// Run tests
	code := m.Run()
	os.Exit(code)
}

func waitForSpannerEmulator() error {
	// Try to connect to the emulator for up to 30 seconds
	for i := 0; i < 30; i++ {
		cmd := exec.Command("nc", "-z", "localhost", "9010")
		if err := cmd.Run(); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return exec.Command("nc", "-z", "localhost", "9010").Run()
}

func setupSpannerInstance() error {
	ctx := context.Background()

	// Set up clients for emulator
	opts := []option.ClientOption{
		option.WithEndpoint(spannerEmulatorHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	// Create instance admin client
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to create instance admin client: %w", err)
	}
	defer instanceAdminClient.Close()

	// Create instance
	instanceReq := &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + testProject,
		InstanceId: testInstance,
		Instance: &instancepb.Instance{
			DisplayName: "Test Instance",
			Config:      "projects/" + testProject + "/instanceConfigs/emulator-config",
			NodeCount:   1,
		},
	}

	instanceOp, err := instanceAdminClient.CreateInstance(ctx, instanceReq)
	if err != nil {
		// Instance might already exist, continue
		fmt.Printf("Instance creation failed (may already exist): %v\n", err)
	} else {
		_, err = instanceOp.Wait(ctx)
		if err != nil {
			fmt.Printf("Failed to wait for instance creation: %v\n", err)
		}
	}

	// Create database admin client
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to create database admin client: %w", err)
	}
	defer databaseAdminClient.Close()

	// Load schema statements from file
	schemaStatements, err := loadSchemaStatements()
	if err != nil {
		return fmt.Errorf("failed to load schema statements: %w", err)
	}

	// Create database
	databaseReq := &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", testDatabase),
		ExtraStatements: schemaStatements,
	}

	databaseOp, err := databaseAdminClient.CreateDatabase(ctx, databaseReq)
	if err != nil {
		// Database might already exist, continue
		fmt.Printf("Database creation failed (may already exist): %v\n", err)
		return nil
	}

	_, err = databaseOp.Wait(ctx)
	if err != nil {
		fmt.Printf("Failed to wait for database creation: %v\n", err)
	}

	return nil
}