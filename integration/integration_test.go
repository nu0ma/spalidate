//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/nu0ma/spalidate/internal/config"
	"github.com/nu0ma/spalidate/internal/validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

const (
	testProject  = "test-project"
	testInstance = "test-instance"
	testDatabase = "test-database"
)

type testEnv struct {
	ctx            context.Context
	instanceAdmin  *instance.InstanceAdminClient
	databaseAdmin  *database.DatabaseAdminClient
	spannerClient  *spanner.Client
	databaseName   string
}

func setupTestEnvironment(t *testing.T) *testEnv {
	ctx := context.Background()
	
	// Get emulator host from environment
	emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST")
	if emulatorHost == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set, skipping integration tests")
	}

	// Create gRPC connection
	conn, err := grpc.Dial(emulatorHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	// Create admin clients
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx, option.WithGRPCConn(conn))
	require.NoError(t, err)

	databaseAdmin, err := database.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	require.NoError(t, err)

	// Create instance
	instanceName := fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance)
	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", testProject),
		InstanceId: testInstance,
		Instance: &instancepb.Instance{
			Name:        instanceName,
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", testProject),
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})
	require.NoError(t, err)
	
	_, err = op.Wait(ctx)
	require.NoError(t, err)

	// Create database with schema
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProject, testInstance, testDatabase)
	dbOp, err := databaseAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instanceName,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", testDatabase),
		ExtraStatements: []string{
			`CREATE TABLE users (
				id INT64 NOT NULL,
				name STRING(100),
				email STRING(100),
				active BOOL,
				score FLOAT64,
				created_at TIMESTAMP
			) PRIMARY KEY (id)`,
			`CREATE TABLE products (
				id INT64 NOT NULL,
				name STRING(100),
				price FLOAT64,
				stock INT64
			) PRIMARY KEY (id)`,
		},
	})
	require.NoError(t, err)
	
	_, err = dbOp.Wait(ctx)
	require.NoError(t, err)

	// Create Spanner client
	spannerClient, err := spanner.NewClient(ctx, databaseName, option.WithGRPCConn(conn))
	require.NoError(t, err)

	return &testEnv{
		ctx:           ctx,
		instanceAdmin: instanceAdmin,
		databaseAdmin: databaseAdmin,
		spannerClient: spannerClient,
		databaseName:  databaseName,
	}
}

func (env *testEnv) cleanup(t *testing.T) {
	env.spannerClient.Close()
	
	// Delete database
	err := env.databaseAdmin.DropDatabase(env.ctx, &databasepb.DropDatabaseRequest{
		Database: env.databaseName,
	})
	if err != nil {
		t.Logf("Failed to delete database: %v", err)
	}
	
	// Delete instance
	err = env.instanceAdmin.DeleteInstance(env.ctx, &instancepb.DeleteInstanceRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance),
	})
	if err != nil {
		t.Logf("Failed to delete instance: %v", err)
	}
	
	env.instanceAdmin.Close()
	env.databaseAdmin.Close()
}

func (env *testEnv) seedData(t *testing.T, mutations []*spanner.Mutation) {
	_, err := env.spannerClient.Apply(env.ctx, mutations)
	require.NoError(t, err)
}

func loadConfig(t *testing.T, filename string) *config.Config {
	path := filepath.Join("testdata", filename)
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var cfg config.Config
	err = yaml.Unmarshal(data, &cfg)
	require.NoError(t, err)

	// Set runtime values
	cfg.Project = testProject
	cfg.Instance = testInstance
	cfg.Database = testDatabase

	return &cfg
}

func TestValidationSuccess(t *testing.T) {
	env := setupTestEnvironment(t)
	defer env.cleanup(t)

	// Seed test data
	mutations := []*spanner.Mutation{
		spanner.InsertOrUpdate("users",
			[]string{"id", "name", "email", "active", "score", "created_at"},
			[]interface{}{int64(1), "Alice", "alice@example.com", true, 95.5, time.Now()},
		),
		spanner.InsertOrUpdate("users",
			[]string{"id", "name", "email", "active", "score", "created_at"},
			[]interface{}{int64(2), "Bob", "bob@example.com", false, 87.3, time.Now()},
		),
		spanner.InsertOrUpdate("products",
			[]string{"id", "name", "price", "stock"},
			[]interface{}{int64(1), "Product A", 29.99, int64(100)},
		),
	}
	env.seedData(t, mutations)

	// Load test config
	cfg := loadConfig(t, "success_test.yaml")
	
	// Run validation
	v := validator.New(env.spannerClient)
	results, err := v.Validate(env.ctx, cfg)
	require.NoError(t, err)
	
	// Check results
	assert.True(t, results.Success)
	assert.Empty(t, results.Errors)
	assert.Len(t, results.Tables, 2)
}

func TestValidationFailureWrongRowCount(t *testing.T) {
	env := setupTestEnvironment(t)
	defer env.cleanup(t)

	// Seed only one user instead of expected two
	mutations := []*spanner.Mutation{
		spanner.InsertOrUpdate("users",
			[]string{"id", "name", "email", "active", "score", "created_at"},
			[]interface{}{int64(1), "Alice", "alice@example.com", true, 95.5, time.Now()},
		),
	}
	env.seedData(t, mutations)

	// Load test config expecting 2 rows
	cfg := loadConfig(t, "wrong_count_test.yaml")
	
	// Run validation
	v := validator.New(env.spannerClient)
	results, err := v.Validate(env.ctx, cfg)
	require.NoError(t, err)
	
	// Check results
	assert.False(t, results.Success)
	assert.NotEmpty(t, results.Errors)
	assert.Contains(t, results.Errors[0], "row count mismatch")
}

func TestValidationFailureWrongValue(t *testing.T) {
	env := setupTestEnvironment(t)
	defer env.cleanup(t)

	// Seed data with different values than expected
	mutations := []*spanner.Mutation{
		spanner.InsertOrUpdate("users",
			[]string{"id", "name", "email", "active", "score", "created_at"},
			[]interface{}{int64(1), "Alice", "alice@example.com", true, 50.0, time.Now()}, // score is 50.0 instead of expected 95.5
		),
	}
	env.seedData(t, mutations)

	// Load test config
	cfg := loadConfig(t, "wrong_value_test.yaml")
	
	// Run validation
	v := validator.New(env.spannerClient)
	results, err := v.Validate(env.ctx, cfg)
	require.NoError(t, err)
	
	// Check results
	assert.False(t, results.Success)
	assert.NotEmpty(t, results.Errors)
	assert.Contains(t, results.Errors[0], "value mismatch")
}

func TestValidationTableNotFound(t *testing.T) {
	env := setupTestEnvironment(t)
	defer env.cleanup(t)

	// Load test config with non-existent table
	cfg := loadConfig(t, "table_not_found_test.yaml")
	
	// Run validation
	v := validator.New(env.spannerClient)
	results, err := v.Validate(env.ctx, cfg)
	require.NoError(t, err)
	
	// Check results
	assert.False(t, results.Success)
	assert.NotEmpty(t, results.Errors)
	assert.Contains(t, results.Errors[0], "table not found")
}

func TestValidationWithNullValues(t *testing.T) {
	env := setupTestEnvironment(t)
	defer env.cleanup(t)

	// Seed data with null values
	mutations := []*spanner.Mutation{
		spanner.InsertOrUpdate("users",
			[]string{"id", "name", "email", "active", "score", "created_at"},
			[]interface{}{int64(1), "Alice", spanner.NullString{}, true, spanner.NullFloat64{}, time.Now()},
		),
	}
	env.seedData(t, mutations)

	// Load test config
	cfg := loadConfig(t, "null_values_test.yaml")
	
	// Run validation
	v := validator.New(env.spannerClient)
	results, err := v.Validate(env.ctx, cfg)
	require.NoError(t, err)
	
	// Check results based on config expectations
	assert.True(t, results.Success)
}