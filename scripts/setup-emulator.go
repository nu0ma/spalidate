package main

import (
	"context"
	"fmt"
	"log"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()

	// Set up clients for emulator - connect to gRPC endpoint
	opts := []option.ClientOption{
		option.WithEndpoint("localhost:9010"),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	// Create instance admin client
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		log.Fatalf("Failed to create instance admin client: %v", err)
	}
	defer instanceAdminClient.Close()

	// Create database admin client
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		log.Fatalf("Failed to create database admin client: %v", err)
	}
	defer databaseAdminClient.Close()

	// Create instance
	fmt.Println("Creating instance...")
	instanceReq := &instancepb.CreateInstanceRequest{
		Parent:     "projects/test-project",
		InstanceId: "test-instance",
		Instance: &instancepb.Instance{
			DisplayName: "Test Instance",
			Config:      "projects/test-project/instanceConfigs/emulator-config",
			NodeCount:   1,
		},
	}

	instanceOp, err := instanceAdminClient.CreateInstance(ctx, instanceReq)
	if err != nil {
		log.Printf("Failed to create instance (may already exist): %v", err)
	} else {
		_, err = instanceOp.Wait(ctx)
		if err != nil {
			log.Printf("Failed to wait for instance creation: %v", err)
		} else {
			fmt.Println("Instance created successfully!")
		}
	}

	// Create database
	fmt.Println("Creating database...")
	databaseReq := &databasepb.CreateDatabaseRequest{
		Parent:          "projects/test-project/instances/test-instance",
		CreateStatement: "CREATE DATABASE `test-database`",
		ExtraStatements: []string{
			"CREATE TABLE Users (UserID STRING(36) NOT NULL, Name STRING(100) NOT NULL, Email STRING(255) NOT NULL, Status INT64 NOT NULL, CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (UserID)",
			"CREATE TABLE Products (ProductID STRING(36) NOT NULL, Name STRING(200) NOT NULL, Price INT64 NOT NULL, IsActive BOOL NOT NULL, CategoryID STRING(36), CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ProductID)",
			"CREATE TABLE Orders (OrderID STRING(36) NOT NULL, UserID STRING(36) NOT NULL, ProductID STRING(36) NOT NULL, Quantity INT64 NOT NULL, OrderDate TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (UserID, ProductID), INTERLEAVE IN PARENT Users ON DELETE CASCADE",
		},
	}

	databaseOp, err := databaseAdminClient.CreateDatabase(ctx, databaseReq)
	if err != nil {
		log.Printf("Failed to create database (may already exist): %v", err)
	} else {
		_, err = databaseOp.Wait(ctx)
		if err != nil {
			log.Printf("Failed to wait for database creation: %v", err)
		} else {
			fmt.Println("Database created successfully!")
		}
	}

	fmt.Println("Setup complete!")
}
