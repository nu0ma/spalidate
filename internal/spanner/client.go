package spanner

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	client   *spanner.Client
	database string
}

type Row map[string]interface{}

func NewClient(project, instance, database string, port int) (*Client, error) {
	ctx := context.Background()

	databasePath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)

	// Check if we're using the emulator
	var opts []option.ClientOption
	if emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST"); emulatorHost != "" {
		// For emulator, use insecure connection
		conn, err := grpc.Dial(emulatorHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to dial emulator: %w", err)
		}
		opts = append(opts, option.WithGRPCConn(conn))
	}

	// The Spanner client library automatically detects and uses SPANNER_EMULATOR_HOST
	client, err := spanner.NewClient(ctx, databasePath, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %w", err)
	}

	return &Client{
		client:   client,
		database: databasePath,
	}, nil
}

func (c *Client) Close() {
	c.client.Close()
}

func (c *Client) CountRows(tableName string) (int, error) {
	ctx := context.Background()

	query := fmt.Sprintf("SELECT COUNT(*) as count FROM %s", tableName)
	stmt := spanner.Statement{SQL: query}

	iter := c.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return 0, fmt.Errorf("failed to count rows in table %s: %w", tableName, err)
	}

	var count int64
	if err := row.Columns(&count); err != nil {
		return 0, fmt.Errorf("failed to scan count: %w", err)
	}

	return int(count), nil
}

func (c *Client) QueryRows(tableName string, columns []string) ([]Row, error) {
	ctx := context.Background()

	columnList := "*"
	if len(columns) > 0 {
		columnList = ""
		for i, col := range columns {
			if i > 0 {
				columnList += ", "
			}
			columnList += col
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s", columnList, tableName)
	stmt := spanner.Statement{SQL: query}

	iter := c.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var rows []Row
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate rows: %w", err)
		}

		rowData := make(Row)
		columnNames := row.ColumnNames()
		values := make([]interface{}, len(columnNames))
		pointers := make([]interface{}, len(columnNames))

		for i := range values {
			pointers[i] = &values[i]
		}

		if err := row.Columns(pointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		for i, name := range columnNames {
			rowData[name] = values[i]
		}

		rows = append(rows, rowData)
	}

	return rows, nil
}

func (c *Client) TableExists(tableName string) (bool, error) {
	ctx := context.Background()

	query := `
		SELECT COUNT(*) as count 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_NAME = @tableName
	`

	stmt := spanner.Statement{
		SQL: query,
		Params: map[string]interface{}{
			"tableName": tableName,
		},
	}

	iter := c.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	var count int64
	if err := row.Columns(&count); err != nil {
		return false, fmt.Errorf("failed to scan table count: %w", err)
	}

	return count > 0, nil
}
