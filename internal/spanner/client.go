package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

const (
	// JSON TypeCode constant (value 11) for Spanner JSON type
	TypeCode_JSON sppb.TypeCode = 11
)

type Client struct {
	client   *spanner.Client
	database string
}

type Row map[string]interface{}

func NewClient(project, instance, database string, port int) (*Client, error) {
	ctx := context.Background()

	databasePath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)

	client, err := spanner.NewClient(ctx, databasePath)
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
	return c.QueryRowsWithOrder(tableName, columns, "")
}

func (c *Client) QueryRowsWithOrder(tableName string, columns []string, orderBy string) ([]Row, error) {
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
	if orderBy != "" {
		query += fmt.Sprintf(" ORDER BY %s", orderBy)
	}
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

		// Use GenericColumnValue for proper type handling
		for i := 0; i < row.Size(); i++ {
			var col spanner.GenericColumnValue
			if err := row.Column(i, &col); err != nil {
				return nil, fmt.Errorf("failed to read column %d: %w", i, err)
			}

			// GenericColumnValue handles NULL internally during Decode

			// Decode based on the actual type
			switch col.Type.Code {
			case sppb.TypeCode_INT64:
				var v int64
				if err := col.Decode(&v); err != nil {
					return nil, fmt.Errorf("failed to decode int64: %w", err)
				}
				rowData[columnNames[i]] = v
			case sppb.TypeCode_STRING:
				var v string
				if err := col.Decode(&v); err != nil {
					return nil, fmt.Errorf("failed to decode string: %w", err)
				}
				rowData[columnNames[i]] = v
			case sppb.TypeCode_FLOAT64:
				var v float64
				if err := col.Decode(&v); err != nil {
					return nil, fmt.Errorf("failed to decode float64: %w", err)
				}
				rowData[columnNames[i]] = v
			case sppb.TypeCode_BOOL:
				var v bool
				if err := col.Decode(&v); err != nil {
					return nil, fmt.Errorf("failed to decode bool: %w", err)
				}
				rowData[columnNames[i]] = v
			case sppb.TypeCode_BYTES:
				var v []byte
				if err := col.Decode(&v); err != nil {
					return nil, fmt.Errorf("failed to decode bytes: %w", err)
				}
				rowData[columnNames[i]] = v
			case sppb.TypeCode_TIMESTAMP:
				var v time.Time
				if err := col.Decode(&v); err != nil {
					return nil, fmt.Errorf("failed to decode timestamp: %w", err)
				}
				rowData[columnNames[i]] = v
			case sppb.TypeCode_DATE:
				var v civil.Date
				if err := col.Decode(&v); err != nil {
					return nil, fmt.Errorf("failed to decode date: %w", err)
				}
				rowData[columnNames[i]] = v
			case TypeCode_JSON:
				var v spanner.NullJSON
				if err := col.Decode(&v); err != nil {
					return nil, fmt.Errorf("failed to decode json: %w", err)
				}
				if v.Valid {
					rowData[columnNames[i]] = v.Value
				} else {
					rowData[columnNames[i]] = nil
				}
			default:
				// For unknown types, store the raw value
				rowData[columnNames[i]] = col.Value
			}
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
