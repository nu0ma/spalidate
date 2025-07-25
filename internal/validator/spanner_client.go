package validator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/structpb"
)

type spannerClient struct {
	client   *spanner.Client
	database string
}

func newSpannerClient(project, instance, database string, port int) (*spannerClient, error) {
	ctx := context.Background()

	databasePath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)

	client, err := spanner.NewClient(ctx, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %w", err)
	}

	return &spannerClient{
		client:   client,
		database: databasePath,
	}, nil
}

func (c *spannerClient) countRows(tableName string) (int, error) {
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

func (c *spannerClient) queryRows(tableName string, columns []string) ([]Row, error) {
	return c.queryRowsWithOrder(tableName, columns, "")
}

func (c *spannerClient) queryRowsWithOrder(tableName string, columns []string, orderBy string) ([]Row, error) {
	ctx := context.Background()

	columnList := "*"
	if len(columns) > 0 {
		var builder strings.Builder
		for i, col := range columns {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(col)
		}
		columnList = builder.String()
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

		for i := 0; i < row.Size(); i++ {
			var col spanner.GenericColumnValue
			if err := row.Column(i, &col); err != nil {
				return nil, fmt.Errorf("failed to read column %d: %w", i, err)
			}

			// Check if column allows NULL values by attempting to decode as nullable type first
			switch col.Type.Code {
			case spannerpb.TypeCode_INT64:
				var v spanner.NullInt64
				if err := col.Decode(&v); err != nil {
					// Try non-nullable version
					var nonNull int64
					if err := col.Decode(&nonNull); err != nil {
						return nil, fmt.Errorf("failed to decode int64: %w", err)
					}
					rowData[columnNames[i]] = nonNull
				} else {
					rowData[columnNames[i]] = v
				}
			case spannerpb.TypeCode_STRING:
				var v spanner.NullString
				if err := col.Decode(&v); err != nil {
					// Try non-nullable version
					var nonNull string
					if err := col.Decode(&nonNull); err != nil {
						return nil, fmt.Errorf("failed to decode string: %w", err)
					}
					rowData[columnNames[i]] = nonNull
				} else {
					rowData[columnNames[i]] = v
				}
			case spannerpb.TypeCode_FLOAT64:
				var v spanner.NullFloat64
				if err := col.Decode(&v); err != nil {
					// Try non-nullable version
					var nonNull float64
					if err := col.Decode(&nonNull); err != nil {
						return nil, fmt.Errorf("failed to decode float64: %w", err)
					}
					rowData[columnNames[i]] = nonNull
				} else {
					rowData[columnNames[i]] = v
				}
			case spannerpb.TypeCode_BOOL:
				var v spanner.NullBool
				if err := col.Decode(&v); err != nil {
					// Try non-nullable version
					var nonNull bool
					if err := col.Decode(&nonNull); err != nil {
						return nil, fmt.Errorf("failed to decode bool: %w", err)
					}
					rowData[columnNames[i]] = nonNull
				} else {
					rowData[columnNames[i]] = v
				}
			case spannerpb.TypeCode_BYTES:
				var v []byte
				if err := col.Decode(&v); err != nil {
					return nil, fmt.Errorf("failed to decode bytes: %w", err)
				}
				rowData[columnNames[i]] = v
			case spannerpb.TypeCode_TIMESTAMP:
				var v spanner.NullTime
				if err := col.Decode(&v); err != nil {
					// Try non-nullable version
					var nonNull time.Time
					if err := col.Decode(&nonNull); err != nil {
						return nil, fmt.Errorf("failed to decode timestamp: %w", err)
					}
					rowData[columnNames[i]] = nonNull
				} else {
					rowData[columnNames[i]] = v
				}
			case spannerpb.TypeCode_DATE:
				var v spanner.NullDate
				if err := col.Decode(&v); err != nil {
					// Try non-nullable version
					var nonNull civil.Date
					if err := col.Decode(&nonNull); err != nil {
						return nil, fmt.Errorf("failed to decode date: %w", err)
					}
					rowData[columnNames[i]] = nonNull
				} else {
					rowData[columnNames[i]] = v
				}
			case spannerpb.TypeCode_NUMERIC:
				var v spanner.NullNumeric
				if err := col.Decode(&v); err != nil {
					// Try non-nullable version using interface{}
					var nonNull interface{}
					if err := col.Decode(&nonNull); err != nil {
						return nil, fmt.Errorf("failed to decode numeric: %w", err)
					}
					// Convert structpb.Value to string if needed
					if sv, ok := nonNull.(*structpb.Value); ok && sv.GetStringValue() != "" {
						nonNull = sv.GetStringValue()
					}
					rowData[columnNames[i]] = nonNull
				} else {
					rowData[columnNames[i]] = v
				}
			case spannerpb.TypeCode_JSON:
				var v spanner.NullJSON
				if err := col.Decode(&v); err != nil {
					// Try non-nullable version using interface{}
					var nonNull interface{}
					if err := col.Decode(&nonNull); err != nil {
						return nil, fmt.Errorf("failed to decode json: %w", err)
					}
					// Convert structpb.Value to string if needed
					if sv, ok := nonNull.(*structpb.Value); ok && sv.GetStringValue() != "" {
						nonNull = sv.GetStringValue()
					}
					rowData[columnNames[i]] = nonNull
				} else {
					rowData[columnNames[i]] = v
				}
			case spannerpb.TypeCode_ARRAY:
				// For arrays, we need to handle different element types
				if col.Type.ArrayElementType != nil {
					switch col.Type.ArrayElementType.Code {
					case spannerpb.TypeCode_STRING:
						var v []string
						if err := col.Decode(&v); err != nil {
							return nil, fmt.Errorf("failed to decode string array: %w", err)
						}
						rowData[columnNames[i]] = v
					case spannerpb.TypeCode_INT64:
						var v []int64
						if err := col.Decode(&v); err != nil {
							return nil, fmt.Errorf("failed to decode int64 array: %w", err)
						}
						rowData[columnNames[i]] = v
					case spannerpb.TypeCode_BOOL:
						var v []bool
						if err := col.Decode(&v); err != nil {
							return nil, fmt.Errorf("failed to decode bool array: %w", err)
						}
						rowData[columnNames[i]] = v
					case spannerpb.TypeCode_FLOAT64:
						var v []float64
						if err := col.Decode(&v); err != nil {
							return nil, fmt.Errorf("failed to decode float64 array: %w", err)
						}
						rowData[columnNames[i]] = v
					default:
						// For other array types, fall back to generic value
						rowData[columnNames[i]] = col.Value
					}
				} else {
					rowData[columnNames[i]] = col.Value
				}
			default:
				rowData[columnNames[i]] = col.Value
			}
		}

		rows = append(rows, rowData)
	}

	return rows, nil
}

func (c *spannerClient) tableExists(tableName string) (bool, error) {
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
