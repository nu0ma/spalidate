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

func (c *spannerClient) queryRowsWithOrder(tableName string, columns []string, orderBy string) ([]Row, error) {
	ctx := context.Background()

	columnList := "*"
	if len(columns) > 0 {
		columnList = strings.Join(columns, ", ")
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

		rowData, err := c.decodeRow(row)
		if err != nil {
			return nil, err
		}
		rows = append(rows, rowData)
	}

	return rows, nil
}

func (c *spannerClient) decodeRow(row *spanner.Row) (Row, error) {
	rowData := make(Row)
	columnNames := row.ColumnNames()

	for i := 0; i < row.Size(); i++ {
		var col spanner.GenericColumnValue
		if err := row.Column(i, &col); err != nil {
			return nil, fmt.Errorf("failed to read column %d: %w", i, err)
		}

		value, err := c.decodeColumn(&col)
		if err != nil {
			return nil, fmt.Errorf("failed to decode column %s: %w", columnNames[i], err)
		}
		rowData[columnNames[i]] = value
	}

	return rowData, nil
}

func (c *spannerClient) decodeColumn(col *spanner.GenericColumnValue) (interface{}, error) {
	// Define decoder functions for each type
	decoders := map[spannerpb.TypeCode]func(*spanner.GenericColumnValue) (interface{}, error){
		spannerpb.TypeCode_INT64:     c.decodeInt64,
		spannerpb.TypeCode_STRING:    c.decodeString,
		spannerpb.TypeCode_FLOAT64:   c.decodeFloat64,
		spannerpb.TypeCode_BOOL:      c.decodeBool,
		spannerpb.TypeCode_BYTES:     c.decodeBytes,
		spannerpb.TypeCode_TIMESTAMP: c.decodeTimestamp,
		spannerpb.TypeCode_DATE:      c.decodeDate,
		spannerpb.TypeCode_NUMERIC:   c.decodeNumeric,
		spannerpb.TypeCode_JSON:      c.decodeJSON,
		spannerpb.TypeCode_ARRAY:     c.decodeArray,
	}

	decoder, ok := decoders[col.Type.Code]
	if !ok {
		return col.Value, nil
	}

	return decoder(col)
}

func (c *spannerClient) decodeInt64(col *spanner.GenericColumnValue) (interface{}, error) {
	var v spanner.NullInt64
	if err := col.Decode(&v); err != nil {
		var nonNull int64
		if err := col.Decode(&nonNull); err != nil {
			return nil, err
		}
		return nonNull, nil
	}
	return v, nil
}

func (c *spannerClient) decodeString(col *spanner.GenericColumnValue) (interface{}, error) {
	var v spanner.NullString
	if err := col.Decode(&v); err != nil {
		var nonNull string
		if err := col.Decode(&nonNull); err != nil {
			return nil, err
		}
		return nonNull, nil
	}
	return v, nil
}

func (c *spannerClient) decodeFloat64(col *spanner.GenericColumnValue) (interface{}, error) {
	var v spanner.NullFloat64
	if err := col.Decode(&v); err != nil {
		var nonNull float64
		if err := col.Decode(&nonNull); err != nil {
			return nil, err
		}
		return nonNull, nil
	}
	return v, nil
}

func (c *spannerClient) decodeBool(col *spanner.GenericColumnValue) (interface{}, error) {
	var v spanner.NullBool
	if err := col.Decode(&v); err != nil {
		var nonNull bool
		if err := col.Decode(&nonNull); err != nil {
			return nil, err
		}
		return nonNull, nil
	}
	return v, nil
}

func (c *spannerClient) decodeBytes(col *spanner.GenericColumnValue) (interface{}, error) {
	var v []byte
	if err := col.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func (c *spannerClient) decodeTimestamp(col *spanner.GenericColumnValue) (interface{}, error) {
	var v spanner.NullTime
	if err := col.Decode(&v); err != nil {
		var nonNull time.Time
		if err := col.Decode(&nonNull); err != nil {
			return nil, err
		}
		return nonNull, nil
	}
	return v, nil
}

func (c *spannerClient) decodeDate(col *spanner.GenericColumnValue) (interface{}, error) {
	var v spanner.NullDate
	if err := col.Decode(&v); err != nil {
		var nonNull civil.Date
		if err := col.Decode(&nonNull); err != nil {
			return nil, err
		}
		return nonNull, nil
	}
	return v, nil
}

func (c *spannerClient) decodeNumeric(col *spanner.GenericColumnValue) (interface{}, error) {
	var v spanner.NullNumeric
	if err := col.Decode(&v); err != nil {
		var nonNull interface{}
		if err := col.Decode(&nonNull); err != nil {
			return nil, err
		}
		if sv, ok := nonNull.(*structpb.Value); ok && sv.GetStringValue() != "" {
			nonNull = sv.GetStringValue()
		}
		return nonNull, nil
	}
	return v, nil
}

func (c *spannerClient) decodeJSON(col *spanner.GenericColumnValue) (interface{}, error) {
	var v spanner.NullJSON
	if err := col.Decode(&v); err != nil {
		var nonNull interface{}
		if err := col.Decode(&nonNull); err != nil {
			return nil, err
		}
		if sv, ok := nonNull.(*structpb.Value); ok && sv.GetStringValue() != "" {
			nonNull = sv.GetStringValue()
		}
		return nonNull, nil
	}
	return v, nil
}

func (c *spannerClient) decodeArray(col *spanner.GenericColumnValue) (interface{}, error) {
	if col.Type.ArrayElementType == nil {
		return col.Value, nil
	}

	arrayDecoders := map[spannerpb.TypeCode]func(*spanner.GenericColumnValue) (interface{}, error){
		spannerpb.TypeCode_STRING: func(col *spanner.GenericColumnValue) (interface{}, error) {
			var v []string
			return v, col.Decode(&v)
		},
		spannerpb.TypeCode_INT64: func(col *spanner.GenericColumnValue) (interface{}, error) {
			var v []int64
			return v, col.Decode(&v)
		},
		spannerpb.TypeCode_BOOL: func(col *spanner.GenericColumnValue) (interface{}, error) {
			var v []bool
			return v, col.Decode(&v)
		},
		spannerpb.TypeCode_FLOAT64: func(col *spanner.GenericColumnValue) (interface{}, error) {
			var v []float64
			return v, col.Decode(&v)
		},
	}

	decoder, ok := arrayDecoders[col.Type.ArrayElementType.Code]
	if !ok {
		return col.Value, nil
	}

	return decoder(col)
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