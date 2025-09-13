package validator

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/nu0ma/spalidate/internal/config"
	spannerClient "github.com/nu0ma/spalidate/internal/spanner"
	"google.golang.org/api/iterator"
)

type Validator struct {
	config        *config.Config
	spannerClient *spannerClient.Client
}

func NewValidator(config *config.Config, client *spannerClient.Client) *Validator {
	return &Validator{
		config:        config,
		spannerClient: client,
	}
}

func (v *Validator) Validate() error {
	ctx := context.Background()

	for tableName, tableConfig := range v.config.Tables {
		if err := v.validateTable(ctx, tableName, tableConfig); err != nil {
			return fmt.Errorf("validation failed for table %s: %w", tableName, err)
		}
	}

	return nil
}

func (v *Validator) validateTable(ctx context.Context, tableName string, tableConfig config.TableConfig) error {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	iter := v.spannerClient.Query(ctx, query)
	defer iter.Stop()

	var rows []map[string]any
	err := iter.Do(func(row *spanner.Row) error {
		columnNames := row.ColumnNames()
		rowData := make(map[string]any)

		for i, colName := range columnNames {
			var val any
			if err := row.Column(i, &val); err != nil {
				return fmt.Errorf("failed to get column %s: %w", colName, err)
			}
			rowData[colName] = val
		}

		rows = append(rows, rowData)
		return nil
	})

	if err != nil {
		if err == iterator.Done {
			if tableConfig.Count > 0 {
				return fmt.Errorf("expected %d rows, but got 0", tableConfig.Count)
			}
			return nil
		}
		return fmt.Errorf("query execution failed: %w", err)
	}

	// 行数の検証
	if len(rows) != tableConfig.Count {
		return fmt.Errorf("row count mismatch: expected %d, got %d", tableConfig.Count, len(rows))
	}

	// columnsが設定されている場合は最初の行を検証
	if len(tableConfig.Columns) > 0 && len(rows) > 0 {
		expectedData := tableConfig.Columns[0]
		actualData := rows[0]

		for key, expectedValue := range expectedData {
			actualValue, exists := actualData[key]
			if !exists {
				return fmt.Errorf("column %s not found in actual data", key)
			}

			if err := v.validateData(actualValue, expectedValue); err != nil {
				return fmt.Errorf("column %s validation failed: %w", key, err)
			}

		}
	}

	return nil
}

func (v *Validator) validateData(record any, expectedData any) error {
	fmt.Printf("record: %+v\n", record)
	fmt.Printf("expectedData: %+v\n", expectedData)
	// TODO: ここにバリデーションのロジックを書く
	return nil
}
