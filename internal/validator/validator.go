package validator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/civil"
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
	// Read column data
	err := iter.Do(func(row *spanner.Row) error {
		columnNames := row.ColumnNames()
		rowData := make(map[string]any)

		for i, colName := range columnNames {
			var gcv spanner.GenericColumnValue
			if err := row.Column(i, &gcv); err != nil {
				return fmt.Errorf("failed to get column %s: %w", colName, err)
			}
			val, derr := decodeGenericValue(&gcv)
			if derr != nil {
				return fmt.Errorf("failed to decode column %s: %w", colName, derr)
			}
			rowData[colName] = val
		}

		rows = append(rows, rowData)
		return nil
	})

	if err != nil && err != iterator.Done {
		return fmt.Errorf("query execution failed: %w", err)
	}

	// When 'columns' is set, verify that for each expected spec
	// at least one row strictly matches with all columns.
	if len(tableConfig.Columns) > 0 {
		for _, expectedData := range tableConfig.Columns {
			matched := false
			for _, actualData := range rows {
				// Key sets must match exactly
				if !sameKeySet(actualData, expectedData) {
					continue
				}
				// Compare all columns
				ok := true
				for key, actualValue := range actualData {
					expectedValue := expectedData[key]
					if err := v.validateData(actualValue, expectedValue); err != nil {
						ok = false
						break
					}
				}
				if ok {
					matched = true
					break
				}
			}
			if !matched {
				return fmt.Errorf("no row strictly matched spec (all columns required): %v", expectedData)
			}
		}
	}

	return nil
}

func (v *Validator) validateData(record any, expectedData any) error {
	// Handle nil/NULL values
	if isSpannerNull(record) {
		if expectedData == nil {
			return nil
		}
		return fmt.Errorf("expected non-null, got NULL (expected=%v)", expectedData)
	}
	if record == nil {
		if expectedData == nil {
			return nil
		}
		return fmt.Errorf("expected %v, got <nil>", expectedData)
	}

	switch r := record.(type) {
	// DATE
	case spanner.NullDate:
		if !r.Valid {
			if expectedData == nil {
				return nil
			}
			return fmt.Errorf("expected %v, got NULL(date)", expectedData)
		}
		return compareDates(r.Date, expectedData)
	case civil.Date:
		return compareDates(r, expectedData)
	case spanner.NullString:
		if !r.Valid {
			if expectedData == nil {
				return nil
			}
			return fmt.Errorf("expected %v, got NULL(string)", expectedData)
		}
		return compareStrings(r.StringVal, expectedData)
	case string:
		return compareStrings(r, expectedData)
	case spanner.NullInt64:
		if !r.Valid {
			if expectedData == nil {
				return nil
			}
			return fmt.Errorf("expected %v, got NULL(int64)", expectedData)
		}
		return compareNumbers(r.Int64, expectedData)
	case int64:
		return compareNumbers(r, expectedData)
	case spanner.NullFloat64:
		if !r.Valid {
			if expectedData == nil {
				return nil
			}
			return fmt.Errorf("expected %v, got NULL(float64)", expectedData)
		}
		return compareNumbers(r.Float64, expectedData)
	case float64:
		return compareNumbers(r, expectedData)

		// JSON (Spanner native JSON type)
	case spanner.NullJSON:
		if !r.Valid {
			if expectedData == nil {
				return nil
			}
			return fmt.Errorf("expected %v, got NULL(json)", expectedData)
		}
		return compareJSON(r.Value, expectedData)
	case spanner.NullBool:
		if !r.Valid {
			if expectedData == nil {
				return nil
			}
			return fmt.Errorf("expected %v, got NULL(bool)", expectedData)
		}
		ev, ok := expectedData.(bool)
		if !ok {
			return typeMismatchError("bool", expectedData)
		}
		if r.Bool != ev {
			return valueMismatchError(r.Bool, ev)
		}
		return nil
	case bool:
		ev, ok := expectedData.(bool)
		if !ok {
			return typeMismatchError("bool", expectedData)
		}
		if r != ev {
			return valueMismatchError(r, ev)
		}
		return nil
	case spanner.NullTime:
		if !r.Valid {
			if expectedData == nil {
				return nil
			}
			return fmt.Errorf("expected %v, got NULL(timestamp)", expectedData)
		}
		return compareTimestamps(r.Time, expectedData)
	case time.Time:
		return compareTimestamps(r, expectedData)
	}

	return fmt.Errorf("unsupported type: %T (value=%v)", record, record)
}

// --- Helpers ---

func isSpannerNull(v any) bool {
	switch x := v.(type) {
	case spanner.NullString:
		return !x.Valid
	case spanner.NullInt64:
		return !x.Valid
	case spanner.NullFloat64:
		return !x.Valid
	case spanner.NullBool:
		return !x.Valid
	case spanner.NullTime:
		return !x.Valid
	default:
		return false
	}
}

func typeMismatchError(expectedKind string, got any) error {
	return fmt.Errorf("type mismatch: expected %s, got %T (value=%v)", expectedKind, got, got)
}

func valueMismatchError(actual, expected any) error {
	return fmt.Errorf("value mismatch: actual=%v, expected=%v", actual, expected)
}

func compareStrings(actual string, expected any) error {
	switch ev := expected.(type) {
	case string:
		// If expected looks like JSON, compare as JSON
		if looksLikeJSON(ev) {
			var a any
			if err := json.Unmarshal([]byte(actual), &a); err != nil {
				return fmt.Errorf("actual is not valid JSON: %w", err)
			}
			var e any
			if err := json.Unmarshal([]byte(ev), &e); err != nil {
				return fmt.Errorf("expected is not valid JSON: %w", err)
			}
			if !deepEqualJSON(a, e) {
				// Keep diff representation concise
				aa, _ := json.Marshal(a)
				ee, _ := json.Marshal(e)
				return valueMismatchError(string(aa), string(ee))
			}
			return nil
		}
		if actual != ev {
			return valueMismatchError(actual, ev)
		}
		return nil
	default:
		// Avoid stringifying when YAML gives numbers/bools; report type mismatch
		return typeMismatchError("string", expected)
	}
}

// JSON comparison (Spanner JSON or generic)
func compareJSON(actual any, expected any) error {
	var a any
	var e any

	// Normalize actual side
	switch v := actual.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &a); err != nil {
			return fmt.Errorf("actual is not valid JSON: %w", err)
		}
	default:
		// Spanner NullJSON.Value is assumed to be map/slice already
		a = v
	}

	// Normalize expected side
	switch v := expected.(type) {
	case string:
		if !looksLikeJSON(v) {
			return typeMismatchError("json(string)", expected)
		}
		if err := json.Unmarshal([]byte(v), &e); err != nil {
			return fmt.Errorf("expected is not valid JSON: %w", err)
		}
	default:
		// Accept map[string]any / []any as-is
		e = v
	}

	if !deepEqualJSON(a, e) {
		aa, _ := json.Marshal(a)
		ee, _ := json.Marshal(e)
		return valueMismatchError(string(aa), string(ee))
	}
	return nil
}

func looksLikeJSON(s string) bool {
	t := strings.TrimSpace(s)
	return (strings.HasPrefix(t, "{") && strings.HasSuffix(t, "}")) ||
		(strings.HasPrefix(t, "[") && strings.HasSuffix(t, "]"))
}

func deepEqualJSON(a, b any) bool {
	// JSON numbers become float64; DeepEqual is sufficient.
	return reflect.DeepEqual(a, b)
}

// sameKeySet checks whether two maps have exactly the same key set.
func sameKeySet(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			return false
		}
	}
	return true
}

func compareNumbers(actual any, expected any) error {
	// 'actual' is expected to be int64 or float64
	avInt, aIsInt := toInt64(actual)
	avFloat, aIsFloat := toFloat64(actual)

	// 'expected' may be int, int64, or float64
	evInt, eIsInt := toInt64(expected)
	evFloat, eIsFloat := toFloat64(expected)

	switch {
	case aIsInt && eIsInt:
		if avInt != evInt {
			return valueMismatchError(avInt, evInt)
		}
		return nil
	case aIsFloat && eIsFloat:
		// No epsilon for floats (simplified). Add tolerance if needed.
		if avFloat != evFloat {
			return valueMismatchError(avFloat, evFloat)
		}
		return nil
	case aIsInt && eIsFloat:
		if float64(avInt) != evFloat {
			return valueMismatchError(float64(avInt), evFloat)
		}
		return nil
	case aIsFloat && eIsInt:
		if avFloat != float64(evInt) {
			return valueMismatchError(avFloat, float64(evInt))
		}
		return nil
	default:
		return typeMismatchError("number", expected)
	}
}

func compareTimestamps(actual time.Time, expected any) error {
	switch ev := expected.(type) {
	case string:
		// Prefer RFC3339 formats
		t, err := parseTimestamp(ev)
		if err != nil {
			return fmt.Errorf("invalid timestamp format for expected value: %w", err)
		}
		if !actual.Equal(t) {
			return valueMismatchError(actual.UTC().Format(time.RFC3339Nano), t.UTC().Format(time.RFC3339Nano))
		}
		return nil
	case time.Time:
		if !actual.Equal(ev) {
			return valueMismatchError(actual.UTC().Format(time.RFC3339Nano), ev.UTC().Format(time.RFC3339Nano))
		}
		return nil
	default:
		return typeMismatchError("timestamp(string RFC3339)", expected)
	}
}

func compareDates(actual civil.Date, expected any) error {
	switch ev := expected.(type) {
	case string:
		// Expect format YYYY-MM-DD
		t, err := time.Parse("2006-01-02", ev)
		if err != nil {
			return fmt.Errorf("invalid date format for expected value (want YYYY-MM-DD): %w", err)
		}
		e := civil.Date{Year: t.Year(), Month: t.Month(), Day: t.Day()}
		if actual != e {
			return valueMismatchError(actual.String(), e.String())
		}
		return nil
	case civil.Date:
		if actual != ev {
			return valueMismatchError(actual.String(), ev.String())
		}
		return nil
	case time.Time:
		// Convert expected timestamp to date (UTC-based truncation)
		e := civil.Date{Year: ev.Year(), Month: ev.Month(), Day: ev.Day()}
		if actual != e {
			return valueMismatchError(actual.String(), e.String())
		}
		return nil
	default:
		return typeMismatchError("date(YYYY-MM-DD)", expected)
	}
}

func parseTimestamp(s string) (time.Time, error) {
	// Accept multiple formats in order
	fmts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	var lastErr error
	for _, f := range fmts {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		} else {
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = errors.New("unknown time parse error")
	}
	return time.Time{}, lastErr
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	default:
		rv := reflect.ValueOf(v)
		if rv.IsValid() && rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Int64 {
			return rv.Int(), true
		}
		return 0, false
	}
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Float32, reflect.Float64:
			return rv.Float(), true
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return float64(rv.Int()), true
		}
		return 0, false
	}
}

// decodeGenericValue decodes a Spanner GenericColumnValue into supported concrete types.
// It returns types that validateData can consume (spanner.Null* or primitives).
func decodeGenericValue(gcv *spanner.GenericColumnValue) (any, error) {
	// DATE type
	{
		var v spanner.NullDate
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v civil.Date
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	// JSON type
	{
		var v spanner.NullJSON
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v spanner.NullString
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v spanner.NullInt64
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v spanner.NullFloat64
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v spanner.NullBool
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v spanner.NullTime
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v string
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v int64
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v float64
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v bool
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	{
		var v time.Time
		if err := gcv.Decode(&v); err == nil {
			return v, nil
		}
	}
	return nil, fmt.Errorf("unsupported column type: %v", gcv.Type)
}
