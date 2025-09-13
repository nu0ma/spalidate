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
	"github.com/nu0ma/spalidate/internal/logging"
	spannerClient "github.com/nu0ma/spalidate/internal/spanner"
	"google.golang.org/api/iterator"
)

type Validator struct {
	config        *config.Config
	spannerClient *spannerClient.Client
}

type colDiff struct {
	column   string
	expected any
	actual   any
}

func NewValidator(config *config.Config, client *spannerClient.Client) *Validator {
	return &Validator{
		config:        config,
		spannerClient: client,
	}
}

func (v *Validator) Validate() error {
	ctx := context.Background()

	names := sortedTableNames(v.config.Tables)
	var errs []string
	for _, tableName := range names {
		tableConfig := v.config.Tables[tableName]
		if err := v.validateTable(ctx, tableName, tableConfig); err != nil {
			errs = append(errs, fmt.Sprintf("validation failed for table %s: %v", tableName, err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
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

	if len(tableConfig.Columns) > 0 {
		// デフォルトで行集合の完全一致を要求
		if err := v.validateStrictRowset(tableName, rows, tableConfig.Columns); err != nil {
			return err
		}
	}

	return nil
}

func (v *Validator) validateStrictRowset(tableName string, actualRows []map[string]any, expectedRows []map[string]any) error {
	if len(actualRows) != len(expectedRows) {
		return fmt.Errorf("unexpected row count for table %s: expected %d, got %d", tableName, len(expectedRows), len(actualRows))
	}
	used := make([]bool, len(actualRows))

	for ei, exp := range expectedRows {
		found := false
		var bestDiffs []colDiff
		for ai, act := range actualRows {
			if used[ai] {
				continue
			}
			if !sameKeySet(act, exp) {
				continue
			}
			diffs := make([]colDiff, 0)
			ok := true
			for key, actualValue := range act {
				expectedValue := exp[key]
				if err := v.validateData(actualValue, expectedValue); err != nil {
					ok = false
					diffs = append(diffs, colDiff{column: key, expected: expectedValue, actual: actualValue})
				}
			}
			if ok {
				used[ai] = true
				found = true
				break
			}
			if len(bestDiffs) == 0 || len(diffs) < len(bestDiffs) {
				bestDiffs = diffs
			}
		}
		if !found {
			if len(bestDiffs) > 0 {
				logging.L().Error(buildMismatchReport(tableName, bestDiffs))
			} else {
				expKeys := sortedKeys(exp)
				var exampleKeys []string
				if len(actualRows) > 0 {
					exampleKeys = sortedKeys(actualRows[0])
				}
				logging.L().Error(buildColumnSetMismatchReport(tableName, expKeys, exampleKeys))
			}
			return fmt.Errorf("expected row %d not found in table %s", ei+1, tableName)
		}
	}

	// any unmatched actual row?
	for _, u := range used {
		if !u {
			return fmt.Errorf("unexpected rows present in table %s", tableName)
		}
	}
	return nil
}

func (v *Validator) validateData(record any, expectedData any) error {
	switch r := record.(type) {
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

func valueToPretty(v any) string {
	switch x := v.(type) {
	case spanner.NullString:
		if !x.Valid {
			return "NULL(string)"
		}
		return x.StringVal
	case spanner.NullInt64:
		if !x.Valid {
			return "NULL(int64)"
		}
		return fmt.Sprintf("%d", x.Int64)
	case spanner.NullFloat64:
		if !x.Valid {
			return "NULL(float64)"
		}
		return fmt.Sprintf("%v", x.Float64)
	case spanner.NullBool:
		if !x.Valid {
			return "NULL(bool)"
		}
		return fmt.Sprintf("%t", x.Bool)
	case spanner.NullTime:
		if !x.Valid {
			return "NULL(timestamp)"
		}
		return x.Time.Format(time.RFC3339)
	case spanner.NullDate:
		if !x.Valid {
			return "NULL(date)"
		}
		return x.Date.String()
	case spanner.NullJSON:
		if !x.Valid {
			return "NULL(json)"
		}
		// Try to compact the JSON value
		if x.Value == nil {
			return "null"
		}
		b, err := json.Marshal(x.Value)
		if err != nil {
			return fmt.Sprintf("%v", x.Value)
		}
		return string(b)
	case civil.Date:
		return x.String()
	case time.Time:
		return x.Format(time.RFC3339)
	case string:
		// Keep as-is; if it looks like JSON, compact it to one line
		if looksLikeJSON(x) {
			var obj any
			if err := json.Unmarshal([]byte(x), &obj); err == nil {
				if b, err := json.Marshal(obj); err == nil {
					return string(b)
				}
			}
		}
		return x
	case map[string]any, []any:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprintf("%v", x)
		}
		return string(b)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func sortedKeys(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	// simple insertion sort to avoid importing sort unnecessarily in this file
	for i := 1; i < len(ks); i++ {
		j := i
		for j > 0 && ks[j-1] > ks[j] {
			ks[j-1], ks[j] = ks[j], ks[j-1]
			j--
		}
	}
	return ks
}

func sortedTableNames(m map[string]config.TableConfig) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	for i := 1; i < len(ks); i++ {
		j := i
		for j > 0 && ks[j-1] > ks[j] {
			ks[j-1], ks[j] = ks[j], ks[j-1]
			j--
		}
	}
	return ks
}

func buildMismatchReport(table string, diffs []colDiff) string {
	var b strings.Builder
	fmt.Fprintf(&b, "✖️ table %s: expected row does not match\n", table)
	fmt.Fprintf(&b, "    column mismatch: %d\n", len(diffs))
	for i, d := range diffs {
		fmt.Fprintf(&b, "\n  %d)  column: %s\n", i+1, d.column)
		fmt.Fprintf(&b, "     ▸ expected: %s\n", valueToPretty(d.expected))
		fmt.Fprintf(&b, "     ▸   actual: %s\n", valueToPretty(d.actual))

	}
	return b.String()
}

func buildColumnSetMismatchReport(table string, expectedCols, exampleActualCols []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "✖️ table %s: expected column set does not match\n", table)
	fmt.Fprintf(&b, "   🧩 expected columns: %s\n", strings.Join(expectedCols, ", "))
	if len(exampleActualCols) > 0 {
		fmt.Fprintf(&b, "   🔎 example actual:  %s\n", strings.Join(exampleActualCols, ", "))
	}
	return b.String()
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
