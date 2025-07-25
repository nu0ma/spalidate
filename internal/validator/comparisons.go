package validator

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"google.golang.org/protobuf/types/known/structpb"
)

func (v *Validator) compareValues(expected, actual interface{}) bool {
	if expected == nil && actual == nil {
		return true
	}

	// Handle Spanner null types when expected is nil
	if expected == nil && actual != nil {
		actualType := reflect.TypeOf(actual)
		switch actualType.String() {
		case "spanner.NullString":
			return v.compareNullString(expected, actual)
		case "spanner.NullInt64":
			return v.compareNullInt64(expected, actual)
		case "spanner.NullBool":
			return v.compareNullBool(expected, actual)
		case "spanner.NullFloat64":
			return v.compareNullFloat64(expected, actual)
		case "spanner.NullTime":
			return v.compareNullTime(expected, actual)
		case "spanner.NullDate":
			return v.compareNullDate(expected, actual)
		case "spanner.NullNumeric":
			return v.compareNullNumeric(expected, actual)
		case "spanner.NullJSON":
			return v.compareNullJSON(expected, actual)
		}
		return false
	}

	if expected == nil || actual == nil {
		return false
	}

	expectedType := reflect.TypeOf(expected)
	actualType := reflect.TypeOf(actual)

	// Handle structpb.Value types (from non-nullable NUMERIC/JSON)
	if strings.Contains(actualType.String(), "structpb.Value") {
		return v.compareStructPBValue(expected, actual)
	}

	// Handle Spanner null types first
	switch actualType.String() {
	case "spanner.NullString":
		return v.compareNullString(expected, actual)
	case "spanner.NullInt64":
		return v.compareNullInt64(expected, actual)
	case "spanner.NullBool":
		return v.compareNullBool(expected, actual)
	case "spanner.NullFloat64":
		return v.compareNullFloat64(expected, actual)
	case "spanner.NullTime":
		return v.compareNullTime(expected, actual)
	case "spanner.NullDate":
		return v.compareNullDate(expected, actual)
	case "spanner.NullNumeric":
		return v.compareNullNumeric(expected, actual)
	case "spanner.NullJSON":
		return v.compareNullJSON(expected, actual)
	case "*big.Rat", "big.Rat":
		return v.compareBigRat(expected, actual)
	case "time.Time":
		return v.compareTimestamp(expected, actual)
	case "[]uint8":
		return v.compareBytes(expected, actual)
	case "map[string]interface{}", "map[string]interface {}":
		return v.compareJSON(expected, actual)
	case "civil.Date":
		return v.compareDate(expected, actual)
	}

	if expectedType.Kind() == reflect.Slice && actualType.Kind() == reflect.Slice {
		return v.compareSlices(expected, actual)
	}

	if v.isNumericType(expectedType) && v.isNumericType(actualType) {
		return v.compareNumericConversions(expected, actual)
	}

	if v.isIntegerType(expectedType) && v.isIntegerType(actualType) {
		return v.compareIntegerConversions(expected, actual)
	}

	if expectedType == actualType {
		return reflect.DeepEqual(expected, actual)
	}

	expectedStr := fmt.Sprintf("%v", expected)
	actualStr := fmt.Sprintf("%v", actual)
	return expectedStr == actualStr
}

func (v *Validator) compareBigRat(expected, actual interface{}) bool {
	var actualRat *big.Rat
	if rat, ok := actual.(*big.Rat); ok {
		actualRat = rat
	} else if rat, ok := actual.(big.Rat); ok {
		actualRat = &rat
	} else {
		return false
	}

	var expectedRat *big.Rat
	switch exp := expected.(type) {
	case string:
		var ok bool
		expectedRat, ok = new(big.Rat).SetString(exp)
		if !ok {
			return false
		}
	case float64:
		expectedRat = new(big.Rat).SetFloat64(exp)
	case int64:
		expectedRat = new(big.Rat).SetInt64(exp)
	case int:
		expectedRat = new(big.Rat).SetInt64(int64(exp))
	case *big.Rat:
		expectedRat = exp
	case big.Rat:
		expectedRat = &exp
	default:
		expectedStr := fmt.Sprintf("%v", exp)
		var ok bool
		expectedRat, ok = new(big.Rat).SetString(expectedStr)
		if !ok {
			return false
		}
	}

	if actualRat.Cmp(expectedRat) == 0 {
		return true
	}

	if v.options.FloatTolerance > 0 {
		diff := new(big.Rat).Sub(actualRat, expectedRat)
		diff.Abs(diff)
		tolerance := new(big.Rat).SetFloat64(v.options.FloatTolerance)
		return diff.Cmp(tolerance) < 0
	}

	return false
}

func (v *Validator) compareTimestamp(expected, actual interface{}) bool {
	actualTime, ok := actual.(time.Time)
	if !ok {
		return false
	}

	expectedStr := fmt.Sprintf("%v", expected)

	var expectedTime time.Time
	var err error

	timeFormats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range timeFormats {
		if expectedTime, err = time.Parse(format, expectedStr); err == nil {
			break
		}
	}

	if err != nil {
		return false
	}

	if v.options.TimestampTruncateTo > 0 {
		actualTime = actualTime.Truncate(v.options.TimestampTruncateTo)
		expectedTime = expectedTime.Truncate(v.options.TimestampTruncateTo)
	}

	return actualTime.Equal(expectedTime)
}

func (v *Validator) compareBytes(expected, actual interface{}) bool {
	actualBytes, ok := actual.([]byte)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case []byte:
		return reflect.DeepEqual(actualBytes, exp)
	case string:
		if exp == "" && len(actualBytes) == 0 {
			return true
		}
		if expectedBytes, err := base64.StdEncoding.DecodeString(exp); err == nil {
			return reflect.DeepEqual(actualBytes, expectedBytes)
		}
		actualStr := base64.StdEncoding.EncodeToString(actualBytes)
		return exp == actualStr
	default:
		return false
	}
}

func (v *Validator) compareJSON(expected, actual interface{}) bool {
	actualMap, ok := actual.(map[string]interface{})
	if !ok {
		return false
	}

	var expectedMap map[string]interface{}
	switch exp := expected.(type) {
	case map[string]interface{}:
		expectedMap = exp
	case string:
		if err := json.Unmarshal([]byte(exp), &expectedMap); err != nil {
			actualJSON, err := json.Marshal(actualMap)
			if err != nil {
				return false
			}
			return exp == string(actualJSON)
		}
	default:
		expectedBytes, err := json.Marshal(exp)
		if err != nil {
			return false
		}
		if err := json.Unmarshal(expectedBytes, &expectedMap); err != nil {
			return false
		}
	}

	return reflect.DeepEqual(actualMap, expectedMap)
}

func (v *Validator) compareSlices(expected, actual interface{}) bool {
	expectedSlice := reflect.ValueOf(expected)
	actualSlice := reflect.ValueOf(actual)

	if expectedSlice.Len() != actualSlice.Len() {
		return false
	}

	for i := 0; i < expectedSlice.Len(); i++ {
		if !v.compareValues(expectedSlice.Index(i).Interface(), actualSlice.Index(i).Interface()) {
			return false
		}
	}
	return true
}

func (v *Validator) compareNumericConversions(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	var expectedFloat, actualFloat float64

	switch expectedVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		expectedFloat = float64(expectedVal.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		expectedFloat = float64(expectedVal.Uint())
	case reflect.Float32, reflect.Float64:
		expectedFloat = expectedVal.Float()
	default:
		return false
	}

	switch actualVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		actualFloat = float64(actualVal.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		actualFloat = float64(actualVal.Uint())
	case reflect.Float32, reflect.Float64:
		actualFloat = actualVal.Float()
	default:
		return false
	}

	if v.options.FloatTolerance > 0 {
		return math.Abs(expectedFloat-actualFloat) <= v.options.FloatTolerance
	}

	return expectedFloat == actualFloat
}

func (v *Validator) isNumericType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func (v *Validator) isIntegerType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func (v *Validator) compareIntegerConversions(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	var expectedInt, actualInt int64

	switch expectedVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		expectedInt = expectedVal.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		expectedInt = int64(expectedVal.Uint())
	default:
		return false
	}

	switch actualVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		actualInt = actualVal.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		actualInt = int64(actualVal.Uint())
	default:
		return false
	}

	return expectedInt == actualInt
}

func (v *Validator) compareNullString(expected, actual interface{}) bool {
	actualNull, ok := actual.(spanner.NullString)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case nil:
		return !actualNull.Valid
	case string:
		return actualNull.Valid && actualNull.StringVal == exp
	case spanner.NullString:
		if !exp.Valid && !actualNull.Valid {
			return true
		}
		return exp.Valid == actualNull.Valid && exp.StringVal == actualNull.StringVal
	default:
		if !actualNull.Valid {
			return false
		}
		expectedStr := fmt.Sprintf("%v", exp)
		return actualNull.StringVal == expectedStr
	}
}

func (v *Validator) compareNullInt64(expected, actual interface{}) bool {
	actualNull, ok := actual.(spanner.NullInt64)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case nil:
		return !actualNull.Valid
	case int, int64:
		if !actualNull.Valid {
			return false
		}
		val := reflect.ValueOf(exp)
		return actualNull.Int64 == val.Convert(reflect.TypeOf(int64(0))).Int()
	case spanner.NullInt64:
		if !exp.Valid && !actualNull.Valid {
			return true
		}
		return exp.Valid == actualNull.Valid && exp.Int64 == actualNull.Int64
	default:
		return false
	}
}

func (v *Validator) compareNullBool(expected, actual interface{}) bool {
	actualNull, ok := actual.(spanner.NullBool)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case nil:
		return !actualNull.Valid
	case bool:
		return actualNull.Valid && actualNull.Bool == exp
	case spanner.NullBool:
		if !exp.Valid && !actualNull.Valid {
			return true
		}
		return exp.Valid == actualNull.Valid && exp.Bool == actualNull.Bool
	default:
		return false
	}
}

func (v *Validator) compareNullFloat64(expected, actual interface{}) bool {
	actualNull, ok := actual.(spanner.NullFloat64)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case nil:
		return !actualNull.Valid
	case float32, float64, int, int64:
		if !actualNull.Valid {
			return false
		}
		val := reflect.ValueOf(exp)
		expectedFloat := val.Convert(reflect.TypeOf(float64(0))).Float()
		if v.options.FloatTolerance > 0 {
			return math.Abs(actualNull.Float64-expectedFloat) <= v.options.FloatTolerance
		}
		return actualNull.Float64 == expectedFloat
	case spanner.NullFloat64:
		if !exp.Valid && !actualNull.Valid {
			return true
		}
		if exp.Valid != actualNull.Valid {
			return false
		}
		if v.options.FloatTolerance > 0 {
			return math.Abs(exp.Float64-actualNull.Float64) <= v.options.FloatTolerance
		}
		return exp.Float64 == actualNull.Float64
	default:
		return false
	}
}

func (v *Validator) compareNullTime(expected, actual interface{}) bool {
	actualNull, ok := actual.(spanner.NullTime)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case nil:
		return !actualNull.Valid
	case string:
		if !actualNull.Valid {
			return false
		}
		return v.compareTimestamp(exp, actualNull.Time)
	case time.Time:
		if !actualNull.Valid {
			return false
		}
		if v.options.TimestampTruncateTo > 0 {
			actualTime := actualNull.Time.Truncate(v.options.TimestampTruncateTo)
			expectedTime := exp.Truncate(v.options.TimestampTruncateTo)
			return actualTime.Equal(expectedTime)
		}
		return actualNull.Time.Equal(exp)
	case spanner.NullTime:
		if !exp.Valid && !actualNull.Valid {
			return true
		}
		if exp.Valid != actualNull.Valid {
			return false
		}
		if v.options.TimestampTruncateTo > 0 {
			actualTime := actualNull.Time.Truncate(v.options.TimestampTruncateTo)
			expectedTime := exp.Time.Truncate(v.options.TimestampTruncateTo)
			return actualTime.Equal(expectedTime)
		}
		return exp.Time.Equal(actualNull.Time)
	default:
		return false
	}
}

func (v *Validator) compareNullDate(expected, actual interface{}) bool {
	actualNull, ok := actual.(spanner.NullDate)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case nil:
		return !actualNull.Valid
	case string:
		if !actualNull.Valid {
			return false
		}
		expectedDate, err := civil.ParseDate(exp)
		if err != nil {
			return false
		}
		return actualNull.Date == expectedDate
	case civil.Date:
		return actualNull.Valid && actualNull.Date == exp
	case spanner.NullDate:
		if !exp.Valid && !actualNull.Valid {
			return true
		}
		return exp.Valid == actualNull.Valid && exp.Date == actualNull.Date
	default:
		return false
	}
}

func (v *Validator) compareNullNumeric(expected, actual interface{}) bool {
	actualNull, ok := actual.(spanner.NullNumeric)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case nil:
		return !actualNull.Valid
	case spanner.NullNumeric:
		if !exp.Valid && !actualNull.Valid {
			return true
		}
		if exp.Valid != actualNull.Valid {
			return false
		}
		return v.compareBigRat(exp.Numeric, actualNull.Numeric)
	default:
		if !actualNull.Valid {
			return false
		}
		return v.compareBigRat(exp, actualNull.Numeric)
	}
}

func (v *Validator) compareNullJSON(expected, actual interface{}) bool {
	actualNull, ok := actual.(spanner.NullJSON)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case nil:
		return !actualNull.Valid
	case spanner.NullJSON:
		if !exp.Valid && !actualNull.Valid {
			return true
		}
		if exp.Valid != actualNull.Valid {
			return false
		}
		return v.compareJSON(exp.Value, actualNull.Value)
	default:
		if !actualNull.Valid {
			return false
		}
		return v.compareJSON(exp, actualNull.Value)
	}
}

func (v *Validator) compareDate(expected, actual interface{}) bool {
	actualDate, ok := actual.(civil.Date)
	if !ok {
		return false
	}

	switch exp := expected.(type) {
	case string:
		expectedDate, err := civil.ParseDate(exp)
		if err != nil {
			return false
		}
		return actualDate == expectedDate
	case civil.Date:
		return actualDate == exp
	default:
		return false
	}
}

func (v *Validator) compareStructPBValue(expected, actual interface{}) bool {
	actualPB, ok := actual.(*structpb.Value)
	if !ok {
		return false
	}
	
	// Extract the string value from structpb.Value
	actualStr := actualPB.GetStringValue()
	
	switch exp := expected.(type) {
	case string:
		return exp == actualStr
	default:
		return fmt.Sprintf("%v", expected) == actualStr
	}
}