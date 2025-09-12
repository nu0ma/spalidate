package validator

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"google.golang.org/protobuf/types/known/structpb"
)

func (v *Validator) compareValues(expected, actual interface{}) bool {
	if expected == nil && actual == nil {
		return true
	}

	// Handle nil expected with Spanner null types
	if expected == nil && actual != nil {
		return v.isNullValue(actual)
	}

	if expected == nil || actual == nil {
		return false
	}

	// Handle special types
	switch a := actual.(type) {
	case *structpb.Value:
		return v.compareStructPBValue(expected, a)
	case spanner.NullString:
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if s, ok := e.(string); ok && a.Valid {
				return a.StringVal == s
			}
			return false
		})
	case spanner.NullInt64:
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if a.Valid {
				return v.compareIntegerValue(e, a.Int64)
			}
			return false
		})
	case spanner.NullBool:
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if b, ok := e.(bool); ok && a.Valid {
				return a.Bool == b
			}
			return false
		})
	case spanner.NullFloat64:
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if a.Valid {
				return v.compareFloatValue(e, a.Float64)
			}
			return false
		})
	case spanner.NullTime:
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if a.Valid {
				return v.compareTimestamp(e, a.Time)
			}
			return false
		})
	case spanner.NullDate:
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if a.Valid {
				return v.compareDateValue(e, a.Date)
			}
			return false
		})
	case spanner.NullNumeric:
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if a.Valid {
				return v.compareBigRat(e, a.Numeric)
			}
			return false
		})
	case spanner.NullJSON:
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if a.Valid {
				return v.compareJSON(e, a.Value)
			}
			return false
		})
	case *big.Rat, big.Rat:
		return v.compareBigRat(expected, a)
	case time.Time:
		return v.compareTimestamp(expected, a)
	case []byte:
		return v.compareBytes(expected, a)
	case map[string]interface{}:
		return v.compareJSON(expected, a)
	case civil.Date:
		return v.compareDateValue(expected, a)
	}

	// Handle slices
	expectedType := reflect.TypeOf(expected)
	actualType := reflect.TypeOf(actual)
	
	if expectedType.Kind() == reflect.Slice && actualType.Kind() == reflect.Slice {
		return v.compareSlices(expected, actual)
	}

	// Handle numeric conversions
	if v.isNumericType(expectedType) && v.isNumericType(actualType) {
		return v.compareNumericConversions(expected, actual)
	}

	// Direct comparison
	if expectedType == actualType {
		return reflect.DeepEqual(expected, actual)
	}

	// Fall back to string comparison
	return fmt.Sprintf("%v", expected) == fmt.Sprintf("%v", actual)
}

func (v *Validator) isNullValue(actual interface{}) bool {
	switch a := actual.(type) {
	case spanner.NullString:
		return !a.Valid
	case spanner.NullInt64:
		return !a.Valid
	case spanner.NullBool:
		return !a.Valid
	case spanner.NullFloat64:
		return !a.Valid
	case spanner.NullTime:
		return !a.Valid
	case spanner.NullDate:
		return !a.Valid
	case spanner.NullNumeric:
		return !a.Valid
	case spanner.NullJSON:
		return !a.Valid
	}
	return false
}

func (v *Validator) compareNullType(expected, actual interface{}, validComparator func(interface{}) bool) bool {
	if expected == nil {
		return v.isNullValue(actual)
	}
	
	// Check if expected is same null type
	if reflect.TypeOf(expected) == reflect.TypeOf(actual) {
		return reflect.DeepEqual(expected, actual)
	}
	
	return validComparator(expected)
}

func (v *Validator) compareIntegerValue(expected interface{}, actualInt64 int64) bool {
	switch e := expected.(type) {
	case int:
		return int64(e) == actualInt64
	case int64:
		return e == actualInt64
	case int32:
		return int64(e) == actualInt64
	case int16:
		return int64(e) == actualInt64
	case int8:
		return int64(e) == actualInt64
	}
	return false
}

func (v *Validator) compareFloatValue(expected interface{}, actualFloat float64) bool {
	var expectedFloat float64
	switch e := expected.(type) {
	case float64:
		expectedFloat = e
	case float32:
		expectedFloat = float64(e)
	case int:
		expectedFloat = float64(e)
	case int64:
		expectedFloat = float64(e)
	default:
		return false
	}
	
	if v.options.FloatTolerance > 0 {
		return math.Abs(expectedFloat-actualFloat) <= v.options.FloatTolerance
	}
	return expectedFloat == actualFloat
}

func (v *Validator) compareBigRat(expected, actual interface{}) bool {
	var actualRat *big.Rat
	switch a := actual.(type) {
	case *big.Rat:
		actualRat = a
	case big.Rat:
		actualRat = &a
	default:
		return false
	}

	var expectedRat *big.Rat
	switch e := expected.(type) {
	case *big.Rat:
		expectedRat = e
	case big.Rat:
		expectedRat = &e
	case string:
		var ok bool
		expectedRat, ok = new(big.Rat).SetString(e)
		if !ok {
			return false
		}
	case float64:
		expectedRat = new(big.Rat).SetFloat64(e)
	case int64:
		expectedRat = new(big.Rat).SetInt64(e)
	case int:
		expectedRat = new(big.Rat).SetInt64(int64(e))
	default:
		expectedStr := fmt.Sprintf("%v", e)
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

	var expectedTime time.Time
	switch e := expected.(type) {
	case time.Time:
		expectedTime = e
	case string:
		var err error
		timeFormats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, format := range timeFormats {
			if expectedTime, err = time.Parse(format, e); err == nil {
				break
			}
		}
		if err != nil {
			return false
		}
	default:
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

	switch e := expected.(type) {
	case []byte:
		return reflect.DeepEqual(actualBytes, e)
	case string:
		if e == "" && len(actualBytes) == 0 {
			return true
		}
		if expectedBytes, err := base64.StdEncoding.DecodeString(e); err == nil {
			return reflect.DeepEqual(actualBytes, expectedBytes)
		}
		return e == base64.StdEncoding.EncodeToString(actualBytes)
	}
	return false
}

func (v *Validator) compareJSON(expected, actual interface{}) bool {
	actualMap, ok := actual.(map[string]interface{})
	if !ok {
		return false
	}

	var expectedMap map[string]interface{}
	switch e := expected.(type) {
	case map[string]interface{}:
		expectedMap = e
	case string:
		if err := json.Unmarshal([]byte(e), &expectedMap); err != nil {
			actualJSON, err := json.Marshal(actualMap)
			if err != nil {
				return false
			}
			return e == string(actualJSON)
		}
	default:
		expectedBytes, err := json.Marshal(e)
		if err != nil {
			return false
		}
		if err := json.Unmarshal(expectedBytes, &expectedMap); err != nil {
			return false
		}
	}

	return reflect.DeepEqual(actualMap, expectedMap)
}

func (v *Validator) compareDateValue(expected interface{}, actualDate civil.Date) bool {
	switch e := expected.(type) {
	case string:
		expectedDate, err := civil.ParseDate(e)
		if err != nil {
			return false
		}
		return actualDate == expectedDate
	case civil.Date:
		return actualDate == e
	}
	return false
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

	expectedFloat := v.toFloat64(expectedVal)
	actualFloat := v.toFloat64(actualVal)

	if v.options.FloatTolerance > 0 {
		return math.Abs(expectedFloat-actualFloat) <= v.options.FloatTolerance
	}
	return expectedFloat == actualFloat
}

func (v *Validator) toFloat64(val reflect.Value) float64 {
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(val.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(val.Uint())
	case reflect.Float32, reflect.Float64:
		return val.Float()
	}
	return 0
}

func (v *Validator) isNumericType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func (v *Validator) compareStructPBValue(expected, actual interface{}) bool {
	actualPB, ok := actual.(*structpb.Value)
	if !ok {
		return false
	}
	
	actualStr := actualPB.GetStringValue()
	expectedStr := fmt.Sprintf("%v", expected)
	return expectedStr == actualStr
}

// Compatibility methods for tests
func (v *Validator) compareNullString(expected, actual interface{}) bool {
	if a, ok := actual.(spanner.NullString); ok {
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if s, ok := e.(string); ok && a.Valid {
				return a.StringVal == s
			}
			return false
		})
	}
	return false
}

func (v *Validator) compareNullInt64(expected, actual interface{}) bool {
	if a, ok := actual.(spanner.NullInt64); ok {
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if a.Valid {
				return v.compareIntegerValue(e, a.Int64)
			}
			return false
		})
	}
	return false
}

func (v *Validator) compareNullBool(expected, actual interface{}) bool {
	if a, ok := actual.(spanner.NullBool); ok {
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if b, ok := e.(bool); ok && a.Valid {
				return a.Bool == b
			}
			return false
		})
	}
	return false
}

func (v *Validator) compareNullFloat64(expected, actual interface{}) bool {
	if a, ok := actual.(spanner.NullFloat64); ok {
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if a.Valid {
				return v.compareFloatValue(e, a.Float64)
			}
			return false
		})
	}
	return false
}

func (v *Validator) compareNullDate(expected, actual interface{}) bool {
	if a, ok := actual.(spanner.NullDate); ok {
		return v.compareNullType(expected, a, func(e interface{}) bool {
			if a.Valid {
				return v.compareDateValue(e, a.Date)
			}
			return false
		})
	}
	return false
}

func (v *Validator) compareDate(expected, actual interface{}) bool {
	if a, ok := actual.(civil.Date); ok {
		return v.compareDateValue(expected, a)
	}
	return false
}