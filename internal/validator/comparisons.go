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

// TypeHandler defines the interface for handling type-specific comparisons
type TypeHandler interface {
	// CanHandle returns true if this handler can process the given type
	CanHandle(actual interface{}) bool
	
	// Compare compares expected and actual values
	Compare(expected, actual interface{}, options ComparisonOptions) bool
	
	// Format formats a value for error messages
	Format(value interface{}) string
}

// TypeRegistry manages type handlers
type TypeRegistry struct {
	handlers []TypeHandler
}

// NewTypeRegistry creates a new type registry with default handlers
func NewTypeRegistry() *TypeRegistry {
	return &TypeRegistry{
		handlers: []TypeHandler{
			&NullTypeHandler{},
			&NumericHandler{},
			&TimestampHandler{},
			&BytesHandler{},
			&JSONHandler{},
			&DateHandler{},
			&SliceHandler{},
			&StructPBHandler{},
		},
	}
}

// Compare finds an appropriate handler and compares values
func (r *TypeRegistry) Compare(expected, actual interface{}, options ComparisonOptions) bool {
	// Handle nil cases
	if expected == nil && actual == nil {
		return true
	}
	
	// Find handler for actual type
	for _, handler := range r.handlers {
		if handler.CanHandle(actual) {
			return handler.Compare(expected, actual, options)
		}
	}
	
	// Fallback to direct comparison
	return r.fallbackCompare(expected, actual, options)
}

// Format finds an appropriate handler and formats the value
func (r *TypeRegistry) Format(value interface{}) string {
	if value == nil {
		return "null"
	}
	
	// Handle string type specifically
	if s, ok := value.(string); ok {
		if len(s) > MaxErrorMessageLength {
			return fmt.Sprintf("%.100s...", s)
		}
		return fmt.Sprintf("%q", s)
	}
	
	for _, handler := range r.handlers {
		if handler.CanHandle(value) {
			return handler.Format(value)
		}
	}
	
	// Default formatting
	str := fmt.Sprintf("%v", value)
	if len(str) > MaxErrorMessageLength {
		return fmt.Sprintf("%.100s...", str)
	}
	return str
}

func (r *TypeRegistry) fallbackCompare(expected, actual interface{}, options ComparisonOptions) bool {
	expectedType := reflect.TypeOf(expected)
	actualType := reflect.TypeOf(actual)
	
	// Handle numeric conversions
	if isNumericType(expectedType) && isNumericType(actualType) {
		return compareNumericValues(expected, actual, options.FloatTolerance)
	}
	
	// Direct comparison
	if expectedType == actualType {
		return reflect.DeepEqual(expected, actual)
	}
	
	// String comparison fallback
	return fmt.Sprintf("%v", expected) == fmt.Sprintf("%v", actual)
}

// NullTypeHandler handles Spanner null types
type NullTypeHandler struct{}

func (h *NullTypeHandler) CanHandle(actual interface{}) bool {
	switch actual.(type) {
	case spanner.NullString, spanner.NullInt64, spanner.NullBool, 
	     spanner.NullFloat64, spanner.NullTime, spanner.NullDate, 
	     spanner.NullNumeric, spanner.NullJSON:
		return true
	}
	return false
}

func (h *NullTypeHandler) Compare(expected, actual interface{}, options ComparisonOptions) bool {
	// Handle nil expected
	if expected == nil {
		return !isValidNullType(actual)
	}
	
	// Check if expected is same null type
	if reflect.TypeOf(expected) == reflect.TypeOf(actual) {
		return reflect.DeepEqual(expected, actual)
	}
	
	switch a := actual.(type) {
	case spanner.NullString:
		if !a.Valid {
			return expected == nil
		}
		if s, ok := expected.(string); ok {
			return a.StringVal == s
		}
		
	case spanner.NullInt64:
		if !a.Valid {
			return expected == nil
		}
		return compareIntegerValue(expected, a.Int64)
		
	case spanner.NullBool:
		if !a.Valid {
			return expected == nil
		}
		if b, ok := expected.(bool); ok {
			return a.Bool == b
		}
		
	case spanner.NullFloat64:
		if !a.Valid {
			return expected == nil
		}
		return compareFloatValue(expected, a.Float64, options.FloatTolerance)
		
	case spanner.NullTime:
		if !a.Valid {
			return expected == nil
		}
		return compareTimeValue(expected, a.Time, options.TimestampTruncateTo)
		
	case spanner.NullDate:
		if !a.Valid {
			return expected == nil
		}
		return compareDateValue(expected, a.Date)
		
	case spanner.NullNumeric:
		if !a.Valid {
			return expected == nil
		}
		return compareBigRatValue(expected, a.Numeric, options.FloatTolerance)
		
	case spanner.NullJSON:
		if !a.Valid {
			return expected == nil
		}
		return compareJSONValue(expected, a.Value, options.IgnoreJSONKeyOrder)
	}
	
	return false
}

func (h *NullTypeHandler) Format(value interface{}) string {
	switch v := value.(type) {
	case spanner.NullString:
		if !v.Valid {
			return "null"
		}
		return fmt.Sprintf("%q", v.StringVal)
	case spanner.NullInt64:
		if !v.Valid {
			return "null"
		}
		return fmt.Sprintf("%d", v.Int64)
	case spanner.NullBool:
		if !v.Valid {
			return "null"
		}
		return fmt.Sprintf("%t", v.Bool)
	case spanner.NullFloat64:
		if !v.Valid {
			return "null"
		}
		return fmt.Sprintf("%g", v.Float64)
	case spanner.NullTime:
		if !v.Valid {
			return "null"
		}
		return v.Time.Format(time.RFC3339Nano)
	case spanner.NullDate:
		if !v.Valid {
			return "null"
		}
		return v.Date.String()
	case spanner.NullNumeric:
		if !v.Valid {
			return "null"
		}
		return v.Numeric.FloatString(6)
	case spanner.NullJSON:
		if !v.Valid {
			return "null"
		}
		data, _ := json.Marshal(v.Value)
		return string(data)
	}
	return fmt.Sprintf("%v", value)
}

// NumericHandler handles big.Rat and numeric types
type NumericHandler struct{}

func (h *NumericHandler) CanHandle(actual interface{}) bool {
	switch actual.(type) {
	case *big.Rat, big.Rat:
		return true
	}
	return false
}

func (h *NumericHandler) Compare(expected, actual interface{}, options ComparisonOptions) bool {
	return compareBigRatValue(expected, actual, options.FloatTolerance)
}

func (h *NumericHandler) Format(value interface{}) string {
	switch v := value.(type) {
	case *big.Rat:
		return v.FloatString(6)
	case big.Rat:
		return v.FloatString(6)
	}
	return fmt.Sprintf("%v", value)
}

// TimestampHandler handles time.Time values
type TimestampHandler struct{}

func (h *TimestampHandler) CanHandle(actual interface{}) bool {
	_, ok := actual.(time.Time)
	return ok
}

func (h *TimestampHandler) Compare(expected, actual interface{}, options ComparisonOptions) bool {
	actualTime, ok := actual.(time.Time)
	if !ok {
		return false
	}
	return compareTimeValue(expected, actualTime, options.TimestampTruncateTo)
}

func (h *TimestampHandler) Format(value interface{}) string {
	if t, ok := value.(time.Time); ok {
		return t.Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("%v", value)
}

// BytesHandler handles []byte values
type BytesHandler struct{}

func (h *BytesHandler) CanHandle(actual interface{}) bool {
	_, ok := actual.([]byte)
	return ok
}

func (h *BytesHandler) Compare(expected, actual interface{}, options ComparisonOptions) bool {
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

func (h *BytesHandler) Format(value interface{}) string {
	if b, ok := value.([]byte); ok {
		return base64.StdEncoding.EncodeToString(b)
	}
	return fmt.Sprintf("%v", value)
}

// JSONHandler handles JSON/map values
type JSONHandler struct{}

func (h *JSONHandler) CanHandle(actual interface{}) bool {
	_, ok := actual.(map[string]interface{})
	return ok
}

func (h *JSONHandler) Compare(expected, actual interface{}, options ComparisonOptions) bool {
	actualMap, ok := actual.(map[string]interface{})
	if !ok {
		return false
	}
	return compareJSONValue(expected, actualMap, options.IgnoreJSONKeyOrder)
}

func (h *JSONHandler) Format(value interface{}) string {
	if m, ok := value.(map[string]interface{}); ok {
		data, _ := json.Marshal(m)
		if len(data) > MaxErrorMessageLength {
			return fmt.Sprintf("%.100s...", string(data))
		}
		return string(data)
	}
	return fmt.Sprintf("%v", value)
}

// DateHandler handles civil.Date values
type DateHandler struct{}

func (h *DateHandler) CanHandle(actual interface{}) bool {
	_, ok := actual.(civil.Date)
	return ok
}

func (h *DateHandler) Compare(expected, actual interface{}, options ComparisonOptions) bool {
	actualDate, ok := actual.(civil.Date)
	if !ok {
		return false
	}
	return compareDateValue(expected, actualDate)
}

func (h *DateHandler) Format(value interface{}) string {
	if d, ok := value.(civil.Date); ok {
		return d.String()
	}
	return fmt.Sprintf("%v", value)
}

// SliceHandler handles slice comparisons
type SliceHandler struct{}

func (h *SliceHandler) CanHandle(actual interface{}) bool {
	if actual == nil {
		return false
	}
	return reflect.TypeOf(actual).Kind() == reflect.Slice
}

func (h *SliceHandler) Compare(expected, actual interface{}, options ComparisonOptions) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)
	
	if expectedVal.Kind() != reflect.Slice || actualVal.Kind() != reflect.Slice {
		return false
	}
	
	if expectedVal.Len() != actualVal.Len() {
		return false
	}
	
	registry := NewTypeRegistry()
	for i := 0; i < expectedVal.Len(); i++ {
		if !registry.Compare(expectedVal.Index(i).Interface(), actualVal.Index(i).Interface(), options) {
			return false
		}
	}
	return true
}

func (h *SliceHandler) Format(value interface{}) string {
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice {
		return fmt.Sprintf("%v", value)
	}
	
	if v.Len() > 10 {
		return fmt.Sprintf("[%d items]", v.Len())
	}
	
	var parts []string
	for i := 0; i < v.Len(); i++ {
		parts = append(parts, fmt.Sprintf("%v", v.Index(i).Interface()))
	}
	return fmt.Sprintf("[%s]", strings.Join(parts, ", "))
}

// StructPBHandler handles structpb.Value
type StructPBHandler struct{}

func (h *StructPBHandler) CanHandle(actual interface{}) bool {
	_, ok := actual.(*structpb.Value)
	return ok
}

func (h *StructPBHandler) Compare(expected, actual interface{}, options ComparisonOptions) bool {
	actualPB, ok := actual.(*structpb.Value)
	if !ok {
		return false
	}
	
	actualStr := actualPB.GetStringValue()
	expectedStr := fmt.Sprintf("%v", expected)
	return expectedStr == actualStr
}

func (h *StructPBHandler) Format(value interface{}) string {
	if v, ok := value.(*structpb.Value); ok {
		return fmt.Sprintf("%q", v.GetStringValue())
	}
	return fmt.Sprintf("%v", value)
}

// Helper functions

func isValidNullType(v interface{}) bool {
	switch val := v.(type) {
	case spanner.NullString:
		return val.Valid
	case spanner.NullInt64:
		return val.Valid
	case spanner.NullBool:
		return val.Valid
	case spanner.NullFloat64:
		return val.Valid
	case spanner.NullTime:
		return val.Valid
	case spanner.NullDate:
		return val.Valid
	case spanner.NullNumeric:
		return val.Valid
	case spanner.NullJSON:
		return val.Valid
	}
	return false
}

func compareIntegerValue(expected interface{}, actualInt64 int64) bool {
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
	case float64:
		return int64(e) == actualInt64
	case float32:
		return int64(e) == actualInt64
	}
	return false
}

func compareFloatValue(expected interface{}, actualFloat float64, tolerance float64) bool {
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
	
	if tolerance > 0 {
		return math.Abs(expectedFloat-actualFloat) <= tolerance
	}
	return expectedFloat == actualFloat
}

func compareBigRatValue(expected, actual interface{}, tolerance float64) bool {
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
	
	if tolerance > 0 {
		diff := new(big.Rat).Sub(actualRat, expectedRat)
		diff.Abs(diff)
		toleranceRat := new(big.Rat).SetFloat64(tolerance)
		return diff.Cmp(toleranceRat) < 0
	}
	
	return false
}

func compareTimeValue(expected interface{}, actualTime time.Time, truncateTo time.Duration) bool {
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
	
	if truncateTo > 0 {
		actualTime = actualTime.Truncate(truncateTo)
		expectedTime = expectedTime.Truncate(truncateTo)
	}
	
	return actualTime.Equal(expectedTime)
}

func compareDateValue(expected interface{}, actualDate civil.Date) bool {
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

func compareJSONValue(expected, actual interface{}, ignoreKeyOrder bool) bool {
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

func compareNumericValues(expected, actual interface{}, tolerance float64) bool {
	expectedFloat := toFloat64(expected)
	actualFloat := toFloat64(actual)
	
	if tolerance > 0 {
		return math.Abs(expectedFloat-actualFloat) <= tolerance
	}
	return expectedFloat == actualFloat
}

func toFloat64(val interface{}) float64 {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return v.Float()
	}
	return 0
}

func isNumericType(t reflect.Type) bool {
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		 reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		 reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

