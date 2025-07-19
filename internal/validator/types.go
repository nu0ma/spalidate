package validator

import (
	"time"
)

const (
	DefaultFloatTolerance = 1e-9
	DefaultPort          = 9010
	MaxErrorMessageLength = 100
)

type Row map[string]interface{}

type ComparisonOptions struct {
	FloatTolerance      float64
	TimestampTruncateTo time.Duration
	IgnoreJSONKeyOrder  bool
	AllowUnorderedRows  bool
}

func DefaultComparisonOptions() ComparisonOptions {
	return ComparisonOptions{
		FloatTolerance:      DefaultFloatTolerance,
		TimestampTruncateTo: 0,
		IgnoreJSONKeyOrder:  true,
		AllowUnorderedRows:  false,
	}
}

type ValidationResult struct {
	Errors   []string
	Messages []string
}

func (r *ValidationResult) HasErrors() bool {
	return len(r.Errors) > 0
}

func (r *ValidationResult) AddError(err string) {
	r.Errors = append(r.Errors, err)
}

func (r *ValidationResult) AddMessage(msg string) {
	r.Messages = append(r.Messages, msg)
}