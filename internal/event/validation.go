package event

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

type FieldError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func Validate(raw json.RawMessage) (Log, []FieldError) {
	if !utf8.Valid(raw) {
		return Log{}, []FieldError{{Field: "record", Code: "invalid_utf8", Message: "log must contain valid UTF-8"}}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return Log{}, []FieldError{{Field: "record", Code: "invalid_json", Message: "invalid log object"}}
	}
	var log Log
	var problems []FieldError
	var timestamp string
	for _, field := range []struct {
		name  string
		dst   *string
		limit int
	}{
		{"timestamp", &timestamp, 0}, {"service", &log.Service, 100},
		{"level", &log.Level, 0}, {"message", &log.Message, 10000},
	} {
		value, exists := fields[field.name]
		problem := FieldError{Field: field.name}
		switch {
		case !exists || string(value) == "null":
			problem.Code, problem.Message = "required", "field is required"
		case json.Unmarshal(value, field.dst) != nil:
			problem.Code, problem.Message = "invalid_type", "must be a string"
		case strings.TrimSpace(*field.dst) == "":
			problem.Code, problem.Message = "required", "must contain a non-whitespace character"
		case field.limit > 0 && utf8.RuneCountInString(*field.dst) > field.limit:
			problem.Code, problem.Message = "too_long", fmt.Sprintf("must be at most %d characters", field.limit)
		case field.name == "timestamp":
			parsed, err := time.Parse(time.RFC3339Nano, timestamp)
			if err != nil {
				problem.Code, problem.Message = "invalid_value", "must be an RFC 3339 timestamp with a timezone"
			} else {
				log.Timestamp = parsed.UTC()
			}
		case field.name == "level":
			if log.Level != "INFO" && log.Level != "WARN" && log.Level != "ERROR" && log.Level != "DEBUG" {
				problem.Code, problem.Message = "invalid_value", "must be INFO, WARN, ERROR, or DEBUG"
			}
		}
		if problem.Code != "" {
			problems = append(problems, problem)
		}
	}
	if len(problems) > 0 {
		return Log{}, problems
	}
	return log, nil
}
