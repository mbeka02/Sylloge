package event_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
)

func TestValidateNormalizesTimeAndPreservesText(t *testing.T) {
	log, problems := event.Validate([]byte(`{"timestamp":"2026-10-27T12:00:00.123456+03:00","service":" claims-api ","level":"ERROR","message":" timeout "}`))
	if len(problems) != 0 {
		t.Fatalf("unexpected validation errors: %v", problems)
	}
	if log.Timestamp.Format(time.RFC3339Nano) != "2026-10-27T09:00:00.123456Z" || log.Service != " claims-api " || log.Message != " timeout " || log.Level != "ERROR" {
		t.Fatalf("unexpected validated record: %+v", log)
	}
}

func TestValidationReportsEachFieldIndependently(t *testing.T) {
	_, problems := event.Validate([]byte(`{"timestamp":false,"service":null,"level":"TRACE","message":" "}`))
	if len(problems) != 4 {
		t.Fatalf("want four field errors, got %v", problems)
	}
	for i, field := range []string{"timestamp", "service", "level", "message"} {
		if problems[i].Field != field || problems[i].Code == "" || problems[i].Message == "" {
			t.Fatalf("missing field context: %v", problems)
		}
	}
}

func TestValidationBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		value       any
		valid       bool
	}{
		{"service limit", "service", strings.Repeat("界", 100), true},
		{"service too long", "service", strings.Repeat("界", 101), false},
		{"message limit", "message", strings.Repeat("界", 10000), true},
		{"message too long", "message", strings.Repeat("界", 10001), false},
		{"empty service", "service", "", false}, {"blank message", "message", "\n\t", false},
		{"integer message", "message", 3, false}, {"missing timezone", "timestamp", "2026-10-27T12:00:00", false},
		{"debug", "level", "DEBUG", true}, {"warn", "level", "WARN", true},
		{"lowercase", "level", "info", false}, {"null", "level", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := map[string]any{"timestamp": "2026-10-27T09:00:00Z", "service": "claims-api", "level": "INFO", "message": "ok"}
			values[tc.field] = tc.value
			raw, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			_, problems := event.Validate(raw)
			if (len(problems) == 0) != tc.valid {
				t.Fatalf("unexpected errors: %v", problems)
			}
			if !tc.valid && (len(problems) != 1 || problems[0].Field != tc.field) {
				t.Fatalf("incorrect field context: %v", problems)
			}
		})
	}
}

func TestValidationRejectsMissingFieldsAndNonObjects(t *testing.T) {
	_, problems := event.Validate([]byte(`{}`))
	if len(problems) != 4 {
		t.Fatalf("missing required fields: %v", problems)
	}
	for _, raw := range []string{`null`, `[]`, `123`, `"text"`, `{`} {
		if _, problems := event.Validate([]byte(raw)); len(problems) == 0 {
			t.Fatalf("accepted non-object %s", raw)
		}
	}
	raw := []byte(`{"timestamp":"2026-10-27T09:00:00Z","service":"api","level":"INFO","message":"`)
	raw = append(raw, 0xff)
	raw = append(raw, []byte(`"}`)...)
	if _, problems := event.Validate(raw); len(problems) == 0 {
		t.Fatal("invalid UTF-8 accepted")
	}
}
