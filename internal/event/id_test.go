package event_test

import (
	"regexp"
	"testing"

	"github.com/mbeka02/sylloge/internal/event"
)

func TestNewIDUsesUUIDv4Format(t *testing.T) {
	format := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := make(map[string]bool)
	for range 32 {
		id := event.NewID()
		if !format.MatchString(id) || seen[id] {
			t.Fatalf("invalid or repeated ID: %q", id)
		}
		seen[id] = true
	}
}
