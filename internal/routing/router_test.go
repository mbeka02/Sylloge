package routing_test

import (
	"testing"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/routing"
)

func TestConfiguredRoutesAndImmutableDefault(t *testing.T) {
	rules := map[string]event.Destination{"claims-api": event.Service1}
	router, err := routing.New(event.Service3, rules)
	if err != nil {
		t.Fatal(err)
	}
	rules["claims-api"] = event.Service2
	if destination, defaulted := router.Route("claims-api"); destination != event.Service1 || defaulted {
		t.Fatal("configured route changed after construction")
	}
	if destination, defaulted := router.Route("unknown-api"); destination != event.Service3 || !defaulted {
		t.Fatal("missing default route")
	}
}

func TestRejectInvalidDestinations(t *testing.T) {
	if _, err := routing.New("other", nil); err == nil {
		t.Fatal("invalid default accepted")
	}
	if _, err := routing.New(event.Service1, map[string]event.Destination{"api": "other"}); err == nil {
		t.Fatal("invalid destination accepted")
	}
}
