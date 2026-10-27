// Package routing selects a destination without depending on HTTP or storage.
package routing

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/mbeka02/sylloge/internal/event"
)

type Router struct {
	defaultDestination event.Destination
	services           map[string]event.Destination
}

func New(defaultDestination event.Destination, services map[string]event.Destination) (*Router, error) {
	if !defaultDestination.Valid() {
		return nil, errors.New("default destination must be service1, service2, or service3")
	}
	rules := make(map[string]event.Destination, len(services))
	for service, destination := range services {
		if !destination.Valid() || !utf8.ValidString(service) || strings.TrimSpace(service) == "" || utf8.RuneCountInString(service) > 100 {
			return nil, errors.New("invalid service routing rule")
		}
		rules[service] = destination
	}
	return &Router{defaultDestination: defaultDestination, services: rules}, nil
}

func (r *Router) Route(service string) (destination event.Destination, defaulted bool) {
	if destination, ok := r.services[service]; ok {
		return destination, false
	}
	return r.defaultDestination, true
}
