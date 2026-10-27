package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/routing"
)

type Routing struct {
	DefaultDestination event.Destination            `json:"default_destination"`
	Services           map[string]event.Destination `json:"services"`
}

// LoadRouting validates configuration only; routing events belongs to the pipeline.
func LoadRouting(path string) (Routing, error) {
	file, err := os.Open(path)
	if err != nil {
		return Routing{}, fmt.Errorf("open ROUTING_CONFIG_FILE: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var rules Routing
	if err := decoder.Decode(&rules); err != nil {
		return Routing{}, errors.New("ROUTING_CONFIG_FILE must contain a routing JSON object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Routing{}, errors.New("ROUTING_CONFIG_FILE must contain exactly one JSON object")
	}
	if _, err := routing.New(rules.DefaultDestination, rules.Services); err != nil {
		return Routing{}, fmt.Errorf("ROUTING_CONFIG_FILE: %w", err)
	}
	return rules, nil
}
