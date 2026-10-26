package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type Routing struct {
	DefaultDestination string            `json:"default_destination"`
	Services           map[string]string `json:"services"`
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
	if !validDestination(rules.DefaultDestination) {
		return Routing{}, errors.New("ROUTING_CONFIG_FILE default_destination must be service1, service2, or service3")
	}
	for service, destination := range rules.Services {
		if strings.TrimSpace(service) == "" || len([]rune(service)) > 100 || !validDestination(destination) {
			return Routing{}, errors.New("ROUTING_CONFIG_FILE contains an invalid service mapping")
		}
	}
	return rules, nil
}

func validDestination(value string) bool {
	return value == "service1" || value == "service2" || value == "service3"
}
