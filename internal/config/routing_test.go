package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mbeka02/sylloge/internal/config"
)

func TestLoadRouting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routing.json")
	if err := os.WriteFile(path, []byte(`{"default_destination":"service3","services":{"claims-api":"service1"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	rules, err := config.LoadRouting(path)
	if err != nil {
		t.Fatal(err)
	}
	if rules.DefaultDestination != "service3" || rules.Services["claims-api"] != "service1" {
		t.Fatal("routing rules were not loaded")
	}
}

func TestLoadRoutingRejectsInvalidRules(t *testing.T) {
	for name, body := range map[string]string{
		"invalid JSON":        `{`,
		"null":                `null`,
		"missing default":     `{"services":{}}`,
		"unknown default":     `{"default_destination":"other"}`,
		"unknown destination": `{"default_destination":"service1","services":{"api":"other"}}`,
		"empty service":       `{"default_destination":"service1","services":{" ":"service1"}}`,
		"unknown field":       `{"default_destination":"service1","password":"private"}`,
		"trailing document":   `{"default_destination":"service1"} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "routing.json")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := config.LoadRouting(path); err == nil {
				t.Fatal("expected invalid routing to be rejected")
			}
		})
	}
}
