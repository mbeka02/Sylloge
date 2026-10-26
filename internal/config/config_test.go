package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/mbeka02/sylloge/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := config.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Port != 3000 || cfg.Environment != "production" {
		t.Fatalf("unexpected defaults: port=%d environment=%q", cfg.HTTP.Port, cfg.Environment)
	}
	if cfg.HTTP.ReadHeaderTimeout != 5*time.Second || cfg.HTTP.ReadTimeout != 10*time.Second ||
		cfg.HTTP.WriteTimeout != 10*time.Second || cfg.HTTP.IdleTimeout != 60*time.Second ||
		cfg.HTTP.MaxHeaderBytes != 16384 || cfg.ShutdownTimeout != 60*time.Second {
		t.Fatal("unexpected HTTP or shutdown limits")
	}
	if cfg.RoutingConfigFile != "configs/routing.example.json" {
		t.Fatalf("unexpected routing file: %q", cfg.RoutingConfigFile)
	}
}

func TestLoadTypedOverrides(t *testing.T) {
	values := map[string]string{
		"PORT": "8081", "APP_ENV": "staging", "ROUTING_CONFIG_FILE": "routing.json",
		"HTTP_READ_HEADER_TIMEOUT": "2s", "HTTP_READ_TIMEOUT": "3s",
		"HTTP_WRITE_TIMEOUT": "4s", "HTTP_IDLE_TIMEOUT": "30s",
		"HTTP_MAX_HEADER_BYTES": "8192", "SHUTDOWN_TIMEOUT": "15s",
	}
	cfg, err := config.Load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Port != 8081 || cfg.Environment != "staging" || cfg.RoutingConfigFile != "routing.json" ||
		cfg.HTTP.ReadHeaderTimeout != 2*time.Second || cfg.HTTP.ReadTimeout != 3*time.Second ||
		cfg.HTTP.WriteTimeout != 4*time.Second || cfg.HTTP.IdleTimeout != 30*time.Second ||
		cfg.HTTP.MaxHeaderBytes != 8192 || cfg.ShutdownTimeout != 15*time.Second {
		t.Fatal("typed overrides were not applied")
	}
}

func TestLoadRejectsInvalidValuesWithoutEchoingThem(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"PORT", ""}, {"PORT", "0"}, {"PORT", "65536"}, {"PORT", "secret-value"},
		{"APP_ENV", " "}, {"ROUTING_CONFIG_FILE", ""},
		{"HTTP_READ_HEADER_TIMEOUT", "-1s"}, {"HTTP_READ_TIMEOUT", "0s"},
		{"HTTP_WRITE_TIMEOUT", "secret-value"}, {"HTTP_IDLE_TIMEOUT", ""},
		{"HTTP_MAX_HEADER_BYTES", "0"}, {"SHUTDOWN_TIMEOUT", "0s"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			_, err := config.Load(func(key string) (string, bool) { return tc.value, key == tc.key })
			if err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("expected an error naming %s", tc.key)
			}
			if strings.Contains(err.Error(), "secret-value") {
				t.Fatal("configuration error disclosed an input value")
			}
		})
	}
}
