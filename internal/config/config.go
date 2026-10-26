// Package config loads and validates the application's startup configuration.
// Development launch tooling prepares the environment; this package never reads .env.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type HTTP struct {
	Port              int
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
}

type Config struct {
	Environment       string
	HTTP              HTTP
	RoutingConfigFile string
	ShutdownTimeout   time.Duration
}

// Load reads each known setting once. An explicitly empty value is invalid,
// rather than falling back to a default. Errors name settings, never their values.
func Load(lookup func(string) (string, bool)) (Config, error) {
	cfg := Config{
		Environment: "production",
		HTTP: HTTP{
			Port: 3000, ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
			IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384,
		},
		RoutingConfigFile: "configs/routing.example.json",
		ShutdownTimeout:   60 * time.Second,
	}
	for _, setting := range []struct {
		key string
		dst *string
	}{
		{"APP_ENV", &cfg.Environment},
		{"ROUTING_CONFIG_FILE", &cfg.RoutingConfigFile},
	} {
		if value, ok := lookup(setting.key); ok {
			if strings.TrimSpace(value) == "" {
				return Config{}, fmt.Errorf("%s must not be empty", setting.key)
			}
			*setting.dst = value
		}
	}
	for _, setting := range []struct {
		key string
		dst *int
		max int
	}{
		{"PORT", &cfg.HTTP.Port, 65535},
		{"HTTP_MAX_HEADER_BYTES", &cfg.HTTP.MaxHeaderBytes, 1 << 20},
	} {
		if value, ok := lookup(setting.key); ok {
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > setting.max {
				return Config{}, fmt.Errorf("%s must be an integer between 1 and %d", setting.key, setting.max)
			}
			*setting.dst = n
		}
	}
	for _, setting := range []struct {
		key string
		dst *time.Duration
	}{
		{"HTTP_READ_HEADER_TIMEOUT", &cfg.HTTP.ReadHeaderTimeout},
		{"HTTP_READ_TIMEOUT", &cfg.HTTP.ReadTimeout},
		{"HTTP_WRITE_TIMEOUT", &cfg.HTTP.WriteTimeout},
		{"HTTP_IDLE_TIMEOUT", &cfg.HTTP.IdleTimeout},
		{"SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout},
	} {
		if value, ok := lookup(setting.key); ok {
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 {
				return Config{}, fmt.Errorf("%s must be a positive duration", setting.key)
			}
			*setting.dst = d
		}
	}
	return cfg, nil
}
