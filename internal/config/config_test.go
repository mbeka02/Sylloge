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

func TestLoadPipelineAndProxySettings(t *testing.T) {
	cfg, err := config.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Pipeline.Capacity != 10000 || cfg.Pipeline.BatchSize != 50 || cfg.Pipeline.AdmissionTimeout != 100*time.Millisecond || cfg.HTTP.MaxActiveRequests != 128 || len(cfg.HTTP.TrustedProxies) != 0 {
		t.Fatal("unexpected pipeline defaults")
	}
	values := map[string]string{"RAW_LOGS_CAPACITY": "200", "INGEST_BATCH_SIZE": "25", "ADMISSION_TIMEOUT": "50ms", "MAX_ACTIVE_REQUESTS": "16", "TRUSTED_PROXIES": "10.0.0.0/8, 2001:db8::/32"}
	cfg, err = config.Load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Pipeline.Capacity != 200 || cfg.Pipeline.BatchSize != 25 || cfg.Pipeline.AdmissionTimeout != 50*time.Millisecond || cfg.HTTP.MaxActiveRequests != 16 || len(cfg.HTTP.TrustedProxies) != 2 {
		t.Fatal("pipeline overrides missing")
	}
	for key, value := range map[string]string{"RAW_LOGS_CAPACITY": "0", "INGEST_BATCH_SIZE": "-1", "ADMISSION_TIMEOUT": "101ms", "MAX_ACTIVE_REQUESTS": "0", "TRUSTED_PROXIES": "not-a-prefix"} {
		t.Run(key, func(t *testing.T) {
			_, err := config.Load(func(k string) (string, bool) { return value, k == key })
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("expected safe configuration error for %s", key)
			}
		})
	}
}

func TestLoadRateLimitSettings(t *testing.T) {
	cfg, err := config.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.RateLimit != 1000 || cfg.HTTP.MaxIPEntries != 10000 || cfg.HTTP.IPIdleTimeout != 60*time.Second {
		t.Fatal("unexpected rate-limit defaults")
	}
	values := map[string]string{"RATE_LIMIT": "25", "RATE_LIMIT_MAX_IPS": "200", "RATE_LIMIT_IDLE_TIMEOUT": "30s"}
	cfg, err = config.Load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.RateLimit != 25 || cfg.HTTP.MaxIPEntries != 200 || cfg.HTTP.IPIdleTimeout != 30*time.Second {
		t.Fatal("rate-limit overrides were not loaded")
	}
	for _, tc := range []struct{ key, value string }{
		{"RATE_LIMIT", ""}, {"RATE_LIMIT", "0"}, {"RATE_LIMIT", "-1"},
		{"RATE_LIMIT_MAX_IPS", "0"}, {"RATE_LIMIT_IDLE_TIMEOUT", "500ms"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			_, err := config.Load(func(key string) (string, bool) { return tc.value, key == tc.key })
			if err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("expected configuration error for %s", tc.key)
			}
		})
	}
}
