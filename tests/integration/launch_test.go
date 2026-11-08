//go:build compose

package integration_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mbeka02/sylloge/internal/config"
)

func composeConfig(t *testing.T, path string, overrides ...string) (map[string]string, error) {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("docker", "compose", "--project-directory", root, "-f", filepath.Join(root, "compose.yaml"), "--env-file", path, "--profile", "dev", "config", "--format", "json")
	// Isolate application inputs without modifying the test process environment.
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if key == "PORT" || key == "APP_ENV" || key == "ROUTING_CONFIG_FILE" || key == "SHUTDOWN_TIMEOUT" || key == "MAX_ACTIVE_REQUESTS" || key == "TRUSTED_PROXIES" || key == "RAW_LOGS_CAPACITY" || key == "INGEST_BATCH_SIZE" || key == "ADMISSION_TIMEOUT" || key == "DATA_DIR" || key == "DEAD_LETTER_PATH" || key == "MEMORY_QUEUE_CAPACITY" || strings.HasPrefix(key, "RATE_LIMIT") || strings.HasPrefix(key, "HTTP_") || strings.HasPrefix(key, "COMPOSE_") {
			continue
		}
		cmd.Env = append(cmd.Env, item)
	}
	cmd.Env = append(cmd.Env, overrides...)
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	} // Do not echo rendered environment values.
	var document struct {
		Services map[string]struct{ Environment map[string]string }
	}
	if err := json.Unmarshal(output, &document); err != nil {
		return nil, err
	}
	return document.Services["app"].Environment, nil
}

func TestDevelopmentStorageEnvironmentPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("DATA_DIR=/app/dev-data\nMEMORY_QUEUE_CAPACITY=12\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		overrides []string
		dir       string
		capacity  int
		invalid   bool
	}{
		{"file", nil, "/app/dev-data", 12, false},
		{"injected", []string{"DATA_DIR=/app/other-data", "MEMORY_QUEUE_CAPACITY=34"}, "/app/other-data", 34, false},
		{"empty capacity", []string{"MEMORY_QUEUE_CAPACITY="}, "", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values, err := composeConfig(t, path, tc.overrides...)
			if err != nil {
				t.Fatal("Compose storage configuration failed")
			}
			cfg, err := config.Load(func(k string) (string, bool) { v, ok := values[k]; return v, ok })
			if (err != nil) != tc.invalid {
				t.Fatal("unexpected storage validation")
			}
			if !tc.invalid && (cfg.Storage.DataDir != tc.dir || cfg.Pipeline.DeliveryCapacity != tc.capacity) {
				t.Fatal("storage precedence not preserved")
			}
		})
	}
}

func TestDevelopmentDeadLetterEnvironmentPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("DATA_DIR=/app/custom-data\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		overrides []string
		expected  string
		invalid   bool
	}{
		{"derived", nil, "/app/custom-data/logs-failed.json", false},
		{"new data directory", []string{"DATA_DIR=/app/other-data"}, "/app/other-data/logs-failed.json", false},
		{"explicit", []string{"DEAD_LETTER_PATH=/app/archive/failed.json"}, "/app/archive/failed.json", false},
		{"empty", []string{"DEAD_LETTER_PATH="}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values, err := composeConfig(t, path, tc.overrides...)
			if err != nil {
				t.Fatal("Compose dead-letter configuration failed")
			}
			cfg, err := config.Load(func(k string) (string, bool) { v, ok := values[k]; return v, ok })
			if (err != nil) != tc.invalid {
				t.Fatal("unexpected dead-letter validation")
			}
			if !tc.invalid && cfg.Storage.DeadLetterPath != tc.expected {
				t.Fatalf("dead-letter path=%q", cfg.Storage.DeadLetterPath)
			}
		})
	}
}

func TestDevelopmentEnvironmentPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("PORT=8080\nAPP_ENV=development\nRATE_LIMIT=5\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                    string
		overrides               []string
		port, environment, rate string
		invalid                 bool
	}{
		{"file", nil, "8080", "development", "5", false},
		{"injected", []string{"PORT=8081", "APP_ENV=staging", "RATE_LIMIT=7"}, "8081", "staging", "7", false},
		{"explicitly empty", []string{"APP_ENV="}, "8080", "", "5", true},
		{"explicitly empty rate", []string{"RATE_LIMIT="}, "8080", "development", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values, err := composeConfig(t, path, tc.overrides...)
			if err != nil {
				t.Fatalf("Compose launch configuration failed: %v", err)
			}
			if values["PORT"] != tc.port || values["APP_ENV"] != tc.environment || values["RATE_LIMIT"] != tc.rate {
				t.Fatal("development input precedence was not preserved")
			}
			_, err = config.Load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
			if (err != nil) != tc.invalid {
				t.Fatal("unexpected application validation result")
			}
		})
	}
}

func TestDevelopmentRequiresRequestedFile(t *testing.T) {
	if _, err := composeConfig(t, filepath.Join(t.TempDir(), "missing.env")); err == nil {
		t.Fatal("missing development environment file did not fail launch")
	}
}
