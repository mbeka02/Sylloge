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
		if key == "PORT" || key == "APP_ENV" || key == "ROUTING_CONFIG_FILE" || key == "SHUTDOWN_TIMEOUT" || strings.HasPrefix(key, "HTTP_") || strings.HasPrefix(key, "COMPOSE_") {
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

func TestDevelopmentEnvironmentPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("PORT=8080\nAPP_ENV=development\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name              string
		overrides         []string
		port, environment string
	}{
		{"file", nil, "8080", "development"},
		{"injected", []string{"PORT=8081", "APP_ENV=staging"}, "8081", "staging"},
		{"explicitly empty", []string{"APP_ENV="}, "8080", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values, err := composeConfig(t, path, tc.overrides...)
			if err != nil {
				t.Fatalf("Compose launch configuration failed: %v", err)
			}
			if values["PORT"] != tc.port || values["APP_ENV"] != tc.environment {
				t.Fatal("development input precedence was not preserved")
			}
			_, err = config.Load(func(key string) (string, bool) { value, ok := values[key]; return value, ok })
			if (err != nil) != (tc.environment == "") {
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
