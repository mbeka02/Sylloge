package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// Exercise the real command's signals and exit status in a subprocess.
func TestCommandHelper(t *testing.T) {
	if os.Getenv("SYLLOGE_TEST_PROCESS") != "1" {
		return
	}
	main()
	os.Exit(0)
}

func TestProductionIgnoresDotenvAndHandlesSIGTERM(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(probe.Addr().(*net.TCPAddr).Port)
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("APP_ENV=development\nPORT=invalid\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "routing.json")
	if err := os.WriteFile(path, []byte(`{"default_destination":"service1"}`), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCommandHelper$")
	cmd.Dir = dir
	cmd.Env = []string{"SYLLOGE_TEST_PROCESS=1", "PORT=" + port, "ROUTING_CONFIG_FILE=" + path, "SHUTDOWN_TIMEOUT=1s"}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	started := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			var record struct {
				Message     string `json:"msg"`
				Environment string `json:"environment"`
			}
			if json.Unmarshal(scanner.Bytes(), &record) == nil && record.Message == "listening" {
				started <- record.Environment
			}
		}
		done <- cmd.Wait()
	}()
	select {
	case environment := <-started:
		if environment != "production" {
			t.Fatal("production unexpectedly loaded development values")
		}
	case err := <-done:
		t.Fatalf("command exited before startup: %v", err)
	case <-ctx.Done():
		t.Fatal("command startup timed out")
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	client.CloseIdleConnections()
	if response.StatusCode != 200 {
		t.Fatalf("health status: %d", response.StatusCode)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SIGTERM shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("command shutdown timed out")
	}
}

func TestInvalidConfigurationExitsWithoutLeakingValue(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestCommandHelper$")
	cmd.Env = []string{"SYLLOGE_TEST_PROCESS=1", "PORT=private-input-value"}
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("invalid configuration exited successfully")
	}
	if !bytes.Contains(output, []byte("PORT")) || bytes.Contains(output, []byte("private-input-value")) {
		t.Fatal("startup error must identify the setting without disclosing its value")
	}
}
