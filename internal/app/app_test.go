package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/mbeka02/sylloge/internal/app"
	"github.com/mbeka02/sylloge/internal/config"
)

type startupLog struct{ address chan string }

func (log startupLog) Write(data []byte) (int, error) {
	var record struct {
		Message string `json:"msg"`
		Address string `json:"address"`
	}
	if json.Unmarshal(data, &record) == nil && record.Message == "listening" {
		log.address <- record.Address
	}
	return len(data), nil
}

func TestRunServesConfiguredPortAndStops(t *testing.T) {
	// Reserve an available port to choose a non-default configuration value.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(func(key string) (string, bool) { return strconv.Itoa(port), key == "PORT" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.RoutingConfigFile = filepath.Join(t.TempDir(), "routing.json")
	if err := os.WriteFile(cfg.RoutingConfigFile, []byte(`{"default_destination":"service1","services":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	address := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx, cfg, slog.New(slog.NewJSONHandler(startupLog{address}, nil))) }()
	select {
	case <-address:
	case err := <-done:
		t.Fatalf("startup failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("startup timed out")
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	client.CloseIdleConnections()
	if err != nil || response.StatusCode != 200 || string(body) != "{\"status\":\"ok\"}\n" {
		t.Fatalf("unexpected health response: status=%d body=%q err=%v", response.StatusCode, body, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown timed out")
	}
	if connection, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), time.Second); err == nil {
		connection.Close()
		t.Fatal("listener still accepts connections after shutdown")
	}
}

func TestRunReportsStartupErrors(t *testing.T) {
	cfg, err := config.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	cfg.RoutingConfigFile = filepath.Join(t.TempDir(), "missing.json")
	if err := app.Run(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("expected a missing routing configuration to fail startup")
	}
}

func TestRunReportsOccupiedPort(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cfg := startupConfig(t)
	cfg.HTTP.Port = listener.Addr().(*net.TCPAddr).Port
	if err := app.Run(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("occupied port must fail startup")
	}
}

func TestRunReportsShutdownDeadlineAndClosesConnections(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	probe.Close()
	cfg := startupConfig(t)
	cfg.HTTP.Port = port
	cfg.ShutdownTimeout = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	address := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx, cfg, slog.New(slog.NewJSONHandler(startupLog{address}, nil))) }()
	select {
	case <-address:
	case err := <-done:
		t.Fatalf("startup failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("startup timed out")
	}
	connection, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	// An incomplete request keeps this connection non-idle during shutdown.
	if _, err := io.WriteString(connection, "GET /healthz HTTP/1.1\r\nHost:"); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	client.CloseIdleConnections()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown exceeded its deadline")
	}
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection remained readable after forced closure")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("connection was not closed at the shutdown deadline")
	}
}

func startupConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	cfg.RoutingConfigFile = filepath.Join(t.TempDir(), "routing.json")
	if err := os.WriteFile(cfg.RoutingConfigFile, []byte(`{"default_destination":"service1"}`), 0600); err != nil {
		t.Fatal(err)
	}
	return cfg
}
