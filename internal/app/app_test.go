package app_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mbeka02/sylloge/internal/app"
	"github.com/mbeka02/sylloge/internal/config"
	_ "modernc.org/sqlite"
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
	cfg.Storage.DataDir = t.TempDir()
	cfg.Storage.DeadLetterPath = filepath.Join(cfg.Storage.DataDir, "logs-failed.json")
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
	cfg.Storage.DataDir = t.TempDir()
	cfg.Storage.DeadLetterPath = filepath.Join(cfg.Storage.DataDir, "logs-failed.json")
	if err := os.WriteFile(cfg.RoutingConfigFile, []byte(`{"default_destination":"service1"}`), 0600); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestHTTPToAllSQLiteDestinationsAndFinalDrain(t *testing.T) {
	cfg := startupConfig(t)
	cfg.HTTP.Port = 0
	cfg.Environment = "integration"
	if err := os.WriteFile(cfg.RoutingConfigFile, []byte(`{"default_destination":"service3","services":{"claims-api":"service1","payments-api":"service2"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	address := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx, cfg, slog.New(slog.NewJSONHandler(startupLog{address}, nil))) }()
	var bound string
	select {
	case bound = <-address:
	case err := <-done:
		t.Fatalf("startup=%v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("startup timeout")
	}
	_, port, err := net.SplitHostPort(bound)
	if err != nil {
		t.Fatal(err)
	}
	base := "http://127.0.0.1:" + port
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("readiness=%d", response.StatusCode)
	}
	body := `[{"timestamp":"2026-10-26T13:30:00+03:00","service":"claims-api","level":"ERROR","message":"claim"},{"timestamp":"2026-10-26T10:30:00Z","service":"payments-api","level":"INFO","message":"payment"},{"timestamp":"2026-10-26T10:30:00Z","service":"unknown-api","level":"WARN","message":"default"}]`
	response, err = client.Post(base+"/logs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 202 {
		t.Fatalf("ingestion=%d", response.StatusCode)
	}
	// Cancel immediately: accepted partial batches must still reach all files.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain timeout")
	}
	for i, producer := range []string{"claims-api", "payments-api", "unknown-api"} {
		db, err := sql.Open("sqlite", filepath.Join(cfg.Storage.DataDir, "logs_service"+strconv.Itoa(i+1)+".db"))
		if err != nil {
			t.Fatal(err)
		}
		var id, timestamp, service, level, message, received, source, environment string
		rows, err := db.Query(`SELECT id,timestamp,service,level,message,received_at,source_ip,env FROM logs`)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		if !rows.Next() {
			rows.Close()
			db.Close()
			t.Fatalf("destination %d has no persisted row", i+1)
		}
		err = rows.Scan(&id, &timestamp, &service, &level, &message, &received, &source, &environment)
		if err != nil {
			t.Fatal(err)
		}
		if id == "" || timestamp != "2026-10-26T10:30:00Z" || service != producer || environment != "integration" || source != "127.0.0.1" || len(received) != 27 {
			t.Fatalf("unexpected row in destination %d: %s %s %s %s %s", i+1, id, service, environment, source, received)
		}
		if rows.Next() {
			t.Fatal("destination contains another producer")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		db.Close()
	}
}

func runningApp(t *testing.T, cfg config.Config) (string, func() error) {
	t.Helper()
	cfg.HTTP.Port = 0
	ctx, cancel := context.WithCancel(t.Context())
	address := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx, cfg, slog.New(slog.NewJSONHandler(startupLog{address}, nil))) }()
	var result error
	var once sync.Once
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown timed out")
			}
		})
		return result
	}
	t.Cleanup(func() { stop() })
	select {
	case bound := <-address:
		_, port, err := net.SplitHostPort(bound)
		if err != nil {
			t.Fatal(err)
		}
		return "http://127.0.0.1:" + port, stop
	case err := <-done:
		// Keep cleanup from waiting for an already-consumed completion.
		once.Do(func() { cancel(); result = err })
		t.Fatalf("startup=%v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("startup timed out")
	}
	return "", stop
}

func waitFor(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out: " + description)
}

func postLog(t *testing.T, client *http.Client, base, service string, count int) int {
	t.Helper()
	entries := make([]string, count)
	for i := range entries {
		entries[i] = `{"timestamp":"2026-10-26T10:30:00Z","service":"` + service + `","level":"INFO","message":"persist me"}`
	}
	response, err := client.Post(base+"/logs", "application/json", strings.NewReader("["+strings.Join(entries, ",")+"]"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func TestLiveHTTPPersistsLowVolumeAndFullBatches(t *testing.T) {
	cfg := startupConfig(t)
	base, stop := runningApp(t, cfg)
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	db, err := sql.Open("sqlite", filepath.Join(cfg.Storage.DataDir, "logs_service1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if status := postLog(t, client, base, "claims-api", 1); status != 202 {
		t.Fatalf("low-volume status=%d", status)
	}
	waitFor(t, "one-second low-volume flush", func() bool { var n int; return db.QueryRow(`SELECT count(*) FROM logs`).Scan(&n) == nil && n == 1 })
	if status := postLog(t, client, base, "claims-api", 100); status != 202 {
		t.Fatalf("full batch status=%d", status)
	}
	waitFor(t, "100-record flush", func() bool { var n int; return db.QueryRow(`SELECT count(*) FROM logs`).Scan(&n) == nil && n == 101 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestFailedDestinationBecomesUnreadyAndBackpressuresWithoutDiscarding(t *testing.T) {
	cfg := startupConfig(t)
	cfg.Pipeline.Capacity = 2
	cfg.Pipeline.BatchSize = 1
	cfg.Pipeline.DeliveryCapacity = 1
	cfg.ShutdownTimeout = 50 * time.Millisecond
	base, stop := runningApp(t, cfg)
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	db, err := sql.Open("sqlite", filepath.Join(cfg.Storage.DataDir, "logs_service1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TRIGGER reject_insert BEFORE INSERT ON logs BEGIN SELECT RAISE(FAIL,'injected write failure'); END`); err != nil {
		t.Fatal(err)
	}
	if status := postLog(t, client, base, "claims-api", 1); status != 202 {
		t.Fatalf("initial status=%d", status)
	}
	waitFor(t, "readiness after real SQLite write failure", func() bool {
		response, err := client.Get(base + "/readyz")
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == 503
	})
	response, err := client.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("failure changed liveness")
	}
	accepted := 1
	for range 8 {
		status := postLog(t, client, base, "claims-api", 1)
		if status == 503 {
			break
		}
		if status != 202 {
			t.Fatalf("unexpected status=%d", status)
		}
		accepted++
	}
	if accepted != 5 {
		t.Fatalf("bounded capacity = %d accepted, want held 1 + queued 1 + dispatch 1 + raw 2", accepted)
	}
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM logs`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("failed batch partially persisted: %d, %v", rows, err)
	}
	err = stop()
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "undurable memory records remain") {
		t.Fatalf("shutdown concealed retained events: %v", err)
	}
}

func TestUnwritableStorageFailsBeforeListening(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions; non-root runtime smoke test covers this case")
	}
	cfg := startupConfig(t)
	if err := os.Chmod(cfg.Storage.DataDir, 0500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(cfg.Storage.DataDir, 0700)
	if err := app.Run(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("unwritable storage did not fail startup")
	}
}

func TestReadinessRecoversAfterSuccessfulDatabaseRetry(t *testing.T) {
	cfg := startupConfig(t)
	base, stop := runningApp(t, cfg)
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	db, err := sql.Open("sqlite", filepath.Join(cfg.Storage.DataDir, "logs_service1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TRIGGER reject_insert BEFORE INSERT ON logs BEGIN SELECT RAISE(FAIL,'temporary write failure'); END`); err != nil {
		t.Fatal(err)
	}
	if status := postLog(t, client, base, "claims-api", 1); status != 202 {
		t.Fatalf("status=%d", status)
	}
	waitFor(t, "unhealthy retrying writer", func() bool {
		response, err := client.Get(base + "/readyz")
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == 503
	})
	if _, err := db.Exec(`DROP TRIGGER reject_insert`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "recovered writer readiness", func() bool {
		response, err := client.Get(base + "/readyz")
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == 200
	})
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM logs`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("recovered rows=%d, %v", count, err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfg.Storage.DeadLetterPath)
	if err != nil || len(data) != 0 {
		t.Fatal("recovered record was incorrectly dead-lettered")
	}
}

func TestInvalidDeadLetterPathFailsBeforeListening(t *testing.T) {
	cfg := startupConfig(t)
	cfg.Storage.DeadLetterPath = t.TempDir()
	if err := app.Run(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("directory accepted as a dead-letter file")
	}
}

func TestDeadLetterPathCannotOverwriteSQLiteDatabase(t *testing.T) {
	cfg := startupConfig(t)
	base, stop := runningApp(t, cfg)
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	if status := postLog(t, client, base, "claims-api", 1); status != 202 {
		t.Fatal(status)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(cfg.Storage.DataDir, "logs_service1.db")
	paths := map[string]string{"database": databasePath, "WAL": databasePath + "-wal", "shared memory": databasePath + "-shm"}
	for name, link := range map[string]func(string, string) error{"symlink": os.Symlink, "hard link": os.Link} {
		path := filepath.Join(t.TempDir(), "linked.json")
		if err := link(databasePath, path); err != nil {
			t.Fatal(err)
		}
		paths[name] = path
	}
	for name, path := range paths {
		t.Run(name, func(t *testing.T) {
			configured := cfg
			configured.Storage.DeadLetterPath = path
			configured.HTTP.Port = 0
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := app.Run(ctx, configured, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil || !strings.Contains(err.Error(), "DEAD_LETTER_PATH") {
				t.Fatalf("database collision should fail startup clearly: %v", err)
			}
		})
	}

	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM logs`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("dead-letter initialization damaged SQLite: count=%d, %v", count, err)
	}
}
