package storage_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/storage"
)

func sample(id string) event.Envelope {
	return event.Envelope{Version: 1, ID: id, BatchID: "batch-1", Destination: event.Service1,
		Log:        event.Log{Timestamp: time.Date(2026, 10, 26, 13, 30, 0, 123000000, time.FixedZone("EAT", 10800)), Service: "claims-api", Level: "ERROR", Message: "claim timed out"},
		ReceivedAt: "2026-10-26T10:30:01.123456Z", SourceIP: "192.0.2.9", Environment: "test"}
}

func TestSQLiteDeduplicatesIDsButRollsBackOtherConstraints(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenSQLite(t.Context(), dir, event.Service1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	first := sample("one")
	if _, err := store.Commit(t.Context(), []event.Envelope{first}); err != nil {
		t.Fatal(err)
	}
	first.Message = "a redelivery must not overwrite the original"
	result, err := store.Commit(t.Context(), []event.Envelope{first, sample("two")})
	if err != nil || result.Inserted != 1 || result.Duplicates != 1 {
		t.Fatalf("redelivery result=%+v, %v", result, err)
	}
	invalid := sample("bad")
	invalid.Level = "FATAL"
	result, err = store.Commit(t.Context(), []event.Envelope{sample("rolled-back"), invalid})
	if err == nil || result != (storage.CommitResult{}) {
		t.Fatalf("constraint failure=%+v, %v", result, err)
	}
	db := inspect(t, dir, event.Service1)
	var count int
	var message string
	if err := db.QueryRow(`SELECT count(*) FROM logs`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("partial batch persisted: count=%d, %v", count, err)
	}
	if err := db.QueryRow(`SELECT message FROM logs WHERE id='one'`).Scan(&message); err != nil || message != "claim timed out" {
		t.Fatalf("duplicate overwrote original: %q, %v", message, err)
	}
	wrong := sample("wrong")
	wrong.Destination = event.Service2
	if _, err := store.Commit(t.Context(), []event.Envelope{wrong}); err == nil {
		t.Fatal("cross-destination commit succeeded")
	}
}

func TestSQLiteBusyAttemptHonorsCancellationAndCanResume(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenSQLite(t.Context(), dir, event.Service1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	db := inspect(t, dir, event.Service1)
	if _, err := db.Exec(`BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(`ROLLBACK`)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := store.Commit(ctx, []event.Envelope{sample("busy")}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("busy commit=%v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("driver ignored attempt deadline: %s", elapsed)
	}
	if _, err := db.Exec(`ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	if result, err := store.Commit(t.Context(), []event.Envelope{sample("busy")}); err != nil || result.Inserted != 1 {
		t.Fatalf("commit after lock release=%+v, %v", result, err)
	}
}

// The output database is an approved artifact seam: Sylloge has no query
// endpoint, so independent SQL verifies its persisted contract.
func inspect(t *testing.T, dir string, destination event.Destination) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, "logs_"+string(destination)+".db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestSQLiteCreatesRequiredSchemaAndCommitsEnrichedRow(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "new", "data")
	store, err := storage.OpenSQLite(t.Context(), dir, event.Service1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	result, err := store.Commit(t.Context(), []event.Envelope{sample("event-1")})
	if err != nil || result.Inserted != 1 || result.Duplicates != 0 {
		t.Fatalf("commit = %+v, %v", result, err)
	}
	db := inspect(t, dir, event.Service1)
	var id, timestamp, service, level, message, received, ip, env string
	if err := db.QueryRow(`SELECT id,timestamp,service,level,message,received_at,source_ip,env FROM logs`).Scan(&id, &timestamp, &service, &level, &message, &received, &ip, &env); err != nil {
		t.Fatal(err)
	}
	if id != "event-1" || timestamp != "2026-10-26T10:30:00.123Z" || service != "claims-api" || level != "ERROR" || message != "claim timed out" || received != "2026-10-26T10:30:01.123456Z" || ip != "192.0.2.9" || env != "test" {
		t.Fatalf("unexpected stored row: %s %s %s %s %s %s %s %s", id, timestamp, service, level, message, received, ip, env)
	}
	var columns int
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info('logs')`).Scan(&columns); err != nil || columns != 8 {
		t.Fatalf("columns = %d, %v", columns, err)
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 1 {
		t.Fatalf("schema version = %d, %v", version, err)
	}
	var journal, runtime string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("journal = %q, %v", journal, err)
	}
	if err := db.QueryRow(`SELECT sqlite_version()`).Scan(&runtime); err != nil {
		t.Fatal(err)
	}
	t.Logf("SQLite runtime: %s", runtime)
}
