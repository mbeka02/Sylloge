package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/metrics"
	"github.com/mbeka02/sylloge/internal/queue"
	"github.com/mbeka02/sylloge/internal/storage"
)

type synchronizedLog struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (l *synchronizedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.data.Write(p)
}

// This deliberately uses wall time and real SQLite/files. The fast virtual-time
// tests establish exact delays; this checks the actual deployed retry schedule.
func TestRealSQLiteRetriesAndThreeDestinationDeadLetters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "logs-failed.json")
	sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	registry := new(metrics.Registry)
	q, _ := queue.NewMemory(1)
	var logs synchronizedLog
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	var writers []*storage.Writer
	for index, destination := range []event.Destination{event.Service1, event.Service2, event.Service3} {
		store, err := storage.OpenSQLite(t.Context(), dir, destination)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		db := inspect(t, dir, destination)
		if _, err := db.Exec(`CREATE TRIGGER reject_write BEFORE INSERT ON logs BEGIN SELECT RAISE(FAIL,'real SQLite write failure'); END`); err != nil {
			t.Fatal(err)
		}
		writer, err := storage.NewWriter(destination, store, storage.WriterOptions{DeadLetters: sink, Metrics: registry, Logger: logger})
		if err != nil {
			t.Fatal(err)
		}
		writers = append(writers, writer)
		record := failedRecord(fmt.Sprintf("destination-%d", index+1)).Event
		record.Destination = destination
		if err := q.Send(t.Context(), record); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	done := make(chan error, 3)
	for i, destination := range []event.Destination{event.Service1, event.Service2, event.Service3} {
		go func() { done <- writers[i].Run(ctx, q.Deliveries(destination)) }()
	}
	for range 3 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if q.Pending() != 0 || registry.Snapshot().DeadLettered != 3 || registry.Snapshot().Retries != 9 || registry.Snapshot().WriteFailures != 12 {
		t.Fatalf("recovery result: %+v pending=%d", registry.Snapshot(), q.Pending())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[event.Destination]bool)
	for _, line := range bytes.Split(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\n'}) {
		var record storage.FailedRecord
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record.Attempts != 4 || record.LastError == "" || record.Event.Destination != record.Destination {
			t.Fatal("missing terminal metadata")
		}
		seen[record.Destination] = true
	}
	if len(seen) != 3 {
		t.Fatal("terminal file missed a destination")
	}
	var previous = make(map[string]time.Time)
	var attempts = make(map[string]int)
	delays := []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}
	for _, line := range bytes.Split(bytes.TrimSpace(logs.data.Bytes()), []byte{'\n'}) {
		var record struct {
			Time    time.Time `json:"time"`
			Message string    `json:"msg"`
			ID      string    `json:"event_id"`
			Attempt int       `json:"attempt"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record.Message != "database write failed; delivery retained" {
			continue
		}
		if record.Attempt != attempts[record.ID]+1 {
			t.Fatal("real attempt sequence changed")
		}
		if record.Attempt > 1 {
			elapsed := record.Time.Sub(previous[record.ID])
			if elapsed < delays[record.Attempt-2]-50*time.Millisecond || elapsed > delays[record.Attempt-2]+3*time.Second {
				t.Fatalf("real retry %d delay=%s", record.Attempt, elapsed)
			}
		}
		previous[record.ID] = record.Time
		attempts[record.ID]++
	}
	if len(attempts) != 3 {
		t.Fatal("real attempt logs missing")
	}
	t.Log("Three SQLite writers each failed at attempts 1–4 with 1s/5s/10s delays, then synced and settled their terminal records")
}
