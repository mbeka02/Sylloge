package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/metrics"
	"github.com/mbeka02/sylloge/internal/queue"
	"github.com/mbeka02/sylloge/internal/storage"
)

// A controlled external-storage adapter isolates the flush clock. Real-file
// transaction tests and application tests establish actual SQLite durability.
type batchStore func(context.Context, []event.Envelope) (storage.CommitResult, error)

func (f batchStore) Commit(ctx context.Context, records []event.Envelope) (storage.CommitResult, error) {
	return f(ctx, records)
}

func TestWriterExhaustionSettlesOnlyAfterSyncedDeadLetters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "logs-failed.json")
		filesystem := &faultFS{path: path}
		sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{FileSystem: filesystem})
		if err != nil {
			t.Fatal(err)
		}
		defer sink.Close()
		filesystem.partial.Store(true)
		q, _ := queue.NewMemory(2)
		for _, id := range []string{"terminal-1", "terminal-2"} {
			record := failedRecord(id).Event
			if err := q.Send(t.Context(), record); err != nil {
				t.Fatal(err)
			}
		}
		q.Close()
		registry := new(metrics.Registry)
		var logs bytes.Buffer
		attempts := 0
		writer, err := storage.NewWriter(event.Service1, batchStore(func(context.Context, []event.Envelope) (storage.CommitResult, error) {
			attempts++
			return storage.CommitResult{}, fmt.Errorf("disk failure %d", attempts)
		}), storage.WriterOptions{DeadLetters: sink, Metrics: registry, Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- writer.Run(t.Context(), q.Deliveries(event.Service1)) }()
		synctest.Wait()
		time.Sleep(16 * time.Second)
		synctest.Wait()
		if q.Pending() != 2 || writer.Pending() != 2 || writer.Healthy() || registry.Snapshot().DeadLettered != 0 {
			t.Fatal("unsynced terminal record was settled or marked healthy")
		}
		filesystem.partial.Store(false)
		time.Sleep(time.Second)
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if q.Pending() != 0 || !writer.Healthy() || registry.Snapshot().DeadLettered != 2 || registry.Snapshot().Retries != 6 || registry.Snapshot().DeadLetterFailures != 1 {
			t.Fatalf("terminal result=%+v pending=%d", registry.Snapshot(), q.Pending())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\n'})
		if len(lines) != 2 {
			t.Fatalf("dead letters=%d", len(lines))
		}
		for i, line := range lines {
			var record storage.FailedRecord
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatal(err)
			}
			if record.ID != fmt.Sprintf("terminal-%d", i+1) || record.Attempts != 4 || record.LastError != "disk failure 4" || len(record.Original) == 0 || record.FailedAt.IsZero() {
				t.Fatalf("terminal metadata=%+v", record)
			}
		}
		var attemptLogs int
		for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte{'\n'}) {
			var record struct {
				Message string `json:"msg"`
				ID      string `json:"event_id"`
				Attempt int    `json:"attempt"`
			}
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatal(err)
			}
			if record.Message == "database write failed; delivery retained" {
				if record.ID == "" || record.Attempt != attemptLogs/2+1 {
					t.Fatal("per-record attempt log missing or wrong")
				}
				attemptLogs++
			}
		}
		if attemptLogs != 8 {
			t.Fatalf("per-record attempts=%d", attemptLogs)
		}
	})
}

func TestWriterResumesTerminalHandlingWithoutRepeatingHandledRecords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "logs-failed.json")
		filesystem := &faultFS{path: path, partialID: "second"}
		sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{FileSystem: filesystem})
		if err != nil {
			t.Fatal(err)
		}
		defer sink.Close()
		q, _ := queue.NewMemory(2)
		q.Send(t.Context(), failedRecord("first").Event)
		q.Send(t.Context(), failedRecord("second").Event)
		q.Close()
		registry := new(metrics.Registry)
		attempts := 0
		writer, err := storage.NewWriter(event.Service1, batchStore(func(context.Context, []event.Envelope) (storage.CommitResult, error) {
			attempts++
			return storage.CommitResult{}, injectedIO
		}), storage.WriterOptions{DeadLetters: sink, Metrics: registry, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- writer.Run(ctx, q.Deliveries(event.Service1)) }()
		synctest.Wait()
		time.Sleep(16 * time.Second)
		synctest.Wait()
		if registry.Snapshot().DeadLettered != 1 || registry.Snapshot().DeadLetterPending != 1 || q.Pending() != 2 {
			t.Fatal("partial terminal state was not retained")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		filesystem.partialID = ""
		if err := writer.Run(t.Context(), q.Deliveries(event.Service1)); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if attempts != 4 || bytes.Count(data, []byte{'\n'}) != 2 || q.Pending() != 0 || registry.Snapshot().DeadLetterPending != 0 {
			t.Fatalf("resume repeated or discarded work: attempts=%d counters=%+v", attempts, registry.Snapshot())
		}
	})
}

func TestWriterRetriesAfterOneFiveAndTenSecondsWithoutChangingIdentity(t *testing.T) {
	for _, recoverOn := range []int{2, 3, 4} {
		t.Run(fmt.Sprintf("attempt_%d", recoverOn), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q, _ := queue.NewMemory(2)
				q.Send(t.Context(), sample("stable-1"))
				q.Send(t.Context(), sample("stable-2"))
				q.Close()
				start := time.Now()
				var attempts []time.Duration
				registry := new(metrics.Registry)
				writer, err := storage.NewWriter(event.Service1, batchStore(func(ctx context.Context, records []event.Envelope) (storage.CommitResult, error) {
					attempts = append(attempts, time.Since(start))
					if len(records) != 2 || records[0].ID != "stable-1" || records[1].ID != "stable-2" || q.Pending() != 2 {
						t.Fatal("retry changed batch identity or settled before commit")
					}
					if len(attempts) < recoverOn {
						return storage.CommitResult{}, errors.New("temporary disk failure")
					}
					return storage.CommitResult{Inserted: 2}, nil
				}), storage.WriterOptions{Metrics: registry, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
				if err != nil {
					t.Fatal(err)
				}
				if err := writer.Run(t.Context(), q.Deliveries(event.Service1)); err != nil {
					t.Fatal(err)
				}
				expected := []time.Duration{0, time.Second, 6 * time.Second, 16 * time.Second}
				if len(attempts) != recoverOn {
					t.Fatalf("attempt times=%v", attempts)
				}
				for i, at := range attempts {
					if at != expected[i] {
						t.Fatalf("attempt %d at %s, want %s", i+1, at, expected[i])
					}
				}
				if q.Pending() != 0 || writer.Pending() != 0 || registry.Snapshot().Committed != 2 {
					t.Fatal("recovered batch was not committed and settled")
				}
			})
		})
	}
}

func TestWriterFlushesAt100AndOneSecondFromFirstRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, _ := queue.NewMemory(100)
		registry := new(metrics.Registry)
		batches := make(chan int, 4)
		writer, err := storage.NewWriter(event.Service1, batchStore(func(ctx context.Context, records []event.Envelope) (storage.CommitResult, error) {
			batches <- len(records)
			return storage.CommitResult{Inserted: uint64(len(records))}, nil
		}), storage.WriterOptions{Metrics: registry, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- writer.Run(t.Context(), q.Deliveries(event.Service1)) }()
		for i := range 100 {
			if err := q.Send(t.Context(), sample(string(rune(i+1)))); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		select {
		case count := <-batches:
			if count != 100 {
				t.Fatalf("size flush=%d", count)
			}
		default:
			t.Fatal("100 records did not flush immediately")
		}
		if q.Pending() != 0 || registry.Snapshot().Committed != 100 {
			t.Fatal("commit was not counted/settled")
		}
		// The timer is based on the first pending record, not a process ticker.
		time.Sleep(250 * time.Millisecond)
		q.Send(t.Context(), sample("timed"))
		synctest.Wait()
		time.Sleep(999 * time.Millisecond)
		synctest.Wait()
		select {
		case <-batches:
			t.Fatal("partial batch flushed before its one-second deadline")
		default:
		}
		if q.Pending() != 1 {
			t.Fatal("uncommitted event was settled early")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		select {
		case count := <-batches:
			if count != 1 {
				t.Fatalf("timer flush=%d", count)
			}
		default:
			t.Fatal("partial batch missed one-second flush")
		}
		q.Send(t.Context(), sample("final"))
		q.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if count := <-batches; count != 1 || q.Pending() != 0 || writer.Pending() != 0 {
			t.Fatal("final partial batch was not flushed and settled")
		}
	})
}

func TestWriterFailureRetainsCustodyAndBackpressuresBoundedQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, _ := queue.NewMemory(1)
		registry := new(metrics.Registry)
		failed := errors.New("disk unavailable")
		fail := true // only changed between completed Run calls
		writer, err := storage.NewWriter(event.Service1, batchStore(func(context.Context, []event.Envelope) (storage.CommitResult, error) {
			if fail {
				return storage.CommitResult{}, failed
			}
			return storage.CommitResult{Inserted: 1}, nil
		}), storage.WriterOptions{Metrics: registry, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		retryCtx, cancelRetry := context.WithCancel(t.Context())
		defer cancelRetry()
		go func() { done <- writer.Run(retryCtx, q.Deliveries(event.Service1)) }()
		q.Send(t.Context(), sample("held"))
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if writer.Pending() != 1 || q.Pending() != 1 || registry.Snapshot().Committed != 0 || registry.Snapshot().WriteFailures != 1 {
			t.Fatal("failed delivery was counted or settled")
		}
		cancelRetry()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled retry=%v", err)
		}
		if err := q.Send(t.Context(), sample("queued")); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		if err := q.Send(ctx, sample("overflow")); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("full queue=%v", err)
		}
		if q.Pending() != 2 {
			t.Fatalf("unbounded/missing pending work: %d", q.Pending())
		}
		fail = false
		q.Close()
		if err := writer.Run(t.Context(), q.Deliveries(event.Service1)); err != nil {
			t.Fatal(err)
		}
		if q.Pending() != 0 || writer.Pending() != 0 || registry.Snapshot().Committed != 2 {
			t.Fatal("retained deliveries did not resume")
		}
	})
}

func TestWriterSettlesDuplicateDeliveriesAfterRealSQLiteCommit(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenSQLite(t.Context(), dir, event.Service1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	q, _ := queue.NewMemory(2)
	q.Send(t.Context(), sample("duplicate"))
	q.Send(t.Context(), sample("duplicate"))
	q.Close()
	registry := new(metrics.Registry)
	writer, err := storage.NewWriter(event.Service1, store, storage.WriterOptions{Metrics: registry})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Run(t.Context(), q.Deliveries(event.Service1)); err != nil {
		t.Fatal(err)
	}
	if q.Pending() != 0 || registry.Snapshot().Committed != 1 || registry.Snapshot().Duplicates != 1 {
		t.Fatalf("deduplicated deliveries not settled: pending=%d counters=%+v", q.Pending(), registry.Snapshot())
	}
	var rows int
	if err := inspect(t, dir, event.Service1).QueryRow(`SELECT count(*) FROM logs`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("stored rows=%d, %v", rows, err)
	}
}

func TestWriterBoundsEachStorageAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, _ := queue.NewMemory(1)
		q.Send(t.Context(), sample("timeout"))
		q.Close()
		writer, err := storage.NewWriter(event.Service1, batchStore(func(ctx context.Context, records []event.Envelope) (storage.CommitResult, error) {
			<-ctx.Done()
			return storage.CommitResult{}, ctx.Err()
		}), storage.WriterOptions{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		if err := writer.Run(t.Context(), q.Deliveries(event.Service1)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("attempt=%v", err)
		}
		if elapsed := time.Since(started); elapsed != 24*time.Second {
			t.Fatalf("write deadline=%s", elapsed)
		}
		if q.Pending() != 1 || writer.Pending() != 1 {
			t.Fatal("timed-out write released custody")
		}
	})
}
