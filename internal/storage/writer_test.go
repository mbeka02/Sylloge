package storage_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
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
		go func() { done <- writer.Run(t.Context(), q.Deliveries(event.Service1)) }()
		q.Send(t.Context(), sample("held"))
		synctest.Wait()
		time.Sleep(time.Second)
		if err := <-done; !errors.Is(err, failed) {
			t.Fatalf("write error=%v", err)
		}
		if writer.Pending() != 1 || q.Pending() != 1 || registry.Snapshot().Committed != 0 || registry.Snapshot().WriteFailures != 1 {
			t.Fatal("failed delivery was counted or settled")
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
		if elapsed := time.Since(started); elapsed != 2*time.Second {
			t.Fatalf("write deadline=%s", elapsed)
		}
		if q.Pending() != 1 || writer.Pending() != 1 {
			t.Fatal("timed-out write released custody")
		}
	})
}
