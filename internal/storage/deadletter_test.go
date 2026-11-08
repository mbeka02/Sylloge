package storage_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mbeka02/sylloge/internal/storage"
)

func failedRecord(id string) storage.FailedRecord {
	e := sample(id)
	e.Original = json.RawMessage(`{"timestamp":"2026-10-26T13:30:00+03:00","service":"claims-api","level":"ERROR","message":"claim timed out","extra":"preserved"}`)
	return storage.FailedRecord{ID: e.ID, BatchID: e.BatchID, Original: e.Original, Event: e, Destination: e.Destination, Attempts: 4, LastError: "disk full", FailedAt: time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)}
}

func TestThreeWritersShareOneSerializedDeadLetterFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs-failed.json")
	sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	var workers sync.WaitGroup
	errors := make(chan error, 3)
	for writer := range 3 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range 20 {
				if err := sink.Append(t.Context(), failedRecord(fmt.Sprintf("%d-%d", writer, index))); err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, line := range bytes.Split(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\n'}) {
		var record storage.FailedRecord
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal("concurrent appends interleaved")
		}
		if seen[record.ID] {
			t.Fatal("duplicate concurrent append")
		}
		seen[record.ID] = true
	}
	if len(seen) != 60 {
		t.Fatalf("concurrent records=%d", len(seen))
	}
}

func TestDeadLetterRotatesOnlyAfterExceedingTenMillionBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs-failed.json")
	sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	record := failedRecord("exact-boundary")
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	record.LastError += strings.Repeat("x", int(storage.DeadLetterRotationSize)-len(encoded)-1)
	if err := sink.Append(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 10_000_000 {
		t.Fatalf("boundary size=%v, %v", info, err)
	}
	if err := sink.Append(t.Context(), failedRecord("crossing")); err != nil {
		t.Fatal(err)
	}
	archives, err := filepath.Glob(filepath.Join(filepath.Dir(path), "logs-failed.*.json"))
	if err != nil || len(archives) != 1 {
		t.Fatalf("rotation=%v, %v", archives, err)
	}
	data, err := os.ReadFile(archives[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(data) <= 10_000_000 || data[len(data)-1] != '\n' {
		t.Fatal("rotation split a complete entry")
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\n'})
	if len(lines) != 2 || !json.Valid(lines[0]) || !json.Valid(lines[1]) {
		t.Fatal("rotated file does not contain two complete records")
	}
	info, err = os.Stat(path)
	if err != nil || info.Size() != 0 {
		t.Fatal("rotation did not create a fresh active file")
	}
	if err := sink.Append(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err := sink.Append(t.Context(), failedRecord("crossing-again")); err != nil {
		t.Fatal(err)
	}
	archives, err = filepath.Glob(filepath.Join(filepath.Dir(path), "logs-failed.*.json"))
	if err != nil || len(archives) != 2 {
		t.Fatal("rotation overwrote an older archive")
	}
}

func TestDeadLetterPreservesIncompleteTailBeforeStartupRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs-failed.json")
	complete, err := json.Marshal(failedRecord("complete"))
	if err != nil {
		t.Fatal(err)
	}
	complete = append(complete, '\n')
	tail := []byte(`{"id":"interrupted","original":{"message":"half`)
	if err := os.WriteFile(path, append(append([]byte{}, complete...), tail...), 0600); err != nil {
		t.Fatal(err)
	}
	sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	preserved, err := filepath.Glob(filepath.Join(filepath.Dir(path), "logs-failed.*.partial"))
	if err != nil || len(preserved) != 1 {
		t.Fatalf("incomplete tail not preserved: %v, %v", preserved, err)
	}
	got, err := os.ReadFile(preserved[0])
	if err != nil || !bytes.Equal(got, tail) {
		t.Fatalf("quarantined tail=%q, %v", got, err)
	}
	got, err = os.ReadFile(path)
	if err != nil || !bytes.Equal(got, complete) {
		t.Fatal("startup repair changed the complete prefix")
	}
	if err := sink.Append(t.Context(), failedRecord("after-repair")); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(bytes.TrimSuffix(got, []byte{'\n'}), []byte{'\n'}) {
		if !json.Valid(line) {
			t.Fatal("repair left invalid JSON Lines")
		}
	}
}

func TestDeadLetterAppendsCompleteRecoverableRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new", "logs-failed.json")
	sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Append(t.Context(), failedRecord("failed-1")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		t.Fatal("entry is not a complete JSON line")
	}
	var got storage.FailedRecord
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != "failed-1" || got.BatchID != "batch-1" || got.Attempts != 4 || got.LastError != "disk full" || got.FailedAt.Format(time.RFC3339) != "2026-11-07T10:00:00Z" || got.Event.ID != got.ID || got.Event.ReceivedAt != "2026-10-26T10:30:01.123456Z" || got.Destination != "service1" {
		t.Fatalf("missing recovery metadata: %+v", got)
	}
	var original map[string]any
	if err := json.Unmarshal(got.Original, &original); err != nil || original["extra"] != "preserved" {
		t.Fatal("original entry was lost")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("dead-letter file should be private")
	}
}
