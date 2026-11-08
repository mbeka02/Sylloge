package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mbeka02/sylloge/internal/storage"
)

var injectedIO = errors.New("injected filesystem failure")

// Inject faults at real filesystem calls while retaining actual on-disk
// artifacts. Flags change only between completed operations in these tests.
type faultFS struct {
	storage.OSFileSystem
	path                                                     string
	short, truncate, rename, directorySync, reopen, tailSync bool
	partial                                                  atomic.Bool
	syncFailures                                             int
	partialID                                                string
}

type blockedSyncFS struct {
	storage.OSFileSystem
	path    string
	enabled atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (f *blockedSyncFS) OpenFile(path string, flag int, mode fs.FileMode) (storage.DeadLetterFile, error) {
	file, err := f.OSFileSystem.OpenFile(path, flag, mode)
	if err != nil {
		return nil, err
	}
	if path == f.path {
		return &blockedSyncFile{DeadLetterFile: file, owner: f}, nil
	}
	return file, nil
}

type blockedSyncFile struct {
	storage.DeadLetterFile
	owner *blockedSyncFS
}

func (f *blockedSyncFile) Sync() error {
	if f.owner.enabled.CompareAndSwap(true, false) {
		close(f.owner.entered)
		<-f.owner.release
	}
	return f.DeadLetterFile.Sync()
}

func TestDeadLetterWaitingForAnotherWriterHonorsCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs-failed.json")
	filesystem := &blockedSyncFS{path: path, entered: make(chan struct{}), release: make(chan struct{})}
	sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{FileSystem: filesystem})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	filesystem.enabled.Store(true)
	done := make(chan error, 1)
	go func() { done <- sink.Append(t.Context(), failedRecord("syncing")) }()
	<-filesystem.entered
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err = sink.Append(ctx, failedRecord("canceled"))
	close(filesystem.release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("serialized append wait=%v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || bytes.Count(data, []byte{'\n'}) != 1 {
		t.Fatal("canceled waiting append changed the file")
	}
}

func (f *faultFS) OpenFile(path string, flag int, mode fs.FileMode) (storage.DeadLetterFile, error) {
	if path == f.path && flag&os.O_CREATE != 0 && f.reopen {
		return nil, injectedIO
	}
	file, err := f.OSFileSystem.OpenFile(path, flag, mode)
	if err != nil {
		return nil, err
	}
	return &faultFile{DeadLetterFile: file, fs: f, path: path}, nil
}
func (f *faultFS) Rename(from, to string) error {
	if f.rename {
		return injectedIO
	}
	return f.OSFileSystem.Rename(from, to)
}

type faultFile struct {
	storage.DeadLetterFile
	fs   *faultFS
	path string
}

func (f *faultFile) Write(p []byte) (int, error) {
	if f.path == f.fs.path && (f.fs.partial.Load() || f.fs.short || f.fs.partialID != "" && bytes.Contains(p, []byte(`"id":"`+f.fs.partialID+`"`))) {
		n, err := f.DeadLetterFile.Write(p[:min(9, len(p))])
		if err != nil {
			return n, err
		}
		if f.fs.short {
			return n, nil
		}
		return n, injectedIO
	}
	return f.DeadLetterFile.Write(p)
}
func (f *faultFile) Sync() error {
	if f.path == f.fs.path && f.fs.syncFailures > 0 {
		f.fs.syncFailures--
		return injectedIO
	}
	if f.path == filepath.Dir(f.fs.path) && f.fs.directorySync {
		return injectedIO
	}
	if strings.HasSuffix(f.path, ".partial") && f.fs.tailSync {
		return injectedIO
	}
	return f.DeadLetterFile.Sync()
}
func (f *faultFile) Truncate(size int64) error {
	if f.fs.truncate {
		return injectedIO
	}
	return f.DeadLetterFile.Truncate(size)
}

func TestDeadLetterRepairsPartialShortAndUnsyncedWrites(t *testing.T) {
	for _, kind := range []string{"partial error", "short write", "sync failure"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "logs-failed.json")
			filesystem := &faultFS{path: path}
			sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{FileSystem: filesystem})
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			if err := sink.Append(t.Context(), failedRecord("prefix")); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "partial error":
				filesystem.partial.Store(true)
			case "short write":
				filesystem.short = true
			case "sync failure":
				filesystem.syncFailures = 1
			}
			if err := sink.Append(t.Context(), failedRecord("retry")); err == nil {
				t.Fatal("failed filesystem operation reported durable completion")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("failed append damaged the complete prefix")
			}
			filesystem.partial.Store(false)
			filesystem.short = false
			if err := sink.Append(t.Context(), failedRecord("retry")); err != nil {
				t.Fatal(err)
			}
			after, err = os.ReadFile(path)
			if err != nil || bytes.Count(after, []byte{'\n'}) != 2 {
				t.Fatal("retry did not produce two complete records")
			}
		})
	}
}

func TestDeadLetterRepairFailureHaltsNewWritesWhileFaultPersists(t *testing.T) {
	for _, kind := range []string{"truncate", "repair sync"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "logs-failed.json")
			filesystem := &faultFS{path: path}
			sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{FileSystem: filesystem})
			if err != nil {
				t.Fatal(err)
			}
			filesystem.partial.Store(true)
			if kind == "truncate" {
				filesystem.truncate = true
			} else {
				filesystem.syncFailures = 1
			}
			if err := sink.Append(t.Context(), failedRecord("damaged")); err == nil || !strings.Contains(err.Error(), "appends halted") {
				t.Fatalf("repair failure=%v", err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			filesystem.partial.Store(false)
			if kind == "repair sync" {
				filesystem.syncFailures = 1
			}
			if err := sink.Append(t.Context(), failedRecord("blocked")); err == nil {
				t.Fatal("append continued after failed repair")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("halted sink changed the damaged file")
			}
			if err := sink.Close(); err == nil {
				t.Fatal("close hid failed repair")
			}
			recovered, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			if err := recovered.Append(t.Context(), failedRecord("recovered")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDeadLetterRetriesRepairBeforeAcceptingNewRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs-failed.json")
	filesystem := &faultFS{path: path}
	sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{FileSystem: filesystem})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if err := sink.Append(t.Context(), failedRecord("prefix")); err != nil {
		t.Fatal(err)
	}
	filesystem.partial.Store(true)
	filesystem.truncate = true
	if err := sink.Append(t.Context(), failedRecord("retry")); err == nil {
		t.Fatal("failed repair reported success")
	}
	filesystem.partial.Store(false)
	filesystem.truncate = false
	if err := sink.Append(t.Context(), failedRecord("retry")); err != nil {
		t.Fatalf("repaired filesystem did not resume: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\n'})
	if len(lines) != 2 || !json.Valid(lines[0]) || !json.Valid(lines[1]) {
		t.Fatal("repair resumed without restoring complete JSON Lines")
	}
}

func TestDeadLetterTailPreservationFailureDoesNotTruncateOriginal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs-failed.json")
	original := []byte("{\"id\":\"complete\"}\n{\"id\":\"partial")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	filesystem := &faultFS{path: path, tailSync: true}
	if sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{FileSystem: filesystem}); err == nil {
		sink.Close()
		t.Fatal("unsynced tail preservation succeeded")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(original, after) {
		t.Fatal("startup destroyed a tail that was not durably preserved")
	}
}

func TestDeadLetterRotationFailuresResumeBeforeFurtherAppends(t *testing.T) {
	for _, kind := range []string{"rename", "directory sync", "reopen"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "logs-failed.json")
			filesystem := &faultFS{path: path}
			sink, err := storage.OpenDeadLetter(t.Context(), path, storage.DeadLetterOptions{FileSystem: filesystem})
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			switch kind {
			case "rename":
				filesystem.rename = true
			case "directory sync":
				filesystem.directorySync = true
			case "reopen":
				filesystem.reopen = true
			}
			record := failedRecord("archived")
			record.LastError = strings.Repeat("x", 10_000_000)
			if err := sink.Append(t.Context(), record); err == nil {
				t.Fatal("failed rotation reported success")
			}
			if err := sink.Append(t.Context(), failedRecord("blocked")); err == nil {
				t.Fatal("append bypassed pending rotation failure")
			}
			filesystem.rename = false
			filesystem.directorySync = false
			filesystem.reopen = false
			if err := sink.Append(t.Context(), failedRecord("after-repair")); err != nil {
				t.Fatal(err)
			}
			archives, err := filepath.Glob(filepath.Join(filepath.Dir(path), "logs-failed.*.json"))
			if err != nil || len(archives) != 1 {
				t.Fatalf("archive lost/overwritten: %v, %v", archives, err)
			}
			archived, err := os.ReadFile(archives[0])
			if err != nil || !bytes.Contains(archived, []byte(`"id":"archived"`)) {
				t.Fatal("rotation lost durable data")
			}
			active, err := os.ReadFile(path)
			if err != nil || bytes.Count(active, []byte{'\n'}) != 1 || !bytes.Contains(active, []byte(`"id":"after-repair"`)) {
				t.Fatal("maintenance retries appended or renamed the wrong record")
			}
		})
	}
}
