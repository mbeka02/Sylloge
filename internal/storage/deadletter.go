package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
)

const DeadLetterRotationSize int64 = 10_000_000

// FailedRecord contains the original JSON value and the immutable enriched
// envelope needed for same-ID, same-destination recovery by an operator.
type FailedRecord struct {
	ID          string            `json:"id"`
	BatchID     string            `json:"batch_id"`
	Original    json.RawMessage   `json:"original"`
	Event       event.Envelope    `json:"event"`
	Destination event.Destination `json:"destination"`
	Attempts    int               `json:"attempts"`
	LastError   string            `json:"last_error"`
	FailedAt    time.Time         `json:"failed_at"`
}

// DeadLetterFileSystem is the filesystem failure seam. The production adapter
// uses ordinary local files; tests can inject partial writes and sync failures.
type DeadLetterFileSystem interface {
	MkdirAll(string, fs.FileMode) error
	OpenFile(string, int, fs.FileMode) (DeadLetterFile, error)
	Rename(string, string) error
	Remove(string) error
}

type DeadLetterFile interface {
	io.Writer
	io.ReaderAt
	Stat() (fs.FileInfo, error)
	Truncate(int64) error
	Sync() error
	Close() error
}

type OSFileSystem struct{}

func (OSFileSystem) MkdirAll(path string, mode fs.FileMode) error { return os.MkdirAll(path, mode) }
func (OSFileSystem) OpenFile(path string, flag int, mode fs.FileMode) (DeadLetterFile, error) {
	return os.OpenFile(path, flag, mode)
}
func (OSFileSystem) Rename(from, to string) error { return os.Rename(from, to) }
func (OSFileSystem) Remove(path string) error     { return os.Remove(path) }

type DeadLetterOptions struct {
	FileSystem DeadLetterFileSystem
	Logger     *slog.Logger
}

type DeadLetter struct {
	gate             chan struct{}
	path             string
	file             DeadLetterFile
	options          DeadLetterOptions
	size             int64
	closed           bool
	repairError      error
	rotationPending  bool
	archive          string
	renamed          bool
	maintenanceError error
}

// OpenDeadLetter validates storage before HTTP starts. Only one process may own
// a path. Context bounds lock waits; local file syscalls themselves must return.
func OpenDeadLetter(ctx context.Context, path string, options DeadLetterOptions) (_ *DeadLetter, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, errors.New("dead-letter path is required")
	}
	if options.FileSystem == nil {
		options.FileSystem = OSFileSystem{}
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if err := options.FileSystem.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create dead-letter directory: %w", err)
	}
	file, err := options.FileSystem.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("open dead-letter file: %w", err)
	}
	sink := &DeadLetter{gate: make(chan struct{}, 1), path: path, file: file, options: options}
	defer func() {
		if err != nil && sink.file != nil {
			err = errors.Join(err, sink.file.Close())
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("dead-letter path must be a regular file")
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("sync dead-letter file: %w", err)
	}
	sink.size = info.Size()
	sink.gate <- struct{}{}
	if err := sink.syncDirectory(); err != nil {
		return nil, err
	}
	if err := sink.repairTail(ctx); err != nil {
		return nil, err
	}
	if sink.size > DeadLetterRotationSize {
		sink.rotationPending = true
		if err := sink.finishRotation(); err != nil {
			return nil, err
		}
	}
	return sink, nil
}

// repairTail uses bounded buffers even if a damaged tail is very large. Its
// separate copy and directory entry are durable before the active file shrinks.
func (d *DeadLetter) repairTail(ctx context.Context) error {
	if d.size == 0 {
		return nil
	}
	var last [1]byte
	if _, err := d.file.ReadAt(last[:], d.size-1); err != nil {
		return err
	}
	if last[0] == '\n' {
		return nil
	}
	buffer := make([]byte, 64*1024)
	offset := d.size
	var prefix int64
	for offset > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		start := max(int64(0), offset-int64(len(buffer)))
		chunk := buffer[:offset-start]
		if _, err := d.file.ReadAt(chunk, start); err != nil {
			return err
		}
		if index := bytes.LastIndexByte(chunk, '\n'); index >= 0 {
			prefix = start + int64(index) + 1
			break
		}
		offset = start
	}
	path, copy, err := d.uniqueFile("partial")
	if err != nil {
		return fmt.Errorf("preserve dead-letter tail: %w", err)
	}
	_, copyErr := io.CopyBuffer(copy, io.NewSectionReader(d.file, prefix, d.size-prefix), buffer)
	if copyErr == nil {
		copyErr = copy.Sync()
	}
	copyErr = errors.Join(copyErr, copy.Close())
	if copyErr != nil {
		return fmt.Errorf("preserve dead-letter tail: %w", copyErr)
	}
	if err := d.syncDirectory(); err != nil {
		return fmt.Errorf("sync preserved tail: %w", err)
	}
	if err := d.file.Truncate(prefix); err != nil {
		return fmt.Errorf("repair incomplete tail: %w", err)
	}
	if err := d.file.Sync(); err != nil {
		return fmt.Errorf("sync repaired tail: %w", err)
	}
	d.size = prefix
	d.options.Logger.Warn("incomplete dead-letter tail preserved and repaired", "preserved_path", path, "complete_bytes", prefix)
	return nil
}

func (d *DeadLetter) uniqueFile(extension string) (string, DeadLetterFile, error) {
	for range 8 {
		path := strings.TrimSuffix(d.path, filepath.Ext(d.path)) + "." + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + event.NewID() + "." + extension
		file, err := d.options.FileSystem.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		return path, file, err
	}
	return "", nil, errors.New("cannot reserve a unique dead-letter filename")
}

// Append returns nil only after a complete line is durable. A failed write or
// sync is rolled back to the last complete offset. A failed repair halts new
// writes; later calls retry that repair before accepting another record.
func (d *DeadLetter) Append(ctx context.Context, record FailedRecord) error {
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode dead letter: %w", err)
	}
	line = append(line, '\n')
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.gate:
	}
	defer func() { d.gate <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed {
		return errors.New("dead-letter sink closed")
	}
	if d.repairError != nil {
		if err := d.repairAppend(); err != nil {
			return err
		}
	}
	// A failed rotation blocks new appends; resume its metadata operations
	// before writing again, so repeated failures cannot grow the file forever.
	if d.rotationPending {
		if err := d.finishRotation(); err != nil {
			return err
		}
	}
	n, err := d.file.Write(line)
	if err == nil && n != len(line) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = d.file.Sync()
	}
	if err != nil {
		return errors.Join(fmt.Errorf("append/sync dead letter: %w", err), d.repairAppend())
	}
	d.size += int64(len(line))
	if d.size > DeadLetterRotationSize {
		d.rotationPending = true
		return d.finishRotation()
	}
	return nil
}

func (d *DeadLetter) repairAppend() error {
	err := d.file.Truncate(d.size)
	if err == nil {
		err = d.file.Sync()
	}
	d.repairError = nil
	if err != nil {
		d.repairError = fmt.Errorf("dead-letter repair failed; appends halted: %w", err)
	}
	return d.repairError
}

func (d *DeadLetter) finishRotation() error {
	err := d.rotate()
	d.maintenanceError = err
	return err
}

func (d *DeadLetter) rotate() error {
	if !d.renamed {
		if d.file != nil {
			file := d.file
			d.file = nil
			if err := file.Close(); err != nil {
				return fmt.Errorf("close dead-letter file for rotation: %w", err)
			}
		}
		if d.archive == "" {
			path, reservation, err := d.uniqueFile("json")
			if err != nil {
				return fmt.Errorf("reserve rotation: %w", err)
			}
			d.archive = path
			if err := reservation.Close(); err != nil {
				return fmt.Errorf("close rotation reservation: %w", err)
			}
		}
		if err := d.options.FileSystem.Rename(d.path, d.archive); err != nil {
			removeErr := d.options.FileSystem.Remove(d.archive)
			if removeErr == nil {
				d.archive = ""
			}
			return errors.Join(fmt.Errorf("rotate dead-letter file: %w", err), removeErr)
		}
		d.renamed = true
	}
	// Persist the archived name before creating the new active file. A retry
	// after this point must never rename an unrelated fresh active file.
	if err := d.syncDirectory(); err != nil {
		return fmt.Errorf("sync rotated dead-letter directory: %w", err)
	}
	if d.file == nil {
		file, err := d.options.FileSystem.OpenFile(d.path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("create active dead-letter file after rotation: %w", err)
		}
		d.file = file
		d.size = 0
	}
	if err := d.file.Sync(); err != nil {
		return fmt.Errorf("sync new active dead-letter file: %w", err)
	}
	if err := d.syncDirectory(); err != nil {
		return fmt.Errorf("sync new active dead-letter directory: %w", err)
	}
	d.rotationPending = false
	d.archive = ""
	d.renamed = false
	return nil
}

func (d *DeadLetter) syncDirectory() error {
	directory, err := d.options.FileSystem.OpenFile(filepath.Dir(d.path), os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open dead-letter directory for sync: %w", err)
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (d *DeadLetter) Close() error {
	<-d.gate
	defer func() { d.gate <- struct{}{} }()
	if d.closed {
		return nil
	}
	d.closed = true
	var closeErr error
	if d.file != nil {
		closeErr = d.file.Close()
	}
	return errors.Join(d.repairError, d.maintenanceError, closeErr)
}
