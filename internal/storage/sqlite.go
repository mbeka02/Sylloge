// Package storage owns transactional persistence and destination batch writers.
package storage

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

const insertLog = `INSERT INTO logs (id,timestamp,service,level,message,received_at,source_ip,env)
VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING`

type CommitResult struct{ Inserted, Duplicates uint64 }

type SQLite struct {
	db          *sql.DB
	insert      *sql.Stmt
	destination event.Destination
}

// OpenSQLite creates a versioned database before ingestion starts. DSN pragmas
// apply to every replacement connection, not just the initial pooled connection.
// Each destination has its own single-connection pool and one writer.
func OpenSQLite(ctx context.Context, directory string, destination event.Destination) (_ *SQLite, err error) {
	if !destination.Valid() {
		return nil, errors.New("invalid storage destination")
	}
	if directory == "" {
		return nil, errors.New("storage directory is required")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	path, err := filepath.Abs(filepath.Join(directory, "logs_"+string(destination)+".db"))
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open %s database file: %w", destination, err)
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	uri := url.URL{Scheme: "file", Path: path}
	query := url.Values{"_pragma": {"busy_timeout(1000)", "journal_mode(WAL)", "synchronous(FULL)"}, "_txlock": {"immediate"}}
	uri.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer func() {
		if err != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("initialize %s: %w", destination, err)
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return nil, err
	}
	if version != 0 && version != 1 {
		return nil, fmt.Errorf("unsupported %s schema version %d", destination, version)
	}
	if version == 0 {
		if _, err := tx.ExecContext(ctx, schema); err != nil {
			return nil, fmt.Errorf("create %s schema: %w", destination, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("initialize %s: %w", destination, err)
	}
	statement, err := db.PrepareContext(ctx, insertLog)
	if err != nil {
		return nil, fmt.Errorf("prepare %s insert: %w", destination, err)
	}
	return &SQLite{db: db, insert: statement, destination: destination}, nil
}

// Commit is all-or-none. Only an existing ID is a successful duplicate; other
// constraint errors roll back the batch. Results are meaningful after commit.
func (s *SQLite) Commit(ctx context.Context, events []event.Envelope) (CommitResult, error) {
	var result CommitResult
	for _, e := range events {
		if e.Destination != s.destination {
			return result, errors.New("event destination does not match database")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	statement := tx.StmtContext(ctx, s.insert)
	defer statement.Close()
	for _, e := range events {
		row, err := statement.ExecContext(ctx, e.ID, e.Timestamp.UTC().Format(time.RFC3339Nano), e.Service, e.Level, e.Message, e.ReceivedAt, e.SourceIP, e.Environment)
		if err != nil {
			return CommitResult{}, err
		}
		n, err := row.RowsAffected()
		if err != nil {
			return CommitResult{}, err
		}
		result.Inserted += uint64(n)
		result.Duplicates += uint64(1 - n)
	}
	if err := tx.Commit(); err != nil {
		return CommitResult{}, err
	}
	return result, nil
}

// Close is called after the destination writer has stopped.
func (s *SQLite) Close() error { return errors.Join(s.insert.Close(), s.db.Close()) }
