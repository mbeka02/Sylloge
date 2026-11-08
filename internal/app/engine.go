package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/mbeka02/sylloge/internal/config"
	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/ingest"
	"github.com/mbeka02/sylloge/internal/metrics"
	"github.com/mbeka02/sylloge/internal/queue"
	"github.com/mbeka02/sylloge/internal/routing"
	"github.com/mbeka02/sylloge/internal/storage"
)

// engine is private lifecycle wiring, not another service abstraction. Its
// workers share an independent lifetime so signal cancellation cannot drop the
// HTTP-accepted backlog before the drain budget has elapsed.
type engine struct {
	pipeline    *ingest.Pipeline
	deliveries  *queue.Memory
	stores      []*storage.SQLite
	deadLetters *storage.DeadLetter
	writers     []*storage.Writer
	metrics     *metrics.Registry
	ready       atomic.Bool
	cancel      context.CancelFunc
	pipeDone    chan struct{}
	writersDone chan struct{}
	mu          sync.Mutex
	failures    error
	logger      *slog.Logger
}

func newEngine(ctx context.Context, cfg config.Config, logger *slog.Logger) (_ *engine, err error) {
	rules, err := config.LoadRouting(cfg.RoutingConfigFile)
	if err != nil {
		return nil, err
	}
	router, err := routing.New(rules.DefaultDestination, rules.Services)
	if err != nil {
		return nil, err
	}
	e := &engine{metrics: new(metrics.Registry), pipeDone: make(chan struct{}), writersDone: make(chan struct{}), logger: logger}
	defer func() {
		if err != nil {
			err = errors.Join(err, e.closeStorage())
		}
	}()
	e.pipeline, err = ingest.New(ingest.Options{Capacity: cfg.Pipeline.Capacity, BatchSize: cfg.Pipeline.BatchSize, AdmissionTimeout: cfg.Pipeline.AdmissionTimeout, Router: router, Metrics: e.metrics, Logger: logger})
	if err != nil {
		return nil, err
	}
	e.deliveries, err = queue.NewMemory(cfg.Pipeline.DeliveryCapacity)
	if err != nil {
		return nil, err
	}
	for _, destination := range []event.Destination{event.Service1, event.Service2, event.Service3} {
		store, err := storage.OpenSQLite(ctx, cfg.Storage.DataDir, destination)
		if err != nil {
			return nil, err
		}
		e.stores = append(e.stores, store)
	}
	// Resolve actual file identities after creating the databases, so even a
	// symlink or hard link cannot direct tail repair into a destination database.
	if deadLetterInfo, statErr := os.Stat(cfg.Storage.DeadLetterPath); statErr == nil {
		for _, destination := range []event.Destination{event.Service1, event.Service2, event.Service3} {
			for _, suffix := range []string{".db", ".db-wal", ".db-shm"} {
				info, statErr := os.Stat(filepath.Join(cfg.Storage.DataDir, "logs_"+string(destination)+suffix))
				if errors.Is(statErr, os.ErrNotExist) {
					continue
				}
				if statErr != nil {
					return nil, statErr
				}
				if os.SameFile(deadLetterInfo, info) {
					return nil, fmt.Errorf("DEAD_LETTER_PATH must not refer to destination database files")
				}
			}
		}
	}
	e.deadLetters, err = storage.OpenDeadLetter(ctx, cfg.Storage.DeadLetterPath, storage.DeadLetterOptions{Logger: logger})
	if err != nil {
		return nil, err
	}
	for i, destination := range []event.Destination{event.Service1, event.Service2, event.Service3} {
		writer, err := storage.NewWriter(destination, e.stores[i], storage.WriterOptions{Metrics: e.metrics, Logger: logger, DeadLetters: e.deadLetters})
		if err != nil {
			return nil, err
		}
		e.writers = append(e.writers, writer)
	}
	return e, nil
}

func (e *engine) start() {
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.ready.Store(true)
	var workers sync.WaitGroup
	for i, destination := range []event.Destination{event.Service1, event.Service2, event.Service3} {
		workers.Add(1)
		go func() { defer workers.Done(); e.record(e.writers[i].Run(ctx, e.deliveries.Deliveries(destination))) }()
	}
	go func() { workers.Wait(); close(e.writersDone) }()
	go func() { defer close(e.pipeDone); e.record(e.pipeline.Run(ctx, e.deliveries.Send)) }()
}

func (e *engine) record(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	e.ready.Store(false)
	e.mu.Lock()
	e.failures = errors.Join(e.failures, err)
	e.mu.Unlock()
	e.logger.Error("pipeline worker stopped; readiness failed", "error", err)
}

// drain follows producer ownership: handlers finish, raw channel closes,
// dispatch finishes, delivery paths close, then writers flush and settle.
func (e *engine) drain(ctx context.Context) error {
	defer e.cancel()
	err := e.pipeline.Close(ctx)
	if err == nil {
		select {
		case <-e.pipeDone:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if err != nil {
		e.cancel()
		// Cancellation releases a blocked dispatcher; Close remains safe against
		// any handler that was interrupted by forced HTTP connection closure.
		err = errors.Join(err, e.pipeline.Close(context.Background()))
	}
	<-e.pipeDone
	e.deliveries.Close()
	select {
	case <-e.writersDone:
	case <-ctx.Done():
		err = errors.Join(err, ctx.Err())
		e.cancel()
		<-e.writersDone
	}
	e.mu.Lock()
	err = errors.Join(err, e.failures)
	e.mu.Unlock()
	if raw, deliveries := e.pipeline.Pending(), e.deliveries.Pending(); raw != 0 || deliveries != 0 {
		e.logger.Error("shutdown has undurable memory records", "raw_pending", raw, "delivery_pending", deliveries)
		err = errors.Join(err, fmt.Errorf("undurable memory records remain: raw=%d deliveries=%d", raw, deliveries))
	}
	return err
}

func (e *engine) healthy() bool {
	if !e.ready.Load() {
		return false
	}
	for _, writer := range e.writers {
		if !writer.Healthy() {
			return false
		}
	}
	return true
}

func (e *engine) closeStorage() error {
	var err error
	for _, store := range e.stores {
		err = errors.Join(err, store.Close())
	}
	if e.deadLetters != nil {
		err = errors.Join(err, e.deadLetters.Close())
	}
	return err
}
