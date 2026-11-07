package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
			err = errors.Join(err, e.closeStores())
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
		writer, err := storage.NewWriter(destination, store, storage.WriterOptions{Metrics: e.metrics, Logger: logger})
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

func (e *engine) closeStores() error {
	var err error
	for _, store := range e.stores {
		err = errors.Join(err, store.Close())
	}
	return err
}
