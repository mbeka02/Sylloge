package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/metrics"
	"github.com/mbeka02/sylloge/internal/queue"
)

const (
	BatchSize     = 100
	FlushInterval = time.Second
	WriteTimeout  = 2 * time.Second
)

// BatchStore commits atomically; counts describe only a successful commit.
type BatchStore interface {
	Commit(context.Context, []event.Envelope) (CommitResult, error)
}

type WriterOptions struct {
	Metrics *metrics.Registry
	Logger  *slog.Logger
}

type Writer struct {
	destination event.Destination
	store       BatchStore
	options     WriterOptions
	running     atomic.Bool
	count       atomic.Int64
	pending     []*queue.Delivery // owned by Run; retained on any failure
}

func NewWriter(destination event.Destination, store BatchStore, options WriterOptions) (*Writer, error) {
	if !destination.Valid() || store == nil {
		return nil, errors.New("writer requires a destination and batch store")
	}
	if options.Metrics == nil {
		options.Metrics = new(metrics.Registry)
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Writer{destination: destination, store: store, options: options}, nil
}

func (w *Writer) Pending() int64 { return w.count.Load() }

// Run owns one destination. Close the input to drain and flush its final batch.
// Cancellation or failure leaves records owned and unsettled; a later Run may
// resume. This milestone deliberately does not retry or dead-letter failures.
func (w *Writer) Run(ctx context.Context, deliveries <-chan *queue.Delivery) error {
	if deliveries == nil {
		return errors.New("writer requires a delivery stream")
	}
	if !w.running.CompareAndSwap(false, true) {
		return errors.New("destination writer already running")
	}
	defer w.running.Store(false)
	if len(w.pending) > 0 {
		if err := w.flush(ctx); err != nil {
			return err
		}
	}
	var timer *time.Timer
	var timeout <-chan time.Time
	var deadline time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(w.pending) > 0 && !time.Now().Before(deadline) {
			if err := w.flush(ctx); err != nil {
				return err
			}
			timer.Stop()
			timeout = nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			if err := w.flush(ctx); err != nil {
				return err
			}
			timeout = nil
		case delivery, ok := <-deliveries:
			if !ok {
				return w.flush(ctx)
			}
			w.pending = append(w.pending, delivery)
			w.count.Add(1)
			if delivery.Envelope.Destination != w.destination {
				return errors.New("delivery destination does not match writer")
			}
			if len(w.pending) == 1 {
				deadline = time.Now().Add(FlushInterval)
				if timer == nil {
					timer = time.NewTimer(FlushInterval)
				} else {
					timer.Reset(FlushInterval)
				}
				timeout = timer.C
			}
			if len(w.pending) == BatchSize {
				if err := w.flush(ctx); err != nil {
					return err
				}
				timer.Stop()
				timeout = nil
			}
		}
	}
}

func (w *Writer) flush(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	records := make([]event.Envelope, len(w.pending))
	for i, delivery := range w.pending {
		records[i] = delivery.Envelope
	}
	attempt, cancel := context.WithTimeout(ctx, WriteTimeout)
	result, err := w.store.Commit(attempt, records)
	cancel()
	if err != nil {
		w.options.Metrics.Add(metrics.WriteFailures, 1)
		var coded interface{ Code() int }
		var code int
		if errors.As(err, &coded) {
			code = coded.Code()
		}
		for _, record := range records {
			w.options.Logger.Error("database write failed; delivery retained", "destination", w.destination, "event_id", record.ID, "batch_id", record.BatchID, "attempt", 1, "sqlite_code", code, "error", err)
		}
		return fmt.Errorf("write %s batch: %w", w.destination, err)
	}
	w.options.Metrics.Add(metrics.Committed, result.Inserted)
	w.options.Metrics.Add(metrics.Duplicates, result.Duplicates)
	for len(w.pending) > 0 {
		if err := w.pending[0].Settle(ctx); err != nil {
			return fmt.Errorf("settle %s delivery: %w", w.destination, err)
		}
		w.pending[0] = nil
		w.pending = w.pending[1:]
		w.count.Add(-1)
	}
	return nil
}
