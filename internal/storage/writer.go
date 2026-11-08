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

// DeadLetterSink returns nil only after durable terminal handling. Failure
// leaves custody with the writer and must be safe to retry with the same record.
type DeadLetterSink interface {
	Append(context.Context, FailedRecord) error
}

type WriterOptions struct {
	Metrics     *metrics.Registry
	Logger      *slog.Logger
	DeadLetters DeadLetterSink
}

type Writer struct {
	destination event.Destination
	store       BatchStore
	options     WriterOptions
	running     atomic.Bool
	count       atomic.Int64
	unhealthy   atomic.Bool
	pending     []*pendingRecord // owned by Run; retained on cancellation
}

type pendingRecord struct {
	delivery              *queue.Delivery
	attempts              int
	nextAttempt           time.Time
	lastError             error
	handled               bool
	failedAt              time.Time
	retrying              bool
	deadlettering         bool
	nextDeadLetterAttempt time.Time
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
func (w *Writer) Healthy() bool  { return !w.unhealthy.Load() }

// Run owns one destination. Close the input to drain and flush its final batch.
// Cancellation leaves records owned and unsettled; a later Run resumes their
// attempt counts and remaining delays. No retry goroutines are created.
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
			w.pending = append(w.pending, &pendingRecord{delivery: delivery})
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
	var unhandled []*pendingRecord
	for _, record := range w.pending {
		if !record.handled {
			unhandled = append(unhandled, record)
		}
	}
	if len(unhandled) > 0 && unhandled[0].attempts < 4 {
		if err := w.writeBatch(ctx, unhandled); err != nil {
			return err
		}
	}
	for _, record := range unhandled {
		if record.handled {
			continue
		}
		if w.options.DeadLetters == nil {
			return fmt.Errorf("write %s exhausted; no dead-letter sink configured: %w", w.destination, record.lastError)
		}
		envelope := record.delivery.Envelope
		failed := FailedRecord{ID: envelope.ID, BatchID: envelope.BatchID, Original: envelope.Original, Event: envelope, Destination: envelope.Destination, Attempts: record.attempts, LastError: record.lastError.Error(), FailedAt: record.failedAt}
		for {
			if err := waitUntil(ctx, record.nextDeadLetterAttempt); err != nil {
				return err
			}
			if err := w.options.DeadLetters.Append(ctx, failed); err != nil {
				w.unhealthy.Store(true)
				w.options.Metrics.Add(metrics.DeadLetterFailures, 1)
				w.options.Logger.Error("dead-letter persistence failed; delivery retained", "destination", w.destination, "event_id", envelope.ID, "batch_id", envelope.BatchID, "attempt", record.attempts, "error", err)
				record.nextDeadLetterAttempt = time.Now().Add(time.Second)
				continue
			}
			record.handled = true
			w.options.Metrics.Add(metrics.DeadLettered, 1)
			w.options.Logger.Warn("record dead-lettered", "destination", w.destination, "event_id", envelope.ID, "batch_id", envelope.BatchID, "attempts", record.attempts)
			w.clearFailureGauges(record)
			break
		}
	}
	for len(w.pending) > 0 {
		if err := w.pending[0].delivery.Settle(ctx); err != nil {
			return fmt.Errorf("settle %s delivery: %w", w.destination, err)
		}
		w.pending[0] = nil
		w.pending = w.pending[1:]
		w.count.Add(-1)
	}
	w.unhealthy.Store(false)
	return nil
}

func (w *Writer) writeBatch(ctx context.Context, pending []*pendingRecord) error {
	records := make([]event.Envelope, len(pending))
	for i, record := range pending {
		records[i] = record.delivery.Envelope
	}
	delays := []time.Duration{time.Second, 5 * time.Second, 10 * time.Second}
	for pending[0].attempts < 4 {
		if err := waitUntil(ctx, pending[0].nextAttempt); err != nil {
			return err
		}
		if pending[0].attempts > 0 {
			w.options.Metrics.Add(metrics.Retries, uint64(len(pending)))
		}
		for _, record := range pending {
			record.attempts++
		}
		attempt, cancel := context.WithTimeout(ctx, WriteTimeout)
		result, err := w.store.Commit(attempt, records)
		cancel()
		if err == nil {
			w.options.Metrics.Add(metrics.Committed, result.Inserted)
			w.options.Metrics.Add(metrics.Duplicates, result.Duplicates)
			for _, record := range pending {
				record.handled = true
				w.clearFailureGauges(record)
				if record.attempts > 1 {
					envelope := record.delivery.Envelope
					w.options.Logger.Info("database write recovered", "destination", w.destination, "event_id", envelope.ID, "batch_id", envelope.BatchID, "attempt", record.attempts)
				}
			}
			return nil
		}
		w.unhealthy.Store(true)
		w.options.Metrics.Add(metrics.WriteFailures, 1)
		var coded interface{ Code() int }
		var code int
		if errors.As(err, &coded) {
			code = coded.Code()
		}
		next := time.Now()
		for _, record := range pending {
			record.lastError = err
			if record.attempts < 4 {
				record.nextAttempt = next.Add(delays[record.attempts-1])
				if !record.retrying {
					record.retrying = true
					w.options.Metrics.AddGauge(metrics.RetryPending, 1)
				}
			} else {
				if record.retrying {
					record.retrying = false
					w.options.Metrics.AddGauge(metrics.RetryPending, -1)
				}
				if !record.deadlettering {
					record.deadlettering = true
					w.options.Metrics.AddGauge(metrics.DeadLetterPending, 1)
				}
				record.failedAt = next.UTC()
			}
			envelope := record.delivery.Envelope
			w.options.Logger.Error("database write failed; delivery retained", "destination", w.destination, "event_id", envelope.ID, "batch_id", envelope.BatchID, "attempt", record.attempts, "sqlite_code", code, "error", err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) clearFailureGauges(record *pendingRecord) {
	if record.retrying {
		record.retrying = false
		w.options.Metrics.AddGauge(metrics.RetryPending, -1)
	}
	if record.deadlettering {
		record.deadlettering = false
		w.options.Metrics.AddGauge(metrics.DeadLetterPending, -1)
	}
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := time.Until(deadline)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
