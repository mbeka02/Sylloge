// Package ingest owns bounded admission, enrichment and destination dispatch.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/metrics"
	"github.com/mbeka02/sylloge/internal/routing"
)

var (
	ErrOverloaded = errors.New("ingestion overloaded")
	ErrClosed     = errors.New("ingestion closed")
	ErrRunning    = errors.New("pipeline consumer already running")
)

type Options struct {
	Capacity         int
	BatchSize        int
	AdmissionTimeout time.Duration
	Router           *routing.Router
	Metrics          *metrics.Registry
	Logger           *slog.Logger
}

type Pipeline struct {
	raw         chan event.Raw
	gate        chan struct{}
	changed     chan struct{} // generation broadcast, protected by gate
	closed      bool          // protected by gate
	options     Options
	running     atomic.Bool
	outstanding atomic.Int64
	pending     []event.Envelope // owned by the one active Run call; retained on failure
}

func New(options Options) (*Pipeline, error) {
	if options.Capacity <= 0 || options.BatchSize <= 0 || options.AdmissionTimeout <= 0 || options.AdmissionTimeout > 100*time.Millisecond || options.Router == nil {
		return nil, errors.New("pipeline requires positive capacity/batch size, a router, and an admission timeout of at most 100ms")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Metrics == nil {
		options.Metrics = new(metrics.Registry)
	}
	p := &Pipeline{raw: make(chan event.Raw, options.Capacity), gate: make(chan struct{}, 1), changed: make(chan struct{}), options: options}
	p.gate <- struct{}{}
	return p, nil
}

// Admit transfers ownership of all records on success. Callers must not mutate
// their payloads after that point. Failure admits none. Cancellation before
// reservation prevents admission; cancellation after reservation cannot undo it.
func (p *Pipeline) Admit(ctx context.Context, records []event.Raw) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(records) > cap(p.raw) {
		return ErrOverloaded
	}
	deadline := time.Now().Add(p.options.AdmissionTimeout)
	timer := time.NewTimer(p.options.AdmissionTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return ErrOverloaded
		case <-p.gate:
		}
		if p.closed {
			p.release()
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			p.release()
			return err
		}
		if !time.Now().Before(deadline) {
			p.release()
			return ErrOverloaded
		}
		if cap(p.raw)-len(p.raw) >= len(records) {
			p.outstanding.Add(int64(len(records)))
			p.options.Metrics.Add(metrics.Admitted, uint64(len(records)))
			// All producers hold this gate; consumers only free room. These sends
			// cannot block, and closing the channel also requires the same gate.
			for _, record := range records {
				p.raw <- record
			}
			p.release()
			return nil
		}
		changed := p.changed
		p.release()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return ErrOverloaded
		case <-changed:
		}
	}
}

// Close prevents new admission and lets Run drain accepted work. It is safe to
// call concurrently with handlers; it never closes a channel under a sender.
func (p *Pipeline) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.gate:
	}
	defer p.release()
	if !p.closed {
		p.closed = true
		close(p.raw)
		close(p.changed)
	}
	return nil
}

// Pending includes buffered and consumer-owned records not yet transferred to
// dispatch. It remains truthful after a canceled or failed Run.
func (p *Pipeline) Pending() int64 { return p.outstanding.Load() }

// Run permits one consumer at a time. A failed/canceled dispatch retains its
// envelope and the rest of the batch; a later Run can resume with the same IDs.
// send must honor context cancellation; nil means custody was transferred.
func (p *Pipeline) Run(ctx context.Context, send func(context.Context, event.Envelope) error) error {
	if send == nil {
		return errors.New("pipeline dispatch function is required")
	}
	if !p.running.CompareAndSwap(false, true) {
		return ErrRunning
	}
	defer p.running.Store(false)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(p.pending) == 0 {
			var first event.Raw
			select {
			case <-ctx.Done():
				return ctx.Err()
			case record, ok := <-p.raw:
				if !ok {
					return nil
				}
				first = record
			}
			p.pending = append(p.pending, p.prepare(first))
		batch:
			for len(p.pending) < p.options.BatchSize {
				select {
				case record, ok := <-p.raw:
					if !ok {
						break batch
					}
					p.pending = append(p.pending, p.prepare(record))
				default:
					break batch
				}
			}
			p.roomAvailable()
		}
		for len(p.pending) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := send(ctx, p.pending[0]); err != nil {
				return fmt.Errorf("dispatch to %s: %w", p.pending[0].Destination, err)
			}
			p.pending[0] = event.Envelope{}
			p.pending = p.pending[1:]
			p.outstanding.Add(-1)
			p.options.Metrics.Add(metrics.Dispatched, 1)
		}
	}
}

func (p *Pipeline) prepare(raw event.Raw) event.Envelope {
	envelope := Enrich(raw)
	destination, defaulted := p.options.Router.Route(envelope.Service)
	envelope.Destination = destination
	if defaulted {
		p.options.Logger.Warn("default routing used", "service", envelope.Service, "destination", destination, "event_id", envelope.ID)
		p.options.Metrics.Add(metrics.DefaultRouted, 1)
	}
	return envelope
}

func (p *Pipeline) release() { p.gate <- struct{}{} }

func (p *Pipeline) roomAvailable() {
	<-p.gate
	defer p.release()
	if !p.closed {
		close(p.changed)
		p.changed = make(chan struct{})
	}
}
