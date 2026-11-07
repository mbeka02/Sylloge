// Package queue owns bounded delivery paths. Broker adapters will share the
// same settlement contract as the memory delivery used by this milestone.
package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/mbeka02/sylloge/internal/event"
)

var ErrClosed = errors.New("delivery queues closed")

type Source string

const MemorySource Source = "memory"

// Delivery remains owned by its consumer until Settle succeeds after durable
// handling. Envelope is immutable. Repeated successful settlement is harmless.
type Delivery struct {
	Envelope event.Envelope
	Source   Source
	mu       sync.Mutex
	settled  bool
	settle   func(context.Context) error
}

func (d *Delivery) Settle(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.settle(ctx); err != nil {
		return err
	}
	d.settled = true
	return nil
}

type Memory struct {
	mu      sync.RWMutex
	closed  bool
	paths   map[event.Destination]chan *Delivery
	pending atomic.Int64
}

func NewMemory(capacity int) (*Memory, error) {
	if capacity <= 0 {
		return nil, errors.New("memory queue capacity must be positive")
	}
	m := &Memory{paths: make(map[event.Destination]chan *Delivery, 3)}
	for _, destination := range []event.Destination{event.Service1, event.Service2, event.Service3} {
		m.paths[destination] = make(chan *Delivery, capacity)
	}
	return m, nil
}

// Send transfers custody on nil. A full destination waits with cancellation;
// no goroutine or spill buffer is created. Only the dispatch worker calls Send.
func (m *Memory) Send(ctx context.Context, envelope event.Envelope) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	path, ok := m.paths[envelope.Destination]
	if !ok {
		return errors.New("invalid queue destination")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d := &Delivery{Envelope: envelope, Source: MemorySource, settle: func(context.Context) error { m.pending.Add(-1); return nil }}
	// A fast consumer cannot settle before Send records the custody transfer.
	d.mu.Lock()
	defer d.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case path <- d:
		m.pending.Add(1)
		return nil
	}
}

// Deliveries has one consumer per destination. Invalid destinations return nil.
func (m *Memory) Deliveries(destination event.Destination) <-chan *Delivery {
	return m.paths[destination]
}

// Pending includes both queued and consumer-owned unsettled deliveries.
func (m *Memory) Pending() int64 { return m.pending.Load() }

// Close is idempotent. Stop or cancel the dispatch producer before calling it:
// it waits for active sends to finish. Existing deliveries remain drainable.
func (m *Memory) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	for _, path := range m.paths {
		close(path)
	}
}
