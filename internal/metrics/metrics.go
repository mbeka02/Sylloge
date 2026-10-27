// Package metrics owns process-local instrumentation. HTTP presentation and
// throughput windows are added with the operational dashboard milestone.
package metrics

import "sync/atomic"

type Counter int

const (
	Admitted Counter = iota
	ValidationRejected
	Overloaded
	RateLimited
	DefaultRouted
	Dispatched
	counterCount
)

type Registry struct{ counters [counterCount]atomic.Uint64 }

type Snapshot struct {
	Admitted, ValidationRejected, Overloaded, RateLimited, DefaultRouted, Dispatched uint64
}

func (r *Registry) Add(counter Counter, n uint64) { r.counters[counter].Add(n) }

func (r *Registry) Snapshot() Snapshot {
	return Snapshot{
		Admitted: r.counters[Admitted].Load(), ValidationRejected: r.counters[ValidationRejected].Load(),
		Overloaded: r.counters[Overloaded].Load(), RateLimited: r.counters[RateLimited].Load(),
		DefaultRouted: r.counters[DefaultRouted].Load(), Dispatched: r.counters[Dispatched].Load(),
	}
}
