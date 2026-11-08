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
	Committed
	Duplicates
	WriteFailures
	Retries
	DeadLettered
	DeadLetterFailures
	counterCount
)

type Gauge int

const (
	RetryPending Gauge = iota
	DeadLetterPending
	gaugeCount
)

type Registry struct {
	counters [counterCount]atomic.Uint64
	gauges   [gaugeCount]atomic.Int64
}

type Snapshot struct {
	Admitted, ValidationRejected, Overloaded, RateLimited, DefaultRouted, Dispatched uint64
	Committed, Duplicates, WriteFailures                                             uint64
	Retries, DeadLettered, DeadLetterFailures                                        uint64
	RetryPending, DeadLetterPending                                                  int64
}

func (r *Registry) Add(counter Counter, n uint64) { r.counters[counter].Add(n) }
func (r *Registry) AddGauge(gauge Gauge, n int64) { r.gauges[gauge].Add(n) }

func (r *Registry) Snapshot() Snapshot {
	return Snapshot{
		Admitted: r.counters[Admitted].Load(), ValidationRejected: r.counters[ValidationRejected].Load(),
		Overloaded: r.counters[Overloaded].Load(), RateLimited: r.counters[RateLimited].Load(),
		DefaultRouted: r.counters[DefaultRouted].Load(), Dispatched: r.counters[Dispatched].Load(),
		Committed: r.counters[Committed].Load(), Duplicates: r.counters[Duplicates].Load(), WriteFailures: r.counters[WriteFailures].Load(),
		Retries: r.counters[Retries].Load(), DeadLettered: r.counters[DeadLettered].Load(), DeadLetterFailures: r.counters[DeadLetterFailures].Load(),
		RetryPending: r.gauges[RetryPending].Load(), DeadLetterPending: r.gauges[DeadLetterPending].Load(),
	}
}
