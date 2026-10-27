// Package event defines the immutable values transferred through the pipeline.
package event

import (
	"encoding/json"
	"time"
)

type Log struct {
	Timestamp time.Time `json:"timestamp"`
	Service   string    `json:"service"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
}

// Metadata is captured at HTTP receipt but applied only after raw-channel dequeue.
type Metadata struct {
	ReceivedAt  time.Time
	SourceIP    string
	Environment string
}

type Raw struct {
	ID       string
	BatchID  string
	Original json.RawMessage
	Log      Log
	Metadata Metadata
}

type Destination string

const (
	Service1 Destination = "service1"
	Service2 Destination = "service2"
	Service3 Destination = "service3"
)

func (d Destination) Valid() bool {
	return d == Service1 || d == Service2 || d == Service3
}

type Envelope struct {
	Version  int             `json:"version"`
	ID       string          `json:"id"`
	BatchID  string          `json:"batch_id"`
	Original json.RawMessage `json:"original"`
	Log
	ReceivedAt  string      `json:"received_at"`
	SourceIP    string      `json:"source_ip"`
	Environment string      `json:"env"`
	Destination Destination `json:"destination"`
}
