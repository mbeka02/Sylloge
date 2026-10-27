package ingest

import "github.com/mbeka02/sylloge/internal/event"

// Enrich performs only in-memory work; receipt time comes from the request,
// rather than the time the consumer eventually removes it from the buffer.
func Enrich(raw event.Raw) event.Envelope {
	sourceIP, environment := raw.Metadata.SourceIP, raw.Metadata.Environment
	if sourceIP == "" {
		sourceIP = "unknown"
	}
	if environment == "" {
		environment = "production"
	}
	return event.Envelope{Version: 1, ID: raw.ID, BatchID: raw.BatchID, Original: raw.Original, Log: raw.Log,
		ReceivedAt: raw.Metadata.ReceivedAt.UTC().Format("2006-01-02T15:04:05.000000Z"), SourceIP: sourceIP, Environment: environment}
}
