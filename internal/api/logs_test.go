package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mbeka02/sylloge/internal/api"
	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/ingest"
	"github.com/mbeka02/sylloge/internal/metrics"
	"github.com/mbeka02/sylloge/internal/routing"
)

const validRecord = `{"timestamp":"2026-10-27T09:00:00Z","service":"claims-api","level":"INFO","message":"ok"}`

func newIngestionHandler(t *testing.T, capacity int) (http.Handler, *ingest.Pipeline, *metrics.Registry) {
	t.Helper()
	return newHandlerWithOptions(t, capacity, api.Options{Environment: "staging", MaxActiveRequests: 1, TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, Now: func() time.Time { return time.Date(2026, 10, 27, 9, 0, 1, 123456000, time.UTC) }})
}

func newHandlerWithOptions(t *testing.T, capacity int, options api.Options) (http.Handler, *ingest.Pipeline, *metrics.Registry) {
	t.Helper()
	counters := new(metrics.Registry)
	router, err := routing.New(event.Service3, map[string]event.Destination{"claims-api": event.Service1})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p, err := ingest.New(ingest.Options{Capacity: capacity, BatchSize: 50, AdmissionTimeout: 100 * time.Millisecond, Router: router, Metrics: counters, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	options.Admitter = p
	options.Metrics = counters
	if options.Logger == nil {
		options.Logger = logger
	}
	h, err := api.NewHandler(options)
	if err != nil {
		t.Fatal(err)
	}
	return h, p, counters
}

func requestLogs(h http.Handler, body, contentType string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader(body))
	r.Header.Set("Content-Type", contentType)
	r.RemoteAddr = "10.0.0.2:1234"
	r.Header.Set("X-Forwarded-For", "192.0.2.1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestMixedBatchAcceptsOnlyValidRecords(t *testing.T) {
	h, p, counters := newIngestionHandler(t, 10)
	w := requestLogs(h, "["+validRecord+`,{"level":"TRACE"}]`, "application/json; charset=utf-8")
	var response struct {
		Status             string
		BatchID            string `json:"batchId"`
		Accepted, Rejected int
		Errors             []struct {
			Index                int
			Field, Code, Message string
		}
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != 202 || response.Status != "accepted" || len(response.BatchID) != 36 || response.Accepted != 1 || response.Rejected != 1 || len(response.Errors) != 4 {
		t.Fatalf("unexpected response: %d %s", w.Code, w.Body)
	}
	for _, problem := range response.Errors {
		if problem.Index != 1 || problem.Field == "" || problem.Code == "" || problem.Message == "" {
			t.Fatal("missing per-entry field context")
		}
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	var delivered []event.Envelope
	if err := p.Run(context.Background(), func(_ context.Context, e event.Envelope) error { delivered = append(delivered, e); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 1 || delivered[0].BatchID != response.BatchID || delivered[0].SourceIP != "192.0.2.1" || delivered[0].ReceivedAt != "2026-10-27T09:00:01.123456Z" || delivered[0].Environment != "staging" || string(delivered[0].Original) != validRecord {
		t.Fatalf("lost event/metadata: %+v", delivered)
	}
	if got := counters.Snapshot(); got.Admitted != 1 || got.ValidationRejected != 1 {
		t.Fatalf("incorrect metrics: %+v", got)
	}
}

func TestEntirelyValidResponseKeepsRequiredShape(t *testing.T) {
	h, _, _ := newIngestionHandler(t, 2)
	w := requestLogs(h, "["+validRecord+"]", "application/json")
	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != 202 || len(response) != 2 || response["status"] != "accepted" || response["batchId"] == nil {
		t.Fatalf("unexpected success contract: %d %s", w.Code, w.Body)
	}
}

func TestRequestRejectionAcceptsNoRecords(t *testing.T) {
	for _, tc := range []struct {
		name, body, contentType string
		status                  int
	}{
		{"malformed", "[" + validRecord + ",", "application/json", 400},
		{"trailing", "[" + validRecord + "] {}", "application/json", 400},
		{"empty", "[]", "application/json", 400}, {"null", "null", "application/json", 400},
		{"object", validRecord, "application/json", 400}, {"scalar", "3", "application/json", 400},
		{"all invalid", "[{},3]", "application/json", 422},
		{"media type", "[" + validRecord + "]", "text/plain", 415},
		{"missing media", "[" + validRecord + "]", "", 415},
		{"oversized", strings.Repeat(" ", 1000001), "application/json", 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, p, _ := newIngestionHandler(t, 10)
			w := requestLogs(h, tc.body, tc.contentType)
			if w.Code != tc.status || p.Pending() != 0 {
				t.Fatalf("got %d pending=%d body=%s", w.Code, p.Pending(), w.Body)
			}
			if w.Header().Get("Content-Type") != "application/json" {
				t.Fatal("errors must use JSON")
			}
		})
	}
}

func TestOverloadNeverAdmitsPartialSubset(t *testing.T) {
	h, p, _ := newIngestionHandler(t, 1)
	w := requestLogs(h, "["+validRecord+","+validRecord+"]", "application/json")
	if w.Code != 503 || strings.TrimSpace(w.Body.String()) != `{"error":"ingestion overloaded"}` || p.Pending() != 0 {
		t.Fatalf("incorrect overload contract: %d %s pending=%d", w.Code, w.Body, p.Pending())
	}
}

func TestBodySizeBoundaryAndChunkedOverflow(t *testing.T) {
	h, p, _ := newIngestionHandler(t, 2)
	body := "[" + validRecord + "]"
	body += strings.Repeat(" ", 1000000-len(body))
	if w := requestLogs(h, body, "application/json"); w.Code != 202 {
		t.Fatalf("exact body limit rejected: %d %s", w.Code, w.Body)
	}
	r := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewBufferString(body+" "))
	r.ContentLength = -1
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 413 || p.Pending() != 1 {
		t.Fatalf("chunked overflow admitted: %d pending=%d", w.Code, p.Pending())
	}
}

type observedBody struct {
	io.ReadCloser
	started chan struct{}
	once    sync.Once
}

func (body *observedBody) Read(data []byte) (int, error) {
	body.once.Do(func() { close(body.started) })
	return body.ReadCloser.Read(data)
}

func TestActiveRequestBoundIncludesBodyReading(t *testing.T) {
	h, p, counters := newIngestionHandler(t, 2)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	started := make(chan struct{})
	request := httptest.NewRequest(http.MethodPost, "/logs", &observedBody{ReadCloser: reader, started: started})
	request.Header.Set("Content-Type", "application/json")
	first := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(first, request) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request did not start reading body")
	}
	if w := requestLogs(h, "["+validRecord+"]", "application/json"); w.Code != 503 || p.Pending() != 0 {
		t.Fatal("concurrent request bypassed bound")
	}
	if _, err := io.WriteString(writer, "["+validRecord+"]"); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("first request did not complete")
	}
	if first.Code != 202 {
		t.Fatalf("first request failed: %d %s", first.Code, first.Body)
	}
	if w := requestLogs(h, "["+validRecord+"]", "application/json"); w.Code != 202 || p.Pending() != 2 {
		t.Fatal("completed request did not release its slot")
	}
	if counters.Snapshot().Overloaded != 1 {
		t.Fatal("overload counter missing")
	}
}

func TestInvalidUTF8RecordDoesNotRejectValidNeighbor(t *testing.T) {
	h, p, _ := newIngestionHandler(t, 2)
	invalid := strings.Replace(validRecord, `"ok"`, "\"\xff\"", 1)
	w := requestLogs(h, "["+validRecord+","+invalid+"]", "application/json")
	var response struct{ Accepted, Rejected int }
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != 202 || response.Accepted != 1 || response.Rejected != 1 || p.Pending() != 1 {
		t.Fatalf("invalid record invalidated the batch: %d %s", w.Code, w.Body)
	}
}

func TestCompressedRequestIsRejected(t *testing.T) {
	h, p, _ := newIngestionHandler(t, 2)
	r := httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader("["+validRecord+"]"))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 415 || p.Pending() != 0 {
		t.Fatal("unsupported encoding was accepted")
	}
}
