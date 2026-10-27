package ingest_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/ingest"
	"github.com/mbeka02/sylloge/internal/metrics"
	"github.com/mbeka02/sylloge/internal/routing"
)

func newPipeline(t *testing.T, capacity int) (*ingest.Pipeline, *metrics.Registry) {
	t.Helper()
	router, err := routing.New(event.Service3, map[string]event.Destination{"claims-api": event.Service1})
	if err != nil {
		t.Fatal(err)
	}
	counters := new(metrics.Registry)
	pipeline, err := ingest.New(ingest.Options{Capacity: capacity, BatchSize: 50, AdmissionTimeout: 100 * time.Millisecond, Router: router, Metrics: counters, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return pipeline, counters
}

func records(n int) []event.Raw {
	result := make([]event.Raw, n)
	for i := range result {
		result[i] = event.Raw{ID: event.NewID(), BatchID: "batch", Original: []byte(`{"extra":true}`), Log: event.Log{Timestamp: time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC), Service: "claims-api", Level: "INFO", Message: "ok"}, Metadata: event.Metadata{ReceivedAt: time.Date(2026, 10, 27, 12, 0, 0, 123456789, time.FixedZone("EAT", 3*3600)), SourceIP: "192.0.2.1", Environment: "test"}}
	}
	return result
}

func TestAcceptedRecordsAreEnrichedRoutedAndDrained(t *testing.T) {
	p, counters := newPipeline(t, 2)
	raw := records(2)
	raw[1].Log.Service = "unmapped"
	raw[1].Metadata.SourceIP = ""
	raw[1].Metadata.Environment = ""
	if err := p.Admit(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if p.Pending() != 2 {
		t.Fatal("accepted records are not accounted for")
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got []event.Envelope
	if err := p.Run(context.Background(), func(_ context.Context, e event.Envelope) error { got = append(got, e); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Destination != event.Service1 || got[1].Destination != event.Service3 {
		t.Fatalf("incorrect routes: %+v", got)
	}
	if got[0].ReceivedAt != "2026-10-27T09:00:00.123456Z" || got[0].SourceIP != "192.0.2.1" || got[0].Environment != "test" || got[0].ID != raw[0].ID || string(got[0].Original) != string(raw[0].Original) || got[0].Version != 1 {
		t.Fatalf("metadata or identity lost: %+v", got[0])
	}
	if got[1].SourceIP != "unknown" || got[1].Environment != "production" {
		t.Fatal("missing enrichment defaults")
	}
	if snapshot := counters.Snapshot(); snapshot.Admitted != 2 || snapshot.Dispatched != 2 || snapshot.DefaultRouted != 1 || p.Pending() != 0 {
		t.Fatalf("incorrect accounting: %+v pending=%d", snapshot, p.Pending())
	}
}

func TestAdmissionIsAtomicAndHasOneDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newPipeline(t, 2)
		if err := p.Admit(context.Background(), records(1)); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := p.Admit(context.Background(), records(2)); !errors.Is(err, ingest.ErrOverloaded) {
			t.Fatalf("expected overload, got %v", err)
		}
		if time.Since(start) != 100*time.Millisecond || p.Pending() != 1 {
			t.Fatal("admission exceeded its shared deadline or partially enqueued a batch")
		}
		start = time.Now()
		if err := p.Admit(context.Background(), records(3)); !errors.Is(err, ingest.ErrOverloaded) || time.Since(start) != 0 {
			t.Fatal("oversized subset must fail immediately")
		}
		if err := p.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		count := 0
		if err := p.Run(context.Background(), func(context.Context, event.Envelope) error { count++; return nil }); err != nil || count != 1 {
			t.Fatalf("partial admission: count=%d err=%v", count, err)
		}
	})
}

func TestDispatchFailureRetainsRecordsForResume(t *testing.T) {
	p, _ := newPipeline(t, 3)
	raw := records(3)
	if err := p.Admit(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("destination unavailable")
	calls := 0
	err := p.Run(context.Background(), func(context.Context, event.Envelope) error {
		calls++
		if calls == 2 {
			return failure
		}
		return nil
	})
	if !errors.Is(err, failure) || p.Pending() != 2 {
		t.Fatalf("lost pending records after failure: %v", err)
	}
	var ids []string
	if err := p.Run(context.Background(), func(_ context.Context, e event.Envelope) error { ids = append(ids, e.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != raw[1].ID || ids[1] != raw[2].ID || p.Pending() != 0 {
		t.Fatal("resume lost or duplicated a successfully dispatched record")
	}
}

func TestLowVolumeDoesNotWaitToFillFiftyRecords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newPipeline(t, 100)
		if err := p.Admit(context.Background(), records(1)); err != nil {
			t.Fatal(err)
		}
		delivered := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- p.Run(context.Background(), func(context.Context, event.Envelope) error { close(delivered); return nil })
		}()
		select {
		case <-delivered:
		case <-time.After(time.Millisecond):
			t.Fatal("low volume record waited for the batch to fill")
		}
		if err := p.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestConcurrentAdmissionAndConsumerHaveNoLossOrDuplicate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, counters := newPipeline(t, 20)
		seen := make(map[string]bool)
		done := make(chan error, 1)
		go func() {
			done <- p.Run(context.Background(), func(_ context.Context, e event.Envelope) error {
				if seen[e.ID] {
					return errors.New("duplicate event")
				}
				seen[e.ID] = true
				return nil
			})
		}()
		var producers sync.WaitGroup
		for range 20 {
			producers.Go(func() {
				if err := p.Admit(context.Background(), records(10)); err != nil {
					t.Errorf("admit: %v", err)
				}
			})
		}
		producers.Wait()
		if err := p.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if len(seen) != 200 || p.Pending() != 0 || counters.Snapshot().Admitted != 200 {
			t.Fatalf("incorrect custody accounting: seen=%d pending=%d", len(seen), p.Pending())
		}
	})
}

func TestCloseIsSafeWithConcurrentProducers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newPipeline(t, 50)
		var producers sync.WaitGroup
		for range 20 {
			producers.Go(func() {
				err := p.Admit(context.Background(), records(1))
				if err != nil && !errors.Is(err, ingest.ErrClosed) {
					t.Errorf("admit during closure: %v", err)
				}
			})
		}
		if err := p.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		producers.Wait()
		if err := p.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		count := int64(0)
		pending := p.Pending()
		if err := p.Run(context.Background(), func(context.Context, event.Envelope) error { count++; return nil }); err != nil {
			t.Fatal(err)
		}
		if count != pending || p.Pending() != 0 {
			t.Fatal("close dropped accepted records")
		}
		if err := p.Admit(context.Background(), records(1)); !errors.Is(err, ingest.ErrClosed) {
			t.Fatalf("closed pipeline accepted work: %v", err)
		}
	})
}

func TestCancellationRetainsAcceptedRecords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newPipeline(t, 3)
		request, cancelRequest := context.WithCancel(context.Background())
		cancelRequest()
		if err := p.Admit(request, records(1)); !errors.Is(err, context.Canceled) || p.Pending() != 0 {
			t.Fatal("canceled request entered buffer")
		}
		if err := p.Admit(context.Background(), records(3)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- p.Run(ctx, func(ctx context.Context, _ event.Envelope) error { close(started); <-ctx.Done(); return ctx.Err() })
		}()
		<-started
		if err := p.Run(context.Background(), func(context.Context, event.Envelope) error { return nil }); !errors.Is(err, ingest.ErrRunning) {
			t.Fatalf("multiple consumers started: %v", err)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || p.Pending() != 3 {
			t.Fatal("cancellation lost an owned batch")
		}
		if err := p.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := p.Run(context.Background(), func(context.Context, event.Envelope) error { return nil }); err != nil || p.Pending() != 0 {
			t.Fatalf("resume failed: %v", err)
		}
	})
}

func TestAdmissionUsesEarlierRequestDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newPipeline(t, 1)
		if err := p.Admit(context.Background(), records(1)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		start := time.Now()
		if err := p.Admit(ctx, records(1)); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 25*time.Millisecond {
			t.Fatalf("ignored request deadline: %v", err)
		}
	})
}

func TestDefaultRoutingEmitsWarning(t *testing.T) {
	var logs bytes.Buffer
	router, err := routing.New(event.Service2, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := ingest.New(ingest.Options{Capacity: 1, BatchSize: 50, AdmissionTimeout: 100 * time.Millisecond, Router: router, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Admit(context.Background(), records(1)); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Run(context.Background(), func(context.Context, event.Envelope) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "default routing used") || !strings.Contains(logs.String(), "service=claims-api") || !strings.Contains(logs.String(), "destination=service2") {
		t.Fatal("default routing did not produce a diagnostic warning")
	}
}

func BenchmarkEnrichment(b *testing.B) {
	raw := records(1)[0]
	b.ReportAllocs()
	for b.Loop() {
		_ = ingest.Enrich(raw)
	}
}
