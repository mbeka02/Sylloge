package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/queue"
)

func TestMemoryRoutesAndBoundsEachDestination(t *testing.T) {
	q, err := queue.NewMemory(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, destination := range []event.Destination{event.Service1, event.Service2, event.Service3} {
		if err := q.Send(t.Context(), event.Envelope{ID: string(destination), Destination: destination}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := q.Send(ctx, event.Envelope{ID: "blocked", Destination: event.Service1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("full destination = %v", err)
	}
	for i, destination := range []event.Destination{event.Service1, event.Service2, event.Service3} {
		delivery := <-q.Deliveries(destination)
		if delivery.Envelope.ID != string(destination) || delivery.Source != queue.MemorySource {
			t.Fatalf("misrouted delivery: %+v", delivery)
		}
		if q.Pending() != int64(3-i) {
			t.Fatalf("receiving must not settle a delivery: pending=%d", q.Pending())
		}
		if err := delivery.Settle(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := delivery.Settle(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()
	q.Close()
	for _, destination := range []event.Destination{event.Service1, event.Service2, event.Service3} {
		for delivery := range q.Deliveries(destination) {
			if err := delivery.Settle(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if q.Pending() != 0 {
		t.Fatalf("pending=%d", q.Pending())
	}
	if err := q.Send(t.Context(), event.Envelope{Destination: event.Service1}); !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("send after close=%v", err)
	}
}
