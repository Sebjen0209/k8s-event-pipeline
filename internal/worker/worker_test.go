package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Sebjen0209/k8s-event-pipeline/internal/stats"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/stream"
)

func TestWorkerAggregatesAndAcks(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer := stream.NewProducer(rdb, "events", 1000)
	for _, e := range []stream.Event{
		{Type: "page_view", Source: "web", TS: "t1"},
		{Type: "page_view", Source: "web", TS: "t2"},
		{Type: "purchase", Source: "app", TS: "t3"},
	} {
		if _, err := producer.Add(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	store := stats.NewStore(rdb)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := New(log, rdb, store, "events", "aggregators", "test-1", prometheus.NewRegistry())
	w.readBlock = 50 * time.Millisecond // a blocking read isn't interruptible; keep shutdown fast in tests

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()

	// The worker consumes asynchronously; poll until all three events landed.
	deadline := time.After(5 * time.Second)
	for {
		snap, err := store.Snapshot(ctx)
		if err == nil && snap.Total == 3 {
			if snap.ByType["page_view"] != 2 || snap.ByType["purchase"] != 1 || snap.BySource["web"] != 2 {
				t.Fatalf("unexpected aggregates: %+v", snap)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for aggregation; last snapshot: %+v (err %v)", snap, err)
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Everything processed must also be acked — nothing left pending.
	pending, err := rdb.XPending(ctx, "events", "aggregators").Result()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending = %d, want 0", pending.Count)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop on context cancel")
	}
}
