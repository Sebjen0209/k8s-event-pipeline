// Package worker consumes the event stream and maintains the aggregates.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"github.com/Sebjen0209/k8s-event-pipeline/internal/stats"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/stream"
)

const (
	defaultReadBlock = 5 * time.Second // wake up regularly so ctx cancellation is honoured
	batchSize        = 64
	// Messages another consumer read but never acked (e.g. its pod died
	// mid-batch) become claimable after this long.
	claimMinIdle = 30 * time.Second
)

type Worker struct {
	log       *slog.Logger
	rdb       *redis.Client
	stats     *stats.Store
	stream    string
	group     string
	consumer  string
	processed prometheus.Counter
	// readBlock bounds how long one XREADGROUP blocks — and therefore how
	// long shutdown can take, since a blocking read isn't interruptible.
	readBlock time.Duration
}

func New(log *slog.Logger, rdb *redis.Client, store *stats.Store, streamName, group, consumer string, reg *prometheus.Registry) *Worker {
	return &Worker{
		log:      log,
		rdb:      rdb,
		stats:    store,
		stream:   streamName,
		group:    group,
		consumer: consumer,
		processed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "worker_events_processed_total",
			Help: "Events folded into the aggregates.",
		}),
		readBlock: defaultReadBlock,
	}
}

// Run consumes until ctx is cancelled. Semantics are at-least-once: an event
// is acked only after its aggregate update succeeded, so a crash between the
// two re-delivers rather than drops.
func (w *Worker) Run(ctx context.Context) error {
	// "0" (not "$") so events published before the first worker came up are
	// still counted. BUSYGROUP just means another replica won the race.
	err := w.rdb.XGroupCreateMkStream(ctx, w.stream, w.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	w.log.Info("worker started", "stream", w.stream, "group", w.group, "consumer", w.consumer)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := w.claimAbandoned(ctx); err != nil && ctx.Err() == nil {
			w.log.Error("autoclaim failed", "err", err)
		}

		res, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    w.group,
			Consumer: w.consumer,
			Streams:  []string{w.stream, ">"},
			Count:    batchSize,
			Block:    w.readBlock,
		}).Result()
		if errors.Is(err, redis.Nil) {
			continue // block timed out with nothing to read
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.log.Error("read failed", "err", err)
			time.Sleep(time.Second) // don't hot-loop against a broken Redis
			continue
		}

		for _, str := range res {
			w.process(ctx, str.Messages)
		}
	}
}

// claimAbandoned takes over pending messages whose consumer went away.
func (w *Worker) claimAbandoned(ctx context.Context) error {
	msgs, _, err := w.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   w.stream,
		Group:    w.group,
		Consumer: w.consumer,
		MinIdle:  claimMinIdle,
		Start:    "0-0",
		Count:    batchSize,
	}).Result()
	if err != nil {
		return err
	}
	if len(msgs) > 0 {
		w.log.Info("claimed abandoned messages", "count", len(msgs))
		w.process(ctx, msgs)
	}
	return nil
}

func (w *Worker) process(ctx context.Context, msgs []redis.XMessage) {
	for _, msg := range msgs {
		e := stream.FromValues(msg.Values)
		if err := w.stats.Record(ctx, e.Type, e.Source, e.TS); err != nil {
			// Leave unacked: it stays pending and is retried/claimed later.
			w.log.Error("record failed", "id", msg.ID, "err", err)
			continue
		}
		if err := w.rdb.XAck(ctx, w.stream, w.group, msg.ID).Err(); err != nil {
			w.log.Error("ack failed", "id", msg.ID, "err", err)
			continue
		}
		w.processed.Inc()
	}
}
