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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Sebjen0209/k8s-event-pipeline/internal/stats"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/stream"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/telemetry"
)

const (
	defaultReadBlock = 5 * time.Second // wake up regularly so ctx cancellation is honoured
	batchSize        = 64
	// Messages another consumer read but never acked (e.g. its pod died
	// mid-batch) become claimable after this long.
	claimMinIdle = 30 * time.Second
	// Scrape-time Redis lookups for the queue gauges must never stall /metrics.
	gaugeTimeout = 500 * time.Millisecond
)

type Worker struct {
	log       *slog.Logger
	rdb       *redis.Client
	stats     *stats.Store
	stream    string
	group     string
	consumer  string
	processed prometheus.Counter
	procTime  prometheus.Histogram
	// readBlock bounds how long one XREADGROUP blocks — and therefore how
	// long shutdown can take, since a blocking read isn't interruptible.
	readBlock time.Duration
}

func New(log *slog.Logger, rdb *redis.Client, store *stats.Store, streamName, group, consumer string, reg *prometheus.Registry) *Worker {
	w := &Worker{
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
		procTime: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "worker_process_duration_seconds",
			Help:    "Time to fold one event into the aggregates (record + ack).",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12), // 0.5ms .. ~1s
		}),
		readBlock: defaultReadBlock,
	}

	// Queue-health gauges are computed at scrape time: the alerting-relevant
	// numbers (backlog, pending, is-redis-there-at-all) come straight from
	// Redis rather than from worker-local counters that die with the pod.
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "worker_stream_length",
		Help: "Entries currently in the event stream (backlog incl. unread).",
	}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), gaugeTimeout)
		defer cancel()
		n, err := rdb.XLen(ctx, streamName).Result()
		if err != nil {
			return -1 // scrape must not fail; -1 marks "unknown" for the dashboard
		}
		return float64(n)
	})
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "worker_stream_pending",
		Help: "Messages delivered to the consumer group but not yet acked.",
	}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), gaugeTimeout)
		defer cancel()
		p, err := rdb.XPending(ctx, streamName, group).Result()
		if err != nil {
			return -1
		}
		return float64(p.Count)
	})
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "worker_redis_up",
		Help: "1 when Redis answers PING from this worker, else 0.",
	}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), gaugeTimeout)
		defer cancel()
		if rdb.Ping(ctx).Err() != nil {
			return 0
		}
		return 1
	})

	return w
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

		// NOGROUP here is handled by the read path below; don't double-log it.
		if err := w.claimAbandoned(ctx); err != nil && ctx.Err() == nil && !strings.Contains(err.Error(), "NOGROUP") {
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
			// NOGROUP: Redis lost its state (restart on empty storage) and
			// the consumer group vanished with it. Recreate and resume —
			// found the hard way in chaos experiment 001, where the worker
			// error-looped here forever while the API kept accepting events.
			if strings.Contains(err.Error(), "NOGROUP") {
				w.log.Warn("consumer group gone (redis state lost) — recreating", "group", w.group)
				if cerr := w.rdb.XGroupCreateMkStream(ctx, w.stream, w.group, "0").Err(); cerr != nil && !strings.Contains(cerr.Error(), "BUSYGROUP") {
					w.log.Error("recreate group failed", "err", cerr)
					time.Sleep(time.Second)
				}
				continue
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
		// Join the trace the producer started: the consumer span shares the
		// trace that began at the original HTTP request.
		msgCtx, span := otel.Tracer("worker").Start(
			telemetry.Extract(ctx, msg.Values),
			w.stream+" process",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.system", "redis"),
				attribute.String("messaging.destination.name", w.stream),
				attribute.String("messaging.message.id", msg.ID),
			))

		start := time.Now()
		e := stream.FromValues(msg.Values)
		if err := w.stats.Record(msgCtx, e.Type, e.Source, e.TS); err != nil {
			// Leave unacked: it stays pending and is retried/claimed later.
			w.log.Error("record failed", "id", msg.ID, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "record failed")
			span.End()
			continue
		}
		if err := w.rdb.XAck(msgCtx, w.stream, w.group, msg.ID).Err(); err != nil {
			w.log.Error("ack failed", "id", msg.ID, "err", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "ack failed")
			span.End()
			continue
		}
		w.procTime.Observe(time.Since(start).Seconds())
		w.processed.Inc()
		span.End()
	}
}
