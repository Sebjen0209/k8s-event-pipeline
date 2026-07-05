// Package stream wraps the Redis Stream that carries events from the ingest
// API to the aggregation workers.
package stream

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Event is one message on the stream. Payload stays an opaque JSON string —
// the pipeline only routes and counts; it never interprets business data.
type Event struct {
	Type    string `json:"type"`
	Source  string `json:"source"`
	Payload string `json:"payload,omitempty"`
	TS      string `json:"ts"`
}

type Producer struct {
	rdb    *redis.Client
	stream string
	maxLen int64
}

// NewProducer writes to the given stream, trimming it to ~maxLen entries so a
// stalled consumer can never grow Redis without bound.
func NewProducer(rdb *redis.Client, stream string, maxLen int64) *Producer {
	return &Producer{rdb: rdb, stream: stream, maxLen: maxLen}
}

// Add appends an event and returns the stream-assigned ID.
func (p *Producer) Add(ctx context.Context, e Event) (string, error) {
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: p.stream,
		MaxLen: p.maxLen,
		Approx: true,
		Values: map[string]any{
			"type":    e.Type,
			"source":  e.Source,
			"payload": e.Payload,
			"ts":      e.TS,
		},
	}).Result()
}

// FromValues rebuilds an Event from the raw field map of a stream message.
func FromValues(values map[string]any) Event {
	str := func(k string) string {
		if v, ok := values[k].(string); ok {
			return v
		}
		return ""
	}
	return Event{Type: str("type"), Source: str("source"), Payload: str("payload"), TS: str("ts")}
}
