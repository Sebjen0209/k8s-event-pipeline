// Package stats maintains and reads the aggregates the worker builds from the
// event stream.
package stats

import (
	"context"
	"errors"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const (
	byTypeKey    = "stats:by_type"
	bySourceKey  = "stats:by_source"
	totalKey     = "stats:total"
	lastEventKey = "stats:last_event_at"
)

type Store struct{ rdb *redis.Client }

func NewStore(rdb *redis.Client) *Store { return &Store{rdb: rdb} }

// Record folds one event into the aggregates. One pipeline keeps round-trips
// per message constant regardless of how many counters we maintain.
func (s *Store) Record(ctx context.Context, eventType, source, ts string) error {
	pipe := s.rdb.Pipeline()
	pipe.HIncrBy(ctx, byTypeKey, eventType, 1)
	pipe.HIncrBy(ctx, bySourceKey, source, 1)
	pipe.Incr(ctx, totalKey)
	pipe.Set(ctx, lastEventKey, ts, 0)
	_, err := pipe.Exec(ctx)
	return err
}

type Snapshot struct {
	Total       int64            `json:"total"`
	ByType      map[string]int64 `json:"byType"`
	BySource    map[string]int64 `json:"bySource"`
	LastEventAt string           `json:"lastEventAt,omitempty"`
}

// Snapshot reads the current aggregates in one pipeline.
func (s *Store) Snapshot(ctx context.Context) (*Snapshot, error) {
	pipe := s.rdb.Pipeline()
	byType := pipe.HGetAll(ctx, byTypeKey)
	bySource := pipe.HGetAll(ctx, bySourceKey)
	total := pipe.Get(ctx, totalKey)
	last := pipe.Get(ctx, lastEventKey)
	// redis.Nil just means some keys don't exist yet (no events recorded).
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	snap := &Snapshot{
		ByType:      toCounts(byType.Val()),
		BySource:    toCounts(bySource.Val()),
		LastEventAt: last.Val(),
	}
	if n, err := strconv.ParseInt(total.Val(), 10, 64); err == nil {
		snap.Total = n
	}
	return snap, nil
}

func toCounts(m map[string]string) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			out[k] = n
		}
	}
	return out
}
