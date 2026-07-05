package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Sebjen0209/k8s-event-pipeline/internal/stats"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/stream"
)

func newTestServer(t *testing.T) (*Server, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(log, rdb, stream.NewProducer(rdb, "events", 1000), stats.NewStore(rdb), prometheus.NewRegistry())
	return s, mr, rdb
}

func TestIngestAccepted(t *testing.T) {
	s, _, rdb := newTestServer(t)

	body := `{"type":"page_view","source":"web","payload":{"path":"/"}}`
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body)))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body: %s)", rec.Code, rec.Body)
	}
	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp["id"] == "" {
		t.Fatalf("expected a stream id, got %q (err %v)", rec.Body, err)
	}
	if n, _ := rdb.XLen(context.Background(), "events").Result(); n != 1 {
		t.Fatalf("stream length = %d, want 1", n)
	}
}

func TestIngestRejectsBadInput(t *testing.T) {
	s, _, _ := newTestServer(t)

	cases := map[string]string{
		"not json":        `{"type":`,
		"missing type":    `{"source":"web"}`,
		"missing source":  `{"type":"page_view"}`,
		"bad characters":  `{"type":"page view!","source":"web"}`,
		"unknown fields":  `{"type":"a","source":"b","extra":true}`,
		"oversized":       `{"type":"a","source":"b","payload":"` + strings.Repeat("x", 9<<10) + `"}`,
		"name too long":   `{"type":"` + strings.Repeat("a", 65) + `","source":"web"}`,
	}
	for name, body := range cases {
		rec := httptest.NewRecorder()
		s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body)))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
	}
}

func TestStats(t *testing.T) {
	s, _, rdb := newTestServer(t)
	store := stats.NewStore(rdb)

	// Empty store reads as zeroes, not an error.
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("empty stats status = %d, want 200", rec.Code)
	}

	ctx := context.Background()
	for range 2 {
		if err := store.Record(ctx, "page_view", "web", "2026-01-01T00:00:00Z"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Record(ctx, "purchase", "app", "2026-01-01T00:00:01Z"); err != nil {
		t.Fatal(err)
	}

	rec = httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/stats", nil))
	var snap stats.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Total != 3 || snap.ByType["page_view"] != 2 || snap.BySource["app"] != 1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	if snap.LastEventAt != "2026-01-01T00:00:01Z" {
		t.Fatalf("lastEventAt = %q", snap.LastEventAt)
	}
}

func TestReadyz(t *testing.T) {
	s, mr, _ := newTestServer(t)

	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz with redis up = %d, want 200", rec.Code)
	}

	mr.Close()
	rec = httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with redis down = %d, want 503", rec.Code)
	}
}
