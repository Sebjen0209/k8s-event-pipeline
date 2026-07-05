// Package api implements the HTTP surface of the ingest service.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/Sebjen0209/k8s-event-pipeline/internal/stats"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/stream"
)

const (
	maxBodyBytes   = 64 << 10 // request bodies above 64 KiB are rejected outright
	maxPayloadSize = 8 << 10  // opaque payload is capped so one client can't bloat the stream
)

// Event names must be usable as metric labels and Redis hash fields.
var nameRe = regexp.MustCompile(`^[a-zA-Z0-9_.:-]{1,64}$`)

type Server struct {
	log      *slog.Logger
	rdb      *redis.Client
	producer *stream.Producer
	stats    *stats.Store
	registry *prometheus.Registry
	ingested prometheus.Counter
}

func New(log *slog.Logger, rdb *redis.Client, producer *stream.Producer, store *stats.Store, reg *prometheus.Registry) *Server {
	return &Server{
		log:      log,
		rdb:      rdb,
		producer: producer,
		stats:    store,
		registry: reg,
		ingested: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "ingest_events_total",
			Help: "Events accepted onto the stream.",
		}),
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", s.handleIngest)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	return mux
}

type ingestRequest struct {
	Type    string          `json:"type"`
	Source  string          `json:"source"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		s.fail(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if !nameRe.MatchString(req.Type) || !nameRe.MatchString(req.Source) {
		s.fail(w, http.StatusBadRequest, "type and source are required: 1-64 chars of [a-zA-Z0-9_.:-]")
		return
	}
	if len(req.Payload) > maxPayloadSize {
		s.fail(w, http.StatusBadRequest, "payload exceeds 8 KiB")
		return
	}

	id, err := s.producer.Add(r.Context(), stream.Event{
		Type:    req.Type,
		Source:  req.Source,
		Payload: string(req.Payload),
		TS:      time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		s.log.Error("enqueue failed", "err", err)
		s.fail(w, http.StatusServiceUnavailable, "event store unavailable")
		return
	}

	s.ingested.Inc()
	// 202: the event is durably queued but aggregation happens asynchronously.
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	snap, err := s.stats.Snapshot(r.Context())
	if err != nil {
		s.log.Error("stats read failed", "err", err)
		s.fail(w, http.StatusServiceUnavailable, "event store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		s.fail(w, http.StatusServiceUnavailable, "redis unreachable")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) fail(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
