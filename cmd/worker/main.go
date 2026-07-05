// worker consumes the event stream and maintains aggregate counters.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/Sebjen0209/k8s-event-pipeline/internal/config"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/stats"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := config.EnvOr("LISTEN_ADDR", ":8080")
	redisAddr := config.EnvOr("REDIS_ADDR", "localhost:6379")
	streamName := config.EnvOr("STREAM_NAME", "events")
	group := config.EnvOr("CONSUMER_GROUP", "aggregators")
	// Pod name via the downward API in the chart; hostname works everywhere else.
	consumer := config.EnvOr("CONSUMER_NAME", hostname())

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())

	w := worker.New(log, rdb, stats.NewStore(rdb), streamName, group, consumer, reg)

	// Probes + metrics; the worker has no other HTTP surface.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(rw http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		rw.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	httpSrv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("probe server failed", "err", err)
			os.Exit(1)
		}
	}()

	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("worker failed", "err", err)
		os.Exit(1)
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

func hostname() string {
	if h, err := os.Hostname(); err == nil {
		return h
	}
	return "worker"
}
