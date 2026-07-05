// ingest-api accepts events over HTTP and enqueues them on a Redis Stream.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"

	"github.com/Sebjen0209/k8s-event-pipeline/internal/api"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/config"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/stats"
	"github.com/Sebjen0209/k8s-event-pipeline/internal/stream"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := config.EnvOr("LISTEN_ADDR", ":8080")
	redisAddr := config.EnvOr("REDIS_ADDR", "localhost:6379")
	streamName := config.EnvOr("STREAM_NAME", "events")
	maxLen, err := strconv.ParseInt(config.EnvOr("STREAM_MAXLEN", "100000"), 10, 64)
	if err != nil {
		log.Error("invalid STREAM_MAXLEN", "err", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())

	srv := api.New(log, rdb, stream.NewProducer(rdb, streamName, maxLen), stats.NewStore(rdb), reg)
	httpSrv := &http.Server{Addr: addr, Handler: srv.Routes(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("ingest-api listening", "addr", addr, "redis", redisAddr, "stream", streamName)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	// Kubernetes sends SIGTERM before removing the pod from endpoints; give
	// in-flight requests a grace window instead of dropping them.
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}
