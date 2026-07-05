// Package telemetry wires OpenTelemetry tracing. Tracing is opt-in: with no
// OTEL_EXPORTER_OTLP_ENDPOINT configured the provider is a no-op, but trace
// context is still propagated so downstream systems keep their correlation.
package telemetry

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Setup configures the global tracer provider and W3C propagators.
// The returned shutdown function flushes pending spans.
func Setup(ctx context.Context, log *slog.Logger, serviceName string) (func(context.Context) error, error) {
	// Propagation is always on — it costs nothing and keeps traceparent
	// flowing through the pipeline even when this hop doesn't export.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		log.Info("tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT unset)")
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}

	res, err := sdkresource.Merge(sdkresource.Default(), sdkresource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	log.Info("tracing enabled", "endpoint", endpoint, "service", serviceName)

	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return tp.Shutdown(ctx)
	}, nil
}

// Inject writes the current trace context into a stream message's field map,
// carrying the trace across the async Redis Stream boundary.
func Inject(ctx context.Context, values map[string]any) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		values["otel."+k] = v
	}
}

// Extract rebuilds the producer's trace context from a stream message's
// field map, so the consumer span joins the same trace.
func Extract(ctx context.Context, values map[string]any) context.Context {
	carrier := propagation.MapCarrier{}
	for k, v := range values {
		if s, ok := v.(string); ok && len(k) > 5 && k[:5] == "otel." {
			carrier[k[5:]] = s
		}
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
