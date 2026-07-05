package telemetry

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// The trace must survive the trip through a stream message's field map: what
// the producer injects, the consumer extracts — same trace ID on both sides.
func TestInjectExtractRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	values := map[string]any{"type": "page_view"}
	Inject(ctx, values)

	if _, ok := values["otel.traceparent"]; !ok {
		t.Fatalf("traceparent not injected; values: %v", values)
	}

	got := trace.SpanContextFromContext(Extract(context.Background(), values))
	if got.TraceID() != traceID {
		t.Fatalf("trace ID lost in transit: got %s, want %s", got.TraceID(), traceID)
	}
}

// Without an OTLP endpoint Setup must be a silent no-op, not an error — the
// pipeline runs identically with tracing off.
func TestSetupWithoutEndpointIsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err := Setup(context.Background(), discardLogger(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
