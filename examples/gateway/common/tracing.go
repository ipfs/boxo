package common

import (
	"context"

	"github.com/ipfs/boxo/tracing"
	"go.opentelemetry.io/contrib/propagators/autoprop"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// SetupTracing sets up the tracing based on the OTEL_* environment variables,
// and the provided service name. It returns a trace.TracerProvider.
func SetupTracing(ctx context.Context, serviceName string) (*trace.TracerProvider, error) {
	tp, err := NewTracerProvider(ctx, serviceName)
	if err != nil {
		return nil, err
	}

	// Sets the default trace provider for this process. If this is not done, tracing
	// will not be enabled. Please note that this will apply to the entire process
	// as it is set as the default tracer, as per OTel recommendations.
	otel.SetTracerProvider(tp)

	// Configures the default propagators used by the Open Telemetry library. By
	// using autoprop.NewTextMapPropagator, we ensure the value of the environmental
	// variable OTEL_PROPAGATORS is respected, if set. By default, Trace Context
	// and Baggage are used. More details on:
	// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/configuration/sdk-environment-variables.md
	otel.SetTextMapPropagator(autoprop.NewTextMapPropagator())

	return tp, nil
}

// NewTracerProvider creates and configures a TracerProvider.
func NewTracerProvider(ctx context.Context, serviceName string) (*trace.TracerProvider, error) {
	exporters, err := tracing.NewSpanExporters(ctx)
	if err != nil {
		return nil, err
	}

	options := []trace.TracerProviderOption{}

	for _, exporter := range exporters {
		options = append(options, trace.WithBatcher(exporter))
	}

	r, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}
	options = append(options, trace.WithResource(r))
	return trace.NewTracerProvider(options...), nil
}
