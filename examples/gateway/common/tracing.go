package common

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/contrib/propagators/autoprop"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// SetupTracing setups Open Telemetry tracing with some defaults that will enable
// tracing in this system. Please note that in a real-case scenario this would
// look slightly different and include more configurations.
func SetupTracing(serviceName string) (*trace.TracerProvider, error) {
	// Creates the resources to create a new tracing provider.
	r, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	// Creates a new tracing provider. In a real-case scenario, this trace provider
	// will likely be configured with exporters in order to be able to access
	// the tracing information. https://opentelemetry.io/docs/instrumentation/go/exporters/
	tp := trace.NewTracerProvider(
		trace.WithResource(r),
	)

	// Sets the default trace provider for this process. If this is not done, tracing
	// will not be enabled. Please note that this will apply to the entire process
	// as it is set as the default tracer.
	otel.SetTracerProvider(tp)

	// Configures the default propagators used by the Open Telemetry library. By
	// using autoprop.NewTextMapPropagator, we ensure the value of the environmental
	// variable OTEL_PROPAGATORS is respected, if set. By default, Trace Context
	// and Baggage are used. More details on:
	// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/sdk-environment-variables.md
	otel.SetTextMapPropagator(autoprop.NewTextMapPropagator())

	return tp, nil
}

// NewClient creates a new HTTP Client wraped with the OTel transport. This will
// ensure correct propagation of tracing headers across multiple HTTP requests when
// Open Telemetry is configured. Please note that NewClient will use the default
// global trace provider and propagators. Therefore, SetupTracing must be called first.
func NewClient() *http.Client {
	return &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
}
