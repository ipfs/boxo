# Tracing

Tracing across the stack follows, as much as possible, the [Open Telemetry]
specifications. Configuration environment variables are specified in the
[OpenTelemetry Environment Variable Specification].

We use the [opentelemetry-go] package, which currently does not have default support
for the `OTEL_TRACES_EXPORTER` environment variables. Therefore, we provide some
helper functions under [`boxo/tracing`](../tracing/) to support these.

In this document, we document the quirks of our custom support for the `OTEL_TRACES_EXPORTER`,
as well as examples of how to use tracing, create traceable headers, and how
to use the Jaeger UI. The [Gateway examples](../examples/gateway/) fully support Tracing.

- [Environment Variables](#environment-variables)
  - [`OTEL_TRACES_EXPORTER`](#otel_traces_exporter)
  - [`OTLP Exporter`](#otlp-exporter)
  - [`Zipkin Exporter`](#zipkin-exporter)
  - [`File Exporter`](#file-exporter)
  - [`OTEL_PROPAGATORS`](#otel_propagators)
- [Using Jaeger UI](#using-jaeger-ui)
- [Generate `traceparent` Header](#generate-traceparent-header)

## Environment Variables

For advanced configurations, such as ratio-based sampling, please see also the
[OpenTelemetry Environment Variable Specification].

### `OTEL_TRACES_EXPORTER`

Specifies the exporters to use as a comma-separated string. Each exporter has a
set of additional environment variables used to configure it. The following values
are supported:

- `otlp`
- `zipkin`
- `stdout`
- `file` -- appends traces to a JSON file on the filesystem

Default: `""` (no exporters)

### `OTLP Exporter`

Unless specified in this section, the OTLP exporter uses the environment variables
documented in [OpenTelemetry Protocol Exporter].

#### `OTEL_EXPORTER_OTLP_PROTOCOL`
Specifies the OTLP protocol to use, which is one of:

- `grpc`
- `http/protobuf`

Default: `"grpc"`

### `Zipkin Exporter`

See [Zipkin Exporter](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/configuration/sdk-environment-variables.md#zipkin-exporter).

### `File Exporter`

#### `OTEL_EXPORTER_FILE_PATH`

Specifies the filesystem path for the JSON file.

Default: `"$PWD/traces.json"`

### `OTEL_PROPAGATORS`

See [General SDK Configuration](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/configuration/sdk-environment-variables.md#general-sdk-configuration).

## Using Jaeger UI

One can use the `jaegertracing/all-in-one` Docker image to run a full Jaeger stack
and configure the Kubo daemon, or gateway examples, to publish traces to it. Here, in an
ephemeral container:

```console
$ docker run -d --rm --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -p 5775:5775/udp \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 14250:14250 \
  -p 14268:14268 \
  -p 14269:14269 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 9411:9411 \
  jaegertracing/all-in-one
```

Then, in other terminal, start the app that uses `boxo/tracing` internally (e.g., a Kubo daemon), with Jaeger exporter enabled:

```console
$ OTEL_EXPORTER_OTLP_INSECURE=true OTEL_TRACES_EXPORTER=otlp ipfs daemon --init
```

Finally, the [Jaeger UI] is available at http://localhost:16686.

## Generate `traceparent` Header

If you want to trace a specific request and want to have its tracing ID, you can
generate a `Traceparent` header. According to the [Trace Context] specification,
the header is formed as follows:

> ```
> version-format   = trace-id "-" parent-id "-" trace-flags
> trace-id         = 32HEXDIGLC  ; 16 bytes array identifier. All zeroes forbidden
> parent-id        = 16HEXDIGLC  ; 8 bytes array identifier. All zeroes forbidden
> trace-flags      = 2HEXDIGLC   ; 8 bit flags. Currently, only one bit is used. See below for details
> ```

To generate a valid `Traceparent` header value, the following script can be used:

```bash
version="00" # fixed in spec at 00
trace_id="$(cat /dev/urandom | tr -dc 'a-f0-9' | fold -w 32 | head -n 1)"
parent_id="00$(cat /dev/urandom | tr -dc 'a-f0-9' | fold -w 14 | head -n 1)"
trace_flag="01"   # sampled
traceparent="$version-$trace_id-$parent_id-$trace_flag"
echo $traceparent
```

**NOTE**: the `tr` command behaves differently on macOS. You may want to install
the GNU `tr` (`gtr`) and use it instead.

Then, the value can be passed onto the request with `curl -H "Traceparent: $traceparent" URL`.
If using Jaeger, you can now search by the trace with ID `$trace_id` and see
the complete trace of this request.

[Open Telemetry]: https://opentelemetry.io/
[opentelemetry-go]: https://github.com/open-telemetry/opentelemetry-go
[Trace Context]: https://www.w3.org/TR/trace-context
[OpenTelemetry Environment Variable Specification]: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/configuration/sdk-environment-variables.md
[OpenTelemetry Protocol Exporter]: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md
[Jaeger UI]: https://github.com/jaegertracing/jaeger-ui
