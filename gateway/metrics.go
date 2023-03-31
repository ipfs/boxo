package gateway

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/go-cid"
	prometheus "github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type ipfsBackendWithMetrics struct {
	api           IPFSBackend
	apiCallMetric *prometheus.HistogramVec
}

func newIPFSBackendWithMetrics(api IPFSBackend) *ipfsBackendWithMetrics {
	// We can add buckets as a parameter in the future, but for now using static defaults
	// suggested in https://github.com/ipfs/kubo/issues/8441

	apiCallMetric := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ipfs",
			Subsystem: "gw_backend",
			Name:      "api_call_duration_seconds",
			Help:      "The time spent in IPFSBackend API calls that returned success.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
		},
		[]string{"name", "result"},
	)

	if err := prometheus.Register(apiCallMetric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			apiCallMetric = are.ExistingCollector.(*prometheus.HistogramVec)
		} else {
			log.Errorf("failed to register ipfs_gw_backend_api_call_duration_seconds: %v", err)
		}
	}

	return &ipfsBackendWithMetrics{api, apiCallMetric}
}

func (b *ipfsBackendWithMetrics) updateApiCallMetric(name string, err error, begin time.Time) {
	end := time.Since(begin).Seconds()
	if err == nil {
		b.apiCallMetric.WithLabelValues(name, "success").Observe(end)
	} else {
		b.apiCallMetric.WithLabelValues(name, "failure").Observe(end)
	}
}

func (b *ipfsBackendWithMetrics) Get(ctx context.Context, path ImmutablePath) (ContentPathMetadata, *GetResponse, error) {
	begin := time.Now()
	name := "IPFSBackend.Get"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, n, err := b.api.Get(ctx, path)

	b.updateApiCallMetric(name, err, begin)
	return md, n, err
}

func (b *ipfsBackendWithMetrics) GetRange(ctx context.Context, path ImmutablePath, ranges ...GetRange) (ContentPathMetadata, files.File, error) {
	begin := time.Now()
	name := "IPFSBackend.GetRange"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, f, err := b.api.GetRange(ctx, path, ranges...)

	b.updateApiCallMetric(name, err, begin)
	return md, f, err
}

func (b *ipfsBackendWithMetrics) GetAll(ctx context.Context, path ImmutablePath) (ContentPathMetadata, files.Node, error) {
	begin := time.Now()
	name := "IPFSBackend.GetAll"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, n, err := b.api.GetAll(ctx, path)

	b.updateApiCallMetric(name, err, begin)
	return md, n, err
}

func (b *ipfsBackendWithMetrics) GetBlock(ctx context.Context, path ImmutablePath) (ContentPathMetadata, files.File, error) {
	begin := time.Now()
	name := "IPFSBackend.GetBlock"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, n, err := b.api.GetBlock(ctx, path)

	b.updateApiCallMetric(name, err, begin)
	return md, n, err
}

func (b *ipfsBackendWithMetrics) Head(ctx context.Context, path ImmutablePath) (ContentPathMetadata, files.Node, error) {
	begin := time.Now()
	name := "IPFSBackend.Head"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, n, err := b.api.Head(ctx, path)

	b.updateApiCallMetric(name, err, begin)
	return md, n, err
}

func (b *ipfsBackendWithMetrics) ResolvePath(ctx context.Context, path ImmutablePath) (ContentPathMetadata, error) {
	begin := time.Now()
	name := "IPFSBackend.ResolvePath"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, err := b.api.ResolvePath(ctx, path)

	b.updateApiCallMetric(name, err, begin)
	return md, err
}

func (b *ipfsBackendWithMetrics) GetCAR(ctx context.Context, path ImmutablePath) (ContentPathMetadata, io.ReadCloser, <-chan error, error) {
	begin := time.Now()
	name := "IPFSBackend.GetCAR"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, rc, errCh, err := b.api.GetCAR(ctx, path)

	// TODO: handle errCh
	b.updateApiCallMetric(name, err, begin)
	return md, rc, errCh, err
}

func (b *ipfsBackendWithMetrics) IsCached(ctx context.Context, path path.Path) bool {
	begin := time.Now()
	name := "IPFSBackend.IsCached"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	bln := b.api.IsCached(ctx, path)

	b.updateApiCallMetric(name, nil, begin)
	return bln
}

func (b *ipfsBackendWithMetrics) GetIPNSRecord(ctx context.Context, cid cid.Cid) ([]byte, error) {
	begin := time.Now()
	name := "IPFSBackend.GetIPNSRecord"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("cid", cid.String())))
	defer span.End()

	r, err := b.api.GetIPNSRecord(ctx, cid)

	b.updateApiCallMetric(name, err, begin)
	return r, err
}

func (b *ipfsBackendWithMetrics) ResolveMutable(ctx context.Context, path path.Path) (ImmutablePath, error) {
	begin := time.Now()
	name := "IPFSBackend.ResolveMutable"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	p, err := b.api.ResolveMutable(ctx, path)

	b.updateApiCallMetric(name, err, begin)
	return p, err
}

func (b *ipfsBackendWithMetrics) GetDNSLinkRecord(ctx context.Context, fqdn string) (path.Path, error) {
	begin := time.Now()
	name := "IPFSBackend.GetDNSLinkRecord"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("fqdn", fqdn)))
	defer span.End()

	p, err := b.api.GetDNSLinkRecord(ctx, fqdn)

	b.updateApiCallMetric(name, err, begin)
	return p, err
}

var _ IPFSBackend = (*ipfsBackendWithMetrics)(nil)

func newHandlerWithMetrics(c Config, api IPFSBackend) *handler {
	i := &handler{
		config: c,
		api:    newIPFSBackendWithMetrics(api),
		// Improved Metrics
		// ----------------------------
		// Time till the first content block (bar in /ipfs/cid/foo/bar)
		// (format-agnostic, across all response types)
		firstContentBlockGetMetric: newHistogramMetric(
			"gw_first_content_block_get_latency_seconds",
			"The time till the first content block is received on GET from the gateway.",
		),

		// Response-type specific metrics
		// ----------------------------
		// Generic: time it takes to execute a successful gateway request (all request types)
		getMetric: newHistogramMetric(
			"gw_get_duration_seconds",
			"The time to GET a successful response to a request (all content types).",
		),
		// UnixFS: time it takes to return a file
		unixfsFileGetMetric: newHistogramMetric(
			"gw_unixfs_file_get_duration_seconds",
			"The time to serve an entire UnixFS file from the gateway.",
		),
		// UnixFS: time it takes to find and serve an index.html file on behalf of a directory.
		unixfsDirIndexGetMetric: newHistogramMetric(
			"gw_unixfs_dir_indexhtml_get_duration_seconds",
			"The time to serve an index.html file on behalf of a directory from the gateway. This is a subset of gw_unixfs_file_get_duration_seconds.",
		),
		// UnixFS: time it takes to generate static HTML with directory listing
		unixfsGenDirListingGetMetric: newHistogramMetric(
			"gw_unixfs_gen_dir_listing_get_duration_seconds",
			"The time to serve a generated UnixFS HTML directory listing from the gateway.",
		),
		// CAR: time it takes to return requested CAR stream
		carStreamGetMetric: newHistogramMetric(
			"gw_car_stream_get_duration_seconds",
			"The time to GET an entire CAR stream from the gateway.",
		),
		// Block: time it takes to return requested Block
		rawBlockGetMetric: newHistogramMetric(
			"gw_raw_block_get_duration_seconds",
			"The time to GET an entire raw Block from the gateway.",
		),
		// TAR: time it takes to return requested TAR stream
		tarStreamGetMetric: newHistogramMetric(
			"gw_tar_stream_get_duration_seconds",
			"The time to GET an entire TAR stream from the gateway.",
		),
		// JSON/CBOR: time it takes to return requested DAG-JSON/-CBOR document
		jsoncborDocumentGetMetric: newHistogramMetric(
			"gw_jsoncbor_get_duration_seconds",
			"The time to GET an entire DAG-JSON/CBOR block from the gateway.",
		),
		// IPNS Record: time it takes to return IPNS record
		ipnsRecordGetMetric: newHistogramMetric(
			"gw_ipns_record_get_duration_seconds",
			"The time to GET an entire IPNS Record from the gateway.",
		),

		// Legacy Metrics
		// ----------------------------
		unixfsGetMetric: newSummaryMetric( // TODO: remove?
			// (deprecated, use firstContentBlockGetMetric instead)
			"unixfs_get_latency_seconds",
			"DEPRECATED: does not do what you think, use gw_first_content_block_get_latency_seconds instead.",
		),
	}
	return i
}

func newSummaryMetric(name string, help string) *prometheus.SummaryVec {
	summaryMetric := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "ipfs",
			Subsystem: "http",
			Name:      name,
			Help:      help,
		},
		[]string{"gateway"},
	)
	if err := prometheus.Register(summaryMetric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			summaryMetric = are.ExistingCollector.(*prometheus.SummaryVec)
		} else {
			log.Errorf("failed to register ipfs_http_%s: %v", name, err)
		}
	}
	return summaryMetric
}

func newHistogramMetric(name string, help string) *prometheus.HistogramVec {
	// We can add buckets as a parameter in the future, but for now using static defaults
	// suggested in https://github.com/ipfs/kubo/issues/8441
	defaultBuckets := []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60}
	histogramMetric := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ipfs",
			Subsystem: "http",
			Name:      name,
			Help:      help,
			Buckets:   defaultBuckets,
		},
		[]string{"gateway"},
	)
	if err := prometheus.Register(histogramMetric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			histogramMetric = are.ExistingCollector.(*prometheus.HistogramVec)
		} else {
			log.Errorf("failed to register ipfs_http_%s: %v", name, err)
		}
	}
	return histogramMetric
}

// spanTrace starts a new span using the standard IPFS tracing conventions.
func spanTrace(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("boxo").Start(ctx, fmt.Sprintf("%s.%s", " Gateway", spanName), opts...)
}
