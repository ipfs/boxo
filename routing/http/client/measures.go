package client

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	// distMS defines the histogram buckets for latency in milliseconds.
	// Similar to distLength, bucket selection uses value < boundary:
	//   - 0ms → bucket with bound 1ms
	//   - 1ms → bucket with bound 2ms
	//   - 50ms → bucket with bound 100ms
	//   - etc.
	//
	// In Prometheus:
	//   - routing_http_client_latency_bucket{le="1"} = requests with latency 0ms
	//   - routing_http_client_latency_bucket{le="100"} = requests with latency <100ms
	//   - routing_http_client_latency_bucket{le="1000"} = requests with latency <1s
	distMS = view.Distribution(0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000)
	// distLength defines the histogram buckets for result counts.
	// In OpenCensus, bucket selection uses value < boundary comparisons:
	//   - Value 0 → bucket with bound 1
	//   - Value 1 → bucket with bound 2
	//   - Value 2 → bucket with bound 5
	//   - etc.
	//
	// When exported to Prometheus, these become cumulative "le" (less than) buckets:
	//   - routing_http_client_length_bucket{le="1"} = count of operations with 0 results
	//   - routing_http_client_length_bucket{le="2"} = count of operations with 0 or 1 results
	//   - routing_http_client_length_bucket{le="5"} = count of operations with 0-4 results
	//
	// To determine specific counts from Prometheus metrics:
	//   - Operations with exactly 0 results: bucket{le="1"}
	//   - Operations with 1+ results: bucket{le="+Inf"} - bucket{le="1"}
	//   - Operations with exactly 1 result: bucket{le="2"} - bucket{le="1"}
	//   - Operations with exactly 2-4 results: bucket{le="5"} - bucket{le="2"}
	distLength = view.Distribution(0, 1, 2, 5, 10, 11, 12, 15, 20, 50, 100, 200, 500)

	measureLatency = stats.Int64("routing_http_client_latency", "the latency of operations by the routing HTTP client", stats.UnitMilliseconds)
	measureLength  = stats.Int64("routing_http_client_length", "the number of elements in a response collection", stats.UnitDimensionless)

	keyOperation  = tag.MustNewKey("operation")
	keyHost       = tag.MustNewKey("host")
	keyStatusCode = tag.MustNewKey("code")
	keyError      = tag.MustNewKey("error")
	keyMediaType  = tag.MustNewKey("mediatype")

	ViewLatency = &view.View{
		Measure:     measureLatency,
		Aggregation: distMS,
		TagKeys:     []tag.Key{keyOperation, keyHost, keyStatusCode, keyError},
	}
	ViewLength = &view.View{
		Measure:     measureLength,
		Aggregation: distLength,
		TagKeys:     []tag.Key{keyOperation, keyHost},
	}

	OpenCensusViews = []*view.View{
		ViewLatency,
		ViewLength,
	}
)

type measurement struct {
	mediaType  string
	operation  string
	err        error
	latency    time.Duration
	statusCode int
	host       string
	length     int
}

func (m measurement) record(ctx context.Context) {
	muts := []tag.Mutator{
		tag.Upsert(keyHost, m.host),
		tag.Upsert(keyOperation, m.operation),
		tag.Upsert(keyStatusCode, strconv.Itoa(m.statusCode)),
		tag.Upsert(keyError, metricsErrStr(m.err)),
		tag.Upsert(keyMediaType, m.mediaType),
	}
	stats.RecordWithTags(ctx, muts, measureLatency.M(m.latency.Milliseconds()))
	stats.RecordWithTags(ctx, muts, measureLength.M(int64(m.length)))
}

func newMeasurement(operation string) *measurement {
	return &measurement{
		operation: operation,
		host:      "None",
		mediaType: "None",
	}
}

// metricsErrStr converts an error into a string that can be used as a metric label.
// Errs are mapped to strings explicitly to avoid accidental high dimensionality.
func metricsErrStr(err error) string {
	if err == nil {
		return "None"
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return "HTTP"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "DeadlineExceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "Canceled"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsNotFound {
			return "DNSNotFound"
		}
		if dnsErr.IsTimeout {
			return "DNSTimeout"
		}
		return "DNS"
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "NetTimeout"
		}
		return "Net"
	}

	return "Other"
}
