package httpnet

import (
	"context"

	imetrics "github.com/ipfs/go-metrics-interface"
)

var durationHistogramBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 240, 480, 960, 1920}

var blockSizesHistogramBuckets = []float64{1, 128 << 10, 256 << 10, 512 << 10, 1024 << 10, 2048 << 10, 4092 << 10}

type ctxKeyT string

var ctxKey ctxKeyT = ctxKeyT(imetrics.CtxScopeKey)

func requestsInFlight(ctx context.Context) imetrics.Gauge {
	return imetrics.NewCtx(ctx, "requests_in_flight", "Current number of in-flight requests").Gauge()
}

func requestsTotal(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "requests_total", "Total request count").Counter()
}

func requestsFailure(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "requests_failure", "Failed (no response, dial error etc) requests count").Counter()
}

func requestsBodyFailure(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "requests_body_failure", "Failure count when reading response body").Counter()
}

func statusNotFound(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "status_404", "Request count with NotFound status").Counter()
}

func statusGone(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "status_410", "Request count with Gone status").Counter()
}

func statusForbidden(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "status_403", "Request count with Forbidden status").Counter()
}

func statusUnavailableForLegalReasons(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "status_451", "Request count with Unavailable For Legal Reasons status").Counter()
}

func statusOK(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "status_200", "Request count with OK status").Counter()
}

func statusTooManyRequests(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "status_429", "Request count with Too Many Requests status").Counter()
}

func statusServiceUnavailable(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "status_503", "Request count with Service Unavailable status").Counter()
}

func statusInternalServerError(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "status_500", "Request count with Internal Server Error status").Counter()
}

func statusOthers(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "status_others", "Request count with other status codes").Counter()
}

func requestTime(ctx context.Context) imetrics.Histogram {
	return imetrics.NewCtx(ctx, "request_duration_seconds", "Histogram of request durations").Histogram(durationHistogramBuckets)
}

func responseSize(ctx context.Context) imetrics.Histogram {
	return imetrics.NewCtx(ctx, "response_bytes", "Histogram of http response sizes").Histogram(blockSizesHistogramBuckets)
}

type metrics struct {
	RequestsInFlight                 imetrics.Gauge
	RequestsTotal                    imetrics.Counter
	RequestsFailure                  imetrics.Counter
	RequestsBodyFailure              imetrics.Counter
	StatusNotFound                   imetrics.Counter
	StatusGone                       imetrics.Counter
	StatusForbidden                  imetrics.Counter
	StatusUnavailableForLegalReasons imetrics.Counter
	StatusOK                         imetrics.Counter
	StatusTooManyRequests            imetrics.Counter
	StatusServiceUnavailable         imetrics.Counter
	StatusInternalServerError        imetrics.Counter
	StatusOthers                     imetrics.Counter
	RequestTime                      imetrics.Histogram
	ResponseSize                     imetrics.Histogram
}

func newMetrics() *metrics {
	ctx := imetrics.CtxScope(context.Background(), "exchange_httpnet")

	return &metrics{
		RequestsInFlight:                 requestsInFlight(ctx),
		RequestsTotal:                    requestsTotal(ctx),
		RequestsFailure:                  requestsFailure(ctx),
		RequestsBodyFailure:              requestsBodyFailure(ctx),
		StatusNotFound:                   statusNotFound(ctx),
		StatusGone:                       statusGone(ctx),
		StatusForbidden:                  statusForbidden(ctx),
		StatusUnavailableForLegalReasons: statusUnavailableForLegalReasons(ctx),
		StatusOK:                         statusOK(ctx),
		StatusTooManyRequests:            statusTooManyRequests(ctx),
		StatusServiceUnavailable:         statusServiceUnavailable(ctx),
		StatusInternalServerError:        statusInternalServerError(ctx),
		StatusOthers:                     statusOthers(ctx),
		RequestTime:                      requestTime(ctx),
		ResponseSize:                     responseSize(ctx),
	}
}

func (m *metrics) updateStatusCounter(statusCode int) {
	m.RequestsTotal.Inc()
	switch statusCode {
	case 404:
		m.StatusNotFound.Inc()
	case 410:
		m.StatusGone.Inc()
	case 403:
		m.StatusForbidden.Inc()
	case 451:
		m.StatusUnavailableForLegalReasons.Inc()
	case 200:
		m.StatusOK.Inc()
	case 429:
		m.StatusTooManyRequests.Inc()
	case 503:
		m.StatusServiceUnavailable.Inc()
	case 500:
		m.StatusInternalServerError.Inc()
	default:
		m.StatusOthers.Inc()
	}
}
