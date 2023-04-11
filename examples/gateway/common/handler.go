package common

import (
	"net/http"

	"github.com/ipfs/boxo/gateway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func NewHandler(gwAPI gateway.IPFSBackend) http.Handler {
	// Initialize the headers and gateway configuration. For this example, we do
	// not add any special headers, but the required ones.
	headers := map[string][]string{}
	gateway.AddAccessControlHeaders(headers)
	conf := gateway.Config{
		Headers: headers,
	}

	// Initialize the public gateways that we will want to have available through
	// Host header rewriting. This step is optional and only required if you're
	// running multiple public gateways and want different settings and support
	// for DNSLink and Subdomain Gateways.
	noDNSLink := false // If you set DNSLink to point at the CID from CAR, you can load it!
	publicGateways := map[string]*gateway.Specification{
		// Support public requests with Host: CID.ipfs.example.net and ID.ipns.example.net
		"example.net": {
			Paths:         []string{"/ipfs", "/ipns"},
			NoDNSLink:     noDNSLink,
			UseSubdomains: true,
		},
		// Support local requests
		"localhost": {
			Paths:         []string{"/ipfs", "/ipns"},
			NoDNSLink:     noDNSLink,
			UseSubdomains: true,
		},
	}

	// Creates a mux to serve the gateway paths. This is not strictly necessary
	// and gwHandler could be used directly. However, on the next step we also want
	// to add prometheus metrics, hence needing the mux.
	gwHandler := gateway.NewHandler(conf, gwAPI)
	mux := http.NewServeMux()
	mux.Handle("/ipfs/", gwHandler)
	mux.Handle("/ipns/", gwHandler)

	// Serves prometheus metrics alongside the gateway. This step is optional and
	// only required if you need or want to access the metrics. You may also decide
	// to expose the metrics on a different path, or port.
	mux.Handle("/debug/metrics/prometheus", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))

	// Then wrap the mux with the hostname handler. Please note that the metrics
	// will not be available under the previously defined publicGateways.
	// You will be able to access the metrics via 127.0.0.1 but not localhost
	// or example.net. If you want to expose the metrics on such gateways,
	// you will have to add the path "/debug" to the variable Paths.
	var handler http.Handler
	handler = gateway.WithHostname(mux, gwAPI, publicGateways, noDNSLink)

	// Then, wrap with the withConnect middleware. This is required since we use
	// http.ServeMux which does not support CONNECT by default.
	handler = withConnect(handler)

	// Finally, wrap with the otelhttp handler. This will allow the tracing system
	// to work and for correct propagation of tracing headers. This step is optional
	// and only required if you want to use tracing. Note that OTel must be correctly
	// setup in order for this to have an effect.
	handler = otelhttp.NewHandler(handler, "Gateway.Request")

	return handler
}

// withConnect provides a middleware that adds support to the HTTP CONNECT method.
// This is required if the implementer is using http.ServeMux, or some other structure
// that does not support the CONNECT method. It should be applied to the top-level handler.
// https://golang.org/src/net/http/request.go#L111
func withConnect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
