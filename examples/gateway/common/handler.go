package common

import (
	"net/http"

	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/gateway/assets"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func NewHandler(gwAPI gateway.IPFSBackend) http.Handler {
	conf := gateway.Config{
		// If you set DNSLink to point at the CID from CAR, you can load it!
		NoDNSLink: false,

		// For these examples we have the trusted mode enabled by default. That is,
		// all types of requests will be accepted. By default, only Trustless Gateway
		// requests work: https://specs.ipfs.tech/http-gateways/trustless-gateway/
		DeserializedResponses: true,

		// Initialize the public gateways that we will want to have available
		// through Host header rewriting. This step is optional, but required
		// if you're running multiple public gateways on different hostnames
		// and want different settings such as support for Deserialized
		// Responses on localhost, or DNSLink and Subdomain Gateways.
		PublicGateways: map[string]*gateway.PublicGateway{
			// Support public requests with Host: CID.ipfs.example.net and ID.ipns.example.net
			"example.net": {
				Paths:         []string{"/ipfs", "/ipns"},
				NoDNSLink:     false,
				UseSubdomains: true,
				// This subdomain gateway is used for testing and therefore we make non-trustless requests.
				DeserializedResponses: true,
			},
			// Support local requests
			"localhost": {
				Paths:         []string{"/ipfs", "/ipns"},
				NoDNSLink:     false,
				UseSubdomains: true,
				// Localhost is considered trusted, ok to allow deserialized responses
				// as long it is not exposed to the internet.
				DeserializedResponses: true,
			},
		},

		// Add an example menu item called 'Boxo', linking to our library.
		Menu: []assets.MenuItem{
			{
				URL:   "https://github.com/ipfs/boxo",
				Title: "Boxo",
			},
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
	handler = gateway.NewHostnameHandler(conf, gwAPI, mux)

	// Then, wrap with the withConnect middleware. This is required since we use
	// http.ServeMux which does not support CONNECT by default.
	handler = withConnect(handler)

	// Add headers middleware that applies any headers we define to all requests
	// as well as a default CORS configuration.
	handler = gateway.NewHeaders(nil).ApplyCors().Wrap(handler)

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
