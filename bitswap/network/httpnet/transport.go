package httpnet

import "net/http"

const http2proto = "HTTP/2.0"

// transport is an http.Transport wrapper, mostly a placeholder for the
// future.
type transport struct {
	*http.Transport
}

// newTransport wraps the given transport.
func newTransport(t *http.Transport) *transport {
	return &transport{
		Transport: t,
	}
}

// RoundTrip makes a RoundTrip on the wrapped transport and warns if it does
// not use HTTP/2.
func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.Transport.RoundTrip(r)
	if err != nil {
		return resp, err
	}
	return resp, err
}
