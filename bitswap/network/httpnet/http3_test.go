package httpnet

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/stretchr/testify/require"
)

func TestAltSvcAdvertisesH3(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{`h3=":443"; ma=86400`, true},
		{`h3=":443"; ma=86400, h2=":443"`, true},
		{`h2=":443", h3=":443"`, true},
		{`  h3=":443"`, true},
		{`h3`, true}, // bare protocol id
		{`h2=":443"`, false},
		{`h3-29=":443"`, false}, // obsolete draft, not HTTP/3
		{``, false},
		{`clear`, false},
		{`clear, h3=":443"`, false}, // clear withdraws all alternatives
		{`quic=":443"`, false},
	}
	for _, tc := range cases {
		require.Equalf(t, tc.want, altSvcAdvertisesH3(tc.in), "input %q", tc.in)
	}
}

func TestH3Capabilities(t *testing.T) {
	now := time.Unix(1000, 0)
	c := newH3Capabilities(10 * time.Minute)
	c.now = func() time.Time { return now }

	require.False(t, c.preferH3("host"), "unknown host should not prefer h3")

	c.learnFromAltSvc("host", `h3=":443"; ma=86400`)
	require.True(t, c.preferH3("host"), "should prefer h3 after learning support")

	c.markFailed("host")
	require.False(t, c.preferH3("host"), "should not prefer h3 during back-off")

	now = now.Add(11 * time.Minute)
	require.True(t, c.preferH3("host"), "should prefer h3 again after back-off expires")

	c.learnFromAltSvc("host", "")
	require.True(t, c.preferH3("host"), "empty Alt-Svc must not change learned state")

	c.learnFromAltSvc("host", `h2=":443"`)
	require.False(t, c.preferH3("host"), "Alt-Svc without h3 should drop support")
}

// fakeRoundTripper is a stand-in for the TCP transport.
type fakeRoundTripper struct {
	header http.Header
	calls  int
}

func (f *fakeRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	f.calls++
	h := f.header
	if h == nil {
		h = http.Header{}
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func TestH3FallbackLearnsFromAltSvc(t *testing.T) {
	h := newH3Fallback(nil, &tls.Config{}, time.Minute)
	defer h.Close()
	fake := &fakeRoundTripper{header: http.Header{"Alt-Svc": {`h3=":443"; ma=86400`}}}
	h.tcp = fake

	require.False(t, h.caps.preferH3("example.com"))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/ipfs/bafkqaaa", nil)
	require.NoError(t, err)
	resp, err := h.RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 1, fake.calls, "first request goes over TCP")
	require.True(t, h.caps.preferH3("example.com"), "Alt-Svc h3 should be learned")
}

func TestH3FallbackFallsBackWhenQUICFails(t *testing.T) {
	fake := &fakeRoundTripper{}
	h := &h3Fallback{
		tcp: fake,
		quic: &http3.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			// Fail fast: nothing listens on UDP below, so the handshake times out.
			QUICConfig: &quic.Config{HandshakeIdleTimeout: 200 * time.Millisecond},
		},
		caps: newH3Capabilities(time.Hour),
	}
	defer h.Close()

	// Pretend a previous response taught us this host serves HTTP/3.
	const authority = "127.0.0.1:1" // port 1: no QUIC listener
	h.caps.learnFromAltSvc(authority, `h3=":1"`)
	require.True(t, h.caps.preferH3(authority))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+authority+"/ipfs/bafkqaaa", nil)
	require.NoError(t, err)
	resp, err := h.RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 1, fake.calls, "request should fall back to TCP after QUIC fails")
	require.False(t, h.caps.preferH3(authority), "failed host should enter back-off")
}

func TestH3FallbackSkipsH3WhenNotReplayable(t *testing.T) {
	fake := &fakeRoundTripper{}
	h := &h3Fallback{
		tcp: fake,
		quic: &http3.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			QUICConfig:      &quic.Config{HandshakeIdleTimeout: 200 * time.Millisecond},
		},
		caps: newH3Capabilities(time.Hour),
	}
	defer h.Close()

	h.caps.learnFromAltSvc("example.com", `h3=":443"`)
	require.True(t, h.caps.preferH3("example.com"))

	// A request with a body cannot be replayed over HTTP/2, so HTTP/3 must be
	// skipped entirely (not attempted and failed, which would set a back-off).
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://example.com/x", strings.NewReader("x"))
	require.NoError(t, err)
	_, err = h.RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, 1, fake.calls)
	require.True(t, h.caps.preferH3("example.com"), "h3 must be skipped, not attempted, for body requests")

	// Plaintext http cannot use QUIC (always TLS), so HTTP/3 must be skipped.
	req2, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.com/x", nil)
	require.NoError(t, err)
	_, err = h.RoundTrip(req2)
	require.NoError(t, err)
	require.Equal(t, 2, fake.calls)
	require.True(t, h.caps.preferH3("example.com"), "h3 must be skipped for plaintext http")
}

func TestH3PeerstorePersistence(t *testing.T) {
	ps, err := pstoremem.NewPeerstore()
	require.NoError(t, err)
	defer ps.Close()

	p := test.RandPeerIDFatal(t)
	u, err := url.Parse("https://example.com")
	require.NoError(t, err)
	urls := []network.ParsedURL{{URL: u}}

	src := newH3Fallback(nil, &tls.Config{}, time.Minute)
	defer src.Close()

	// Nothing learned yet: persisted verdict is false, a fresh cache stays h2.
	src.persistToPeerstore(ps, p, urls)
	cold := newH3Fallback(nil, &tls.Config{}, time.Minute)
	defer cold.Close()
	cold.seedFromPeerstore(ps, p, urls)
	require.False(t, cold.caps.preferH3("example.com"))

	// Learn HTTP/3, persist it, and a brand-new cache should pick it up.
	src.caps.learnFromAltSvc("example.com", `h3=":443"`)
	src.persistToPeerstore(ps, p, urls)

	warm := newH3Fallback(nil, &tls.Config{}, time.Minute)
	defer warm.Close()
	require.False(t, warm.caps.preferH3("example.com"))
	warm.seedFromPeerstore(ps, p, urls)
	require.True(t, warm.caps.preferH3("example.com"), "should restore h3 support from peerstore")
}
