package httpnet

import (
	"crypto/tls"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// http3proto is the value of http.Response.Proto for responses received over
// HTTP/3, as set by the quic-go http3 transport. It mirrors http2proto.
const http3proto = "HTTP/3.0"

// peerstoreHTTP3Key is the peerstore key under which we remember whether a
// peer's HTTP endpoint supports HTTP/3. Stored per peer like
// peerstoreSupportsHeadKey, it lets HTTP/3 support survive reconnects, and
// process restarts when the peerstore is backed by a persistent datastore.
const peerstoreHTTP3Key = "http-retrieval-http3-support"

// Defaults for the HTTP/3 transport.
const (
	// DefaultH3HandshakeTimeout bounds how long we wait for a QUIC handshake
	// before giving up on HTTP/3 for a request and falling back to HTTP/2. Two
	// reasons it is short, and shorter than the TCP dial timeout
	// (DefaultDialTimeout):
	//
	//   - It is a fallback trigger, not a terminal give-up. A failed TCP dial
	//     means the peer is unreachable; a failed QUIC handshake only means
	//     UDP/QUIC is not working here, and we immediately retry over HTTP/2,
	//     which we already know the host speaks.
	//   - The HTTP/3 attempt and the HTTP/2 fallback share one request
	//     deadline (the MessageSender SendTimeout). Time spent waiting for QUIC
	//     is time the fallback no longer has, so we keep the HTTP/3 budget
	//     small to leave the fallback room to succeed.
	DefaultH3HandshakeTimeout = 2 * time.Second

	// DefaultH3FailureBackoff is how long we avoid retrying HTTP/3 against a
	// host after a failed QUIC attempt, so we do not re-pay the handshake
	// timeout on every request to a host we cannot reach over UDP.
	//
	// Back-off is per host on purpose. HTTP/3 can fail for unrelated reasons
	// (a host's HTTP/3 endpoint is down, or the local network blocks UDP), and
	// a single global "HTTP/3 is broken" switch would let a few failing hosts
	// disable HTTP/3 for hosts that work fine. If UDP is blocked everywhere,
	// each host independently learns that once and falls back to HTTP/2; the
	// cost is just one fast handshake timeout per host per back-off window.
	DefaultH3FailureBackoff = 30 * time.Minute
)

// WithHTTP3 enables opportunistic HTTP/3 (QUIC) retrieval.
//
// HTTP/3 cannot be negotiated by upgrading a live TCP connection: a client
// must learn out of band that a host serves HTTP/3, then open a separate QUIC
// connection. With this option enabled, the network discovers HTTP/3 support
// from the "Alt-Svc" header that servers send on their HTTP/2 responses (RFC
// 7838). No change to provider records or multiaddrs is required: endpoints
// keep announcing the usual /tcp/443/tls/http address, and we upgrade to
// HTTP/3 only when the server tells us it is available.
//
// The behaviour per host is:
//
//  1. The first request goes over HTTP/2 (the existing TCP transport).
//  2. If the response advertises "h3" via Alt-Svc, we remember it (and persist
//     it in the peerstore so it survives reconnects).
//  3. Later requests to that host try HTTP/3 first.
//  4. If the QUIC attempt fails (UDP blocked, no HTTP/3 listener, handshake
//     timeout), we fall back to HTTP/2 and back off from HTTP/3 for that host
//     before trying again.
//
// Caveats:
//
//   - HTTP/3 needs outbound UDP. In networks that block UDP/443 every request
//     falls back to HTTP/2 after a short handshake timeout, so correctness is
//     unaffected but no speed-up is gained.
//   - The quic-go transport does not honor HTTP proxy settings
//     (http.ProxyFromEnvironment); requests that need a proxy should rely on
//     the HTTP/2 path. QUIC cannot traverse an HTTP CONNECT proxy.
//   - Operators may need to raise the UDP receive buffer limit
//     (net.core.rmem_max / wmem_max on Linux) to avoid a quic-go warning about
//     an undersized buffer.
//
// HTTP/3 is opt-in and off by default.
func WithHTTP3() Option {
	return func(net *Network) {
		net.enableHTTP3 = true
	}
}

// h3Fallback is an http.RoundTripper that prefers HTTP/3 for hosts known to
// support it and transparently falls back to a TCP transport (HTTP/1.1 or
// HTTP/2) otherwise. It learns HTTP/3 support from Alt-Svc response headers.
//
// HTTP/3 is only attempted for https requests with no body, so a failed
// attempt can always be retried over HTTP/2 with the same request (quic-go
// closes a non-nil body while sending). Block retrieval always uses nil-body
// GET/HEAD requests.
type h3Fallback struct {
	tcp  http.RoundTripper // HTTP/1.1 + HTTP/2 over TCP (the default transport)
	quic *http3.Transport  // HTTP/3 over QUIC
	caps *h3Capabilities
}

// newH3Fallback builds an h3Fallback wrapping the given TCP transport. The TLS
// config is cloned so the HTTP/3 transport cannot mutate the one used by the
// TCP transport; only the settings we care about (such as InsecureSkipVerify)
// carry over. The http3 transport sets the "h3" ALPN itself.
func newH3Fallback(tcp http.RoundTripper, tlsClientConfig *tls.Config, idleTimeout time.Duration) *h3Fallback {
	return &h3Fallback{
		tcp: tcp,
		quic: &http3.Transport{
			TLSClientConfig: tlsClientConfig.Clone(),
			QUICConfig: &quic.Config{
				HandshakeIdleTimeout: DefaultH3HandshakeTimeout,
				// Keep QUIC connections long-lived: a generous idle timeout plus
				// keep-alive pings maintain one connection per host instead of
				// reopening it. Besides saving handshakes, this avoids a churn
				// penalty seen against some servers (e.g. Cloudflare), where
				// rapidly closing and reopening a QUIC connection from the same
				// source triggers a Retry / address-validation round trip (or a
				// probe timeout), making a reconnect much slower than a fresh TCP
				// one. Long-lived connections never hit this.
				MaxIdleTimeout:  idleTimeout,   // matches the TCP transport's IdleConnTimeout
				KeepAlivePeriod: connKeepAlive, // matches the TCP dialer's keep-alive
			},
		},
		caps: newH3Capabilities(DefaultH3FailureBackoff),
	}
}

// RoundTrip implements http.RoundTripper.
func (h *h3Fallback) RoundTrip(req *http.Request) (*http.Response, error) {
	authority := req.URL.Host

	// Only attempt HTTP/3 when it can apply and we can safely fall back:
	//   - https only: QUIC is always TLS, so plaintext http endpoints (LAN)
	//     can never speak HTTP/3.
	//   - nil body: a failed attempt is retried over HTTP/2 with the same
	//     request, and quic-go closes a non-nil body while sending.
	if req.URL.Scheme == "https" && req.Body == nil && h.caps.preferH3(authority) {
		resp, err := h.quic.RoundTrip(req)
		if err == nil {
			// Keep learning from Alt-Svc on the HTTP/3 path too, so a server
			// withdrawing HTTP/3 (Alt-Svc: clear) is noticed promptly.
			h.caps.learnFromAltSvc(authority, resp.Header.Get("Alt-Svc"))
			return resp, nil
		}

		// A non-nil error from RoundTrip means we could not obtain an HTTP
		// response over QUIC at all (UDP blocked, no HTTP/3 listener, or the
		// handshake timed out). A real HTTP error status such as 404 is
		// returned as a *response*, not an error, so reaching here always
		// signals a connectivity problem worth falling back from.
		//
		// The exception is a cancelled request: HTTP/2 would fail the same
		// way, so we return the original error rather than retrying.
		if req.Context().Err() != nil {
			return nil, err
		}
		h.caps.markFailed(authority)
		log.Debugf("HTTP/3 request to %s failed (%s); falling back to HTTP/2", authority, err)
	}

	resp, err := h.tcp.RoundTrip(req)
	if err == nil {
		h.caps.learnFromAltSvc(authority, resp.Header.Get("Alt-Svc"))
	}
	return resp, err
}

// Close releases the QUIC transport and its UDP socket.
func (h *h3Fallback) Close() error {
	return h.quic.Close()
}

// seedFromPeerstore primes the in-memory cache from a previously persisted
// HTTP/3 verdict, so the first request after a reconnect (or restart, with a
// persistent peerstore) can prefer HTTP/3 without re-learning it from an
// Alt-Svc header. A stale hint is self-correcting: RoundTrip falls back to
// HTTP/2 and backs off if HTTP/3 turns out to be unavailable.
func (h *h3Fallback) seedFromPeerstore(ps peerstore.Peerstore, p peer.ID, urls []network.ParsedURL) {
	v, err := ps.Get(p, peerstoreHTTP3Key)
	if err != nil {
		return // nothing remembered
	}
	if supported, ok := v.(bool); !ok || !supported {
		return
	}
	for _, u := range urls {
		h.caps.seedSupported(u.URL.Host)
	}
}

// persistToPeerstore records, per peer, whether any of its endpoints is
// currently preferred over HTTP/3, mirroring how HEAD support is stored.
//
// The verdict is one bool per peer, not per endpoint. On a multi-endpoint peer
// where only some speak HTTP/3, seedFromPeerstore marks all of them capable;
// the non-h3 ones self-correct on their first failed attempt (one handshake
// timeout, then per-host back-off). Peers normally have a single HTTP endpoint,
// so this is rarely observable.
func (h *h3Fallback) persistToPeerstore(ps peerstore.Peerstore, p peer.ID, urls []network.ParsedURL) {
	supported := false
	for _, u := range urls {
		if h.caps.preferH3(u.URL.Host) {
			supported = true
			break
		}
	}
	_ = ps.Put(p, peerstoreHTTP3Key, supported)
}

// h3Capabilities remembers, per authority (host:port), whether a server is
// known to support HTTP/3 and whether a recent HTTP/3 attempt failed. It is
// safe for concurrent use.
type h3Capabilities struct {
	mu      sync.Mutex
	hosts   map[string]h3Status
	backoff time.Duration
	now     func() time.Time // overridable in tests
}

type h3Status struct {
	// supported is set when the server advertised "h3" via Alt-Svc (or when
	// seeded from the peerstore).
	supported bool
	// retryAfter, when non-zero, blocks HTTP/3 attempts to this host until that
	// time. It is set after a failed QUIC attempt so we do not pay the
	// handshake timeout on every request to a host we cannot reach over UDP.
	retryAfter time.Time
}

func newH3Capabilities(backoff time.Duration) *h3Capabilities {
	return &h3Capabilities{
		hosts:   make(map[string]h3Status),
		backoff: backoff,
		now:     time.Now,
	}
}

// preferH3 reports whether we should try HTTP/3 for the given authority: the
// server is known to support it and we are not in a back-off window.
func (c *h3Capabilities) preferH3(authority string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.hosts[authority]
	if !s.supported {
		return false
	}
	return s.retryAfter.IsZero() || c.now().After(s.retryAfter)
}

// learnFromAltSvc records HTTP/3 support from an Alt-Svc response header. An
// empty header is ignored, since servers do not repeat Alt-Svc on every
// response and we must not forget what we already learned.
func (c *h3Capabilities) learnFromAltSvc(authority, altSvc string) {
	if altSvc == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.hosts[authority]
	s.supported = altSvcAdvertisesH3(altSvc)
	c.hosts[authority] = s
}

// seedSupported marks an authority as HTTP/3-capable, but only when we have no
// fresher in-process knowledge about it. Used to prime the cache from the
// peerstore on a cold start, without overriding what we learned this run.
func (c *h3Capabilities) seedSupported(authority string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.hosts[authority]; ok {
		return
	}
	c.hosts[authority] = h3Status{supported: true}
}

// markFailed records that an HTTP/3 attempt to authority failed and starts a
// per-host back-off before HTTP/3 is tried against it again.
func (c *h3Capabilities) markFailed(authority string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.hosts[authority]
	s.retryAfter = c.now().Add(c.backoff)
	c.hosts[authority] = s
}

// altSvcAdvertisesH3 reports whether an Alt-Svc header value advertises
// HTTP/3. The header is a comma-separated list of alternative services, each
// written as `protocol-id="host:port"; param=value` (RFC 7838); we only need
// to know whether the "h3" protocol id is present. The special value "clear"
// withdraws all alternatives and so reports false. Obsolete draft ids such as
// "h3-29" are intentionally not treated as HTTP/3.
func altSvcAdvertisesH3(altSvc string) bool {
	for alt := range strings.SplitSeq(altSvc, ",") {
		alt = strings.TrimSpace(alt)
		if alt == "clear" {
			return false // server withdrew all alternatives
		}
		// The protocol id is everything up to the first '='.
		if id, _, _ := strings.Cut(alt, "="); id == "h3" {
			return true
		}
	}
	return false
}
