// Package httpnet implements an Exchange network that sends and receives
// Exchange messages from peers' HTTP endpoints.
package httpnet

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/ipfs/boxo/bitswap/network"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("httpnet")

var ErrNoHTTPAddresses = errors.New("AddrInfo does not contain any valid HTTP addresses")
var ErrNoSuccess = errors.New("none of the peer HTTP endpoints responded successfully to request")

var _ network.BitSwapNetwork = (*Network)(nil)

// Defaults for the different options
var (
	DefaultMaxBlockSize            int64 = 2 << 20            // 2MiB.
	DefaultUserAgent                     = defaultUserAgent() // Usually will result in a "boxo@commitID"
	DefaultDialTimeout                   = 3 * time.Second
	DefaultIdleConnTimeout               = 30 * time.Second
	DefaultResponseHeaderTimeout         = 10 * time.Second
	DefaultMaxIdleConns                  = 50
	DefaultSupportsHave                  = true
	DefaultInsecureSkipVerify            = false
	DefaultMaxBackoff                    = time.Minute
	DefaultMaxHTTPAddressesPerPeer       = 10
)

var pingCid = "bafkqaaa" // identity CID

const http2proto = "HTTP/2.0"

// Option allows to configure the Network.
type Option func(net *Network)

// WithUserAgent sets the user agent when making requests.
func WithUserAgent(agent string) Option {
	return func(net *Network) {
		net.userAgent = agent
	}
}

// WithMaxBlockSize sets the maximum size of an HTTP response (block).
func WithMaxBlockSize(size int64) Option {
	return func(net *Network) {
		net.maxBlockSize = size
	}
}

// WithDialTimeout sets the maximum time to wait for a connection to be set up.
func WithDialTimeout(t time.Duration) Option {
	return func(net *Network) {
		net.dialTimeout = t
	}
}

// WithIdleConnTimeout sets how long to keep connections alive before closing
// them when no requests happen.
func WithIdleConnTimeout(t time.Duration) Option {
	return func(net *Network) {
		net.idleConnTimeout = t
	}
}

// WithResponseHeaderTimeout sets how long to wait for a response to start
// arriving. It is the time given to the provider to find and start sending
// the block. It does not affect the time it takes to download the request body.
func WithResponseHeaderTimeout(t time.Duration) Option {
	return func(net *Network) {
		net.responseHeaderTimeout = t
	}
}

// WithMaxIdleConns sets how many keep-alive connections we can have where no
// requests are happening.
func WithMaxIdleConns(n int) Option {
	return func(net *Network) {
		net.maxIdleConns = n
	}
}

// WithSupportsHave specifies whether want to expose that we can handle Have
// messages (i.e. to the MessageQueue). Have messages trigger HEAD HTTP
// requests. Not all HTTP-endpoints may know how to handle a HEAD request.
func WithSupportsHave(b bool) Option {
	return func(net *Network) {
		net.supportsHave = b
	}
}

// WithInsecureSkipVerify allows making HTTPS connections to test servers.
// Use for testing.
func WithInsecureSkipVerify(b bool) Option {
	return func(net *Network) {
		net.insecureSkipVerify = b
	}
}

// WithAllowlist sets the hostnames that we are allowed to connect to via
// HTTP.
func WithAllowlist(hosts []string) Option {
	return func(net *Network) {
		net.allowlist = make(map[string]struct{})
		for _, h := range hosts {
			net.allowlist[h] = struct{}{}
		}
	}
}

// WithMaxHTTPAddressesPerPeer limits how many http addresses we attempt to
// connect to per peer.
func WithMaxHTTPAddressesPerPeer(max int) Option {
	return func(net *Network) {
		net.maxHTTPAddressesPerPeer = max
	}
}

type Network struct {
	// NOTE: Stats must be at the top of the heap allocation to ensure 64bit
	// alignment.
	stats network.Stats

	host   host.Host
	client *http.Client

	receivers       []network.Receiver
	connEvtMgr      *network.ConnectEventManager
	pinger          *pinger
	requestTracker  *requestTracker
	cooldownTracker *cooldownTracker

	// options
	userAgent               string
	maxBlockSize            int64
	dialTimeout             time.Duration
	idleConnTimeout         time.Duration
	responseHeaderTimeout   time.Duration
	maxIdleConns            int
	supportsHave            bool
	insecureSkipVerify      bool
	maxHTTPAddressesPerPeer int
	allowlist               map[string]struct{}

	metrics *metrics
}

// New returns a BitSwapNetwork supported by underlying IPFS host.
func New(host host.Host, opts ...Option) network.BitSwapNetwork {
	htnet := &Network{
		host:                    host,
		userAgent:               defaultUserAgent(),
		maxBlockSize:            DefaultMaxBlockSize,
		dialTimeout:             DefaultDialTimeout,
		idleConnTimeout:         DefaultIdleConnTimeout,
		responseHeaderTimeout:   DefaultResponseHeaderTimeout,
		maxIdleConns:            DefaultMaxIdleConns,
		supportsHave:            DefaultSupportsHave,
		insecureSkipVerify:      DefaultInsecureSkipVerify,
		maxHTTPAddressesPerPeer: DefaultMaxHTTPAddressesPerPeer,
		metrics:                 newMetrics(),
	}

	for _, opt := range opts {
		opt(htnet)
	}

	reqTracker := newRequestTracker()
	htnet.requestTracker = reqTracker

	cooldownTracker := newCooldownTracker(DefaultMaxBackoff)
	htnet.cooldownTracker = cooldownTracker

	netdialer := &net.Dialer{
		// Timeout for connects to complete.
		Timeout:   htnet.dialTimeout,
		KeepAlive: 15 * time.Second,
		// TODO for go1.23
		// // KeepAlive config for sending probes for an active
		// // connection.
		// KeepAliveConfig: net.KeepAliveConfig{
		// 	Enable:   true,
		// 	Idle:     15 * time.Second, // default
		// 	Interval: 15 * time.Second, // default
		// 	Count:    2,                // default would be 9
		// },
	}

	// Re: wasm: see
	// https://cs.opensource.google/go/go/+/266626211e40d1f2c3a34fa4cd2023f5310cbd7d
	// In wasm builds custom Dialer gets ignored. DefaultTransport makes
	// sure it sets DialContext to nil for wasm builds as to not break the
	// "contract". Probably makes no difference in the end, but we do the
	// same, just in case.
	dialCtx := netdialer.DialContext
	if http.DefaultTransport.(*http.Transport).DialContext == nil {
		dialCtx = nil
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: htnet.insecureSkipVerify,
	}

	t := &http.Transport{
		TLSClientConfig:   tlsCfg,
		Proxy:             http.ProxyFromEnvironment,
		DialContext:       dialCtx,
		ForceAttemptHTTP2: true,
		// MaxIdleConns: how many keep-alive conns can we have without
		// requests.
		MaxIdleConns: htnet.maxIdleConns,
		// IdleConnTimeout: how long can a keep-alive connection stay
		// around without requests.
		IdleConnTimeout:        htnet.idleConnTimeout,
		ResponseHeaderTimeout:  htnet.responseHeaderTimeout,
		ExpectContinueTimeout:  1 * time.Second,
		MaxResponseHeaderBytes: 2 << 10,  // 2KiB
		ReadBufferSize:         64 << 10, // 64KiB. Default is 4KiB and most blocks will be larger.
	}

	c := &http.Client{
		Transport: t,
	}
	htnet.client = c

	pinger := newPinger(host, htnet.client, pingCid, htnet.userAgent)
	htnet.pinger = pinger

	return htnet
}

// Start sets up the given receivers to be notified when message responses are
// received. It also starts the connection event manager. Start must be called
// before using the Network.
func (ht *Network) Start(receivers ...network.Receiver) {
	log.Infof("httpnet: HTTP retrieval system started with allowlist: %s", ht.allowlist)
	ht.receivers = receivers
	connectionListeners := make([]network.ConnectionListener, len(receivers))
	for i, v := range receivers {
		connectionListeners[i] = v
	}
	ht.connEvtMgr = network.NewConnectEventManager(connectionListeners...)

	ht.connEvtMgr.Start()
}

// Stop stops the connect event manager associated with this network.
// Other methods should no longer be used after calling Stop().
func (ht *Network) Stop() {
	ht.connEvtMgr.Stop()
	ht.cooldownTracker.stopCleaner()
}

// Ping triggers a ping to the given peer and returns the latency.
func (ht *Network) Ping(ctx context.Context, p peer.ID) ping.Result {
	return ht.pinger.ping(ctx, p)

}

// Latency returns the EWMA latency for the given peer.
func (ht *Network) Latency(p peer.ID) time.Duration {
	return ht.pinger.latency(p)
}

func (ht *Network) senderURLs(p peer.ID) []*senderURL {
	pi := ht.host.Peerstore().PeerInfo(p)
	urls := network.ExtractURLsFromPeer(pi)
	if len(urls) == 0 {
		return nil
	}
	return ht.cooldownTracker.fillSenderURLs(urls)
}

// SendMessage sends the given message to the given peer. It uses
// NewMessageSender under the hood, with default options.
func (ht *Network) SendMessage(ctx context.Context, p peer.ID, msg bsmsg.BitSwapMessage) error {

	if len(msg.Wantlist()) == 0 {
		return nil
	}

	log.Debugf("SendMessage: %s", p)

	// Note: SendMessage seems to only be used to send cancellations.
	// So default options are fine.
	sender, err := ht.NewMessageSender(ctx, p, nil)
	if err != nil {
		return err
	}
	return sender.SendMsg(ctx, msg)
}

// Self returns the local peer ID.
func (ht *Network) Self() peer.ID {
	return ht.host.ID()
}

// Connect attempts setting up an HTTP connection to the given peer. The given
// AddrInfo must include at least one HTTP endpoint for the peer. HTTP URLs in
// AddrInfo will be tried by making an HTTP GET request to
// "ipfs/bafyaabakaieac", which is the CID for an empty directory (inlined).
// Any completed request, regardless of the HTTP response, is considered a
// connection success and marks this peer as "connected", setting it up to
// handle messages and make requests. The peer will be pinged regularly to
// collect latency measurements until DisconnectFrom() is called.
func (ht *Network) Connect(ctx context.Context, p peer.AddrInfo) error {
	htaddrs, _ := network.SplitHTTPAddrs(p)
	if len(htaddrs.Addrs) == 0 {
		return ErrNoHTTPAddresses
	}

	// avoid funny things like someone adding 100 broken urls to a peer.
	if len(htaddrs.Addrs) > ht.maxHTTPAddressesPerPeer {
		htaddrs.Addrs = htaddrs.Addrs[0:ht.maxHTTPAddressesPerPeer]
	}

	urls := network.ExtractURLsFromPeer(htaddrs)
	if len(ht.allowlist) > 0 {
		var filteredURLs []network.ParsedURL
		var filteredAddrs []multiaddr.Multiaddr
		for i, u := range urls {
			host, _, err := net.SplitHostPort(u.URL.Host)
			if err != nil {
				return err
			}
			if _, ok := ht.allowlist[host]; ok {
				filteredURLs = append(filteredURLs, u)
				filteredAddrs = append(filteredAddrs, htaddrs.Addrs[i])
			}
		}
		urls = filteredURLs
		htaddrs.Addrs = filteredAddrs
	}
	// if len(filteredURLs == 0) nothing will happen below and we will return
	// an error below.

	// We will know try to talk to this peer by making HTTP requests to its urls
	// and recording which ones work.
	// This allows re-using the connections that we are about to open next
	// time with the client. We call peer.Connected()
	// on success.
	var workingAddrs []multiaddr.Multiaddr
	for i, u := range urls {
		req, err := buildRequest(ctx, u, "GET", pingCid, ht.userAgent)
		if err != nil {
			log.Debug(err)
			return err
		}

		log.Debugf("connect request to %s", req.URL)
		resp, err := ht.client.Do(req)
		if err != nil {
			log.Debugf("connect error %s", err)
			if ctxErr := ctx.Err(); ctxErr != nil {
				// abort when context cancelled
				return ctxErr
			}
			continue
		}

		if resp.Proto != http2proto {
			log.Warnf("%s://%q is not using HTTP/2 (%s)", req.URL.Scheme, req.URL.Host, resp.Proto)
		}

		if resp.StatusCode >= 500 { // 5xx
			// We made a proper request and got a 5xx back.
			// We cannot consider this a working connection.
			continue
		}
		workingAddrs = append(workingAddrs, htaddrs.Addrs[i])
	}

	if len(workingAddrs) > 0 {
		ht.host.Peerstore().AddAddrs(p.ID, workingAddrs, peerstore.PermanentAddrTTL)
		ht.connEvtMgr.Connected(p.ID)
		ht.pinger.startPinging(p.ID)

		// We "connected"
		return nil
	}

	err := fmt.Errorf("%w: %s", ErrNoSuccess, p.ID)
	log.Debug(err)
	return err
}

// DisconnectFrom marks this peer as Disconnected in the connection event
// manager, stops pinging for latency measurements and removes it from the
// peerstore.
func (ht *Network) DisconnectFrom(ctx context.Context, p peer.ID) error {
	pi := ht.host.Peerstore().PeerInfo(p)
	_, bsaddrs := network.SplitHTTPAddrs(pi)
	ht.host.Peerstore().ClearAddrs(p)
	if len(bsaddrs.Addrs) == 0 {
		// this should always be the case unless we have been
		// contacted via bitswap...
		ht.connEvtMgr.Disconnected(p)
	} else { // re-add bitswap addresses
		// unfortunately we cannot maintain ttl info
		ht.host.Peerstore().SetAddrs(p, bsaddrs.Addrs, peerstore.TempAddrTTL)
	}
	ht.pinger.stopPinging(p)

	// coolDownTracker: we leave untouched. We want to keep
	// ongoing cooldowns there in case we reconnect to this peer.

	return nil
}

// TagPeer uses the host's ConnManager to tag a peer.
func (ht *Network) TagPeer(p peer.ID, tag string, w int) {
	ht.host.ConnManager().TagPeer(p, tag, w)
}

// UntagPeer uses the host's ConnManager to untag a peer.
func (ht *Network) UntagPeer(p peer.ID, tag string) {
	ht.host.ConnManager().UntagPeer(p, tag)
}

// Protect does nothing. The purpose of Protect is to mantain connections as
// long as they are used. But our connections are already maintained as long
// as they are, and closed when not.
func (ht *Network) Protect(p peer.ID, tag string) {
}

// Unprotect does nothing. The purpose of Unprotect is to be able to close
// connections when they are no longer relevant. Our connections are already
// closed when they are not used. It returns always true as technically our
// connections are potentially still protected as long as they are used.
func (ht *Network) Unprotect(p peer.ID, tag string) bool {
	return true
}

// Stats returns message counts for this peer. Each message sent is an HTTP
// requests. Each message received is an HTTP response.
func (ht *Network) Stats() network.Stats {
	return network.Stats{
		MessagesRecvd: atomic.LoadUint64(&ht.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&ht.stats.MessagesSent),
	}
}

// buildRequests sets up common settings for making a requests.
func buildRequest(ctx context.Context, u network.ParsedURL, method string, cid string, userAgent string) (*http.Request, error) {
	// copy url
	sendURL, _ := url.Parse(u.URL.String())
	sendURL.RawQuery = "format=raw"
	sendURL.Path += "/ipfs/" + cid

	req, err := http.NewRequestWithContext(ctx,
		method,
		sendURL.String(),
		nil,
	)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	headers := make(http.Header)
	headers.Add("Accept", "application/vnd.ipld.raw")
	headers.Add("User-Agent", userAgent)
	if u.SNI != "" {
		headers.Add("Host", u.SNI)
	}
	req.Header = headers
	return req, nil
}

// NewMessageSender returns a MessageSender implementation which sends the
// given message to the given peer over HTTP.
// An error is returned of the peer has no known HTTP endpoints.
func (ht *Network) NewMessageSender(ctx context.Context, p peer.ID, opts *network.MessageSenderOpts) (network.MessageSender, error) {
	// cooldowns made by other senders between now and SendMsg will not be
	// taken into account since we access that info here only. From that
	// point, we only react to cooldowns/errors received by this message
	// sender and not others. This is mostly fine given how MessageSender
	// is used as part of MessageQueue:
	//
	// * We expect peers to be associated with single urls so there will
	// not be multiple message sender for the same url normally.
	// * We remember cooldowns between message senders (i.e. when a queue
	// dies and a new one is created).
	// * We track cooldowns in the urls for the lifetime of this sender.
	//
	// This way we minimize lock contention around the cooldown map, with
	// one read access per message sender only.
	urls := ht.senderURLs(p)
	if len(urls) == 0 {
		return nil, ErrNoHTTPAddresses
	}

	log.Debugf("NewMessageSender: %s", p)
	senderOpts := setSenderOpts(opts)

	return &httpMsgSender{
		// ctx ??
		ht:      ht,
		peer:    p,
		urls:    urls,
		closing: make(chan struct{}, 1),
		opts:    senderOpts,
	}, nil
}

// defaultUserAgent returns a useful user agent version string allowing us to
// identify requests coming from official releases of this module vs forks.
func defaultUserAgent() (ua string) {
	p := reflect.ValueOf(Network{}).Type().PkgPath()
	// we have monorepo, so stripping the remainder
	importPath := strings.TrimSuffix(p, "/bitswap/network/httpnet")

	ua = importPath
	var module *debug.Module
	if bi, ok := debug.ReadBuildInfo(); ok {
		// If debug.ReadBuildInfo was successful, we can read Version by finding
		// this client in the dependency list of the app that has it in go.mod
		for _, dep := range bi.Deps {
			if dep.Path == importPath {
				module = dep
				break
			}
		}
		if module != nil {
			ua += "@" + module.Version
			return
		}
		ua += "@unknown"
	}
	return
}
