package httpnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	"github.com/ipfs/boxo/bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	probing "github.com/prometheus-community/pro-bing"
	"go.uber.org/multierr"
)

var log = logging.Logger("httpnet")

var ( // todo
	maxSendTimeout = 2 * time.Minute
	minSendTimeout = 10 * time.Second
	sendLatency    = 2 * time.Second
	minSendRate    = (100 * 1000) / 8 // 100kbit/s
)

var ErrNoHTTPAddresses = errors.New("AddrInfo does not contain any valid HTTP addresses")
var ErrNoSuccess = errors.New("none of the peer HTTP endpoints responded successfully to request")

var _ network.BitSwapNetwork = (*httpnet)(nil)

type ctxKey string

const pidCtxKey ctxKey = "peerid"

// Defaults for the different options
var (
	DefaultMaxBlockSize   int64 = 2 << 20            // 2MiB.
	DefaultUserAgent            = defaultUserAgent() // Usually will result in a "boxo@commitID"
	DefaultCooldownPeriod       = 10 * time.Second
)

type Option func(net *httpnet)

func WithUserAgent(agent string) Option {
	return func(net *httpnet) {
		net.userAgent = agent
	}
}

func WithMaxBlockSize(size int64) Option {
	return func(net *httpnet) {
		net.maxBlockSize = size
	}
}

// WithDefaultCooldownPeriod specifies how long we will avoid making requests
// to a peer URL after it errors, unless Retry-Header specifies a different
// amount of time.
func WithDefaultCooldownPeriod(d time.Duration) Option {
	return func(net *httpnet) {
		net.defaultCooldownPeriod = d
	}
}

type httpnet struct {
	// NOTE: Stats must be at the top of the heap allocation to ensure 64bit
	// alignment.
	stats network.Stats

	host   host.Host
	client *http.Client
	dialer *dialer

	// inbound messages from the network are forwarded to the receiver
	receivers  []network.Receiver
	connEvtMgr *network.ConnectEventManager

	latMapLock sync.RWMutex
	latMap     map[peer.ID]time.Duration

	cooldownURLsLock sync.RWMutex
	cooldownURLs     map[string]time.Time

	userAgent             string
	maxBlockSize          int64
	defaultCooldownPeriod time.Duration
}

// wrap a connection to detect connect/disconnect events.
// and also to re-use existing ones.
type conn struct {
	pid peer.ID
	net.Conn
	connEvtMgr *network.ConnectEventManager
}

func (c *conn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if err != nil {
		c.connEvtMgr.Disconnected(c.pid)
	}
	return n, err
}

func (c *conn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if err != nil {
		c.connEvtMgr.Disconnected(c.pid)
	}
	return n, err
}

func (c *conn) Close() error {
	err := c.Conn.Close()
	c.connEvtMgr.Disconnected(c.pid)
	return err
}

type dialer struct {
	dialer     *net.Dialer
	connEvtMgr *network.ConnectEventManager
}

// DialContext dials using the dialer but calls back on the connection event
// manager PeerConnected() on success.
func (d *dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	pid := ctx.Value(pidCtxKey).(peer.ID)
	cn, err := d.dialer.DialContext(ctx, network, address)
	if err != nil {
		d.connEvtMgr.Disconnected(pid)
		return nil, err
	}
	d.connEvtMgr.Connected(pid)
	return &conn{
		pid:        pid,
		Conn:       cn,
		connEvtMgr: d.connEvtMgr,
	}, err
}

// New returns a BitSwapNetwork supported by underlying IPFS host.
func New(host host.Host, opts ...Option) network.BitSwapNetwork {
	netdialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	dialer := &dialer{
		dialer: netdialer,
	}
	c := &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext, // maybe breaks wasm
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	net := httpnet{
		host:         host,
		client:       c,
		dialer:       dialer,
		latMap:       make(map[peer.ID]time.Duration),
		cooldownURLs: make(map[string]time.Time),
		userAgent:    defaultUserAgent(),
		maxBlockSize: DefaultMaxBlockSize,
	}

	for _, opt := range opts {
		opt(&net)
	}

	return &net
}

func (ht *httpnet) Start(receivers ...network.Receiver) {
	fmt.Println("START HTTP NET")
	ht.receivers = receivers
	connectionListeners := make([]network.ConnectionListener, len(receivers))
	for i, v := range receivers {
		connectionListeners[i] = v
	}
	ht.connEvtMgr = network.NewConnectEventManager(connectionListeners...)
	ht.dialer.connEvtMgr = ht.connEvtMgr

	ht.connEvtMgr.Start()
}

func (ht *httpnet) Stop() {
	ht.connEvtMgr.Stop()
}

// Ping
func (ht *httpnet) Ping(ctx context.Context, p peer.ID) ping.Result {
	pi := ht.host.Peerstore().PeerInfo(p)
	urls := network.ExtractURLsFromPeer(pi)
	if len(urls) == 0 {
		return ping.Result{
			Error: ErrNoHTTPAddresses,
		}
	}

	log.Debugf("Ping: %s", p)

	// pick the first one. In general there should not be more than one
	// url per peer. FIXME: right?
	// Remove port from url.
	pingURLHost := strings.Split(urls[0].Host, ":")[0]

	pinger, err := probing.NewPinger(pingURLHost)
	if err != nil {
		log.Debug("pinger error ", err)
		return ping.Result{
			RTT:   0,
			Error: err,
		}
	}
	pinger.Count = 1

	err = pinger.RunWithContext(ctx)
	if err != nil {
		log.Debug("ping error ", err)
		return ping.Result{
			RTT:   0,
			Error: err,
		}
	}
	lat := pinger.Statistics().AvgRtt
	ht.recordLatency(p, lat)
	log.Debugf("ping latency %s %s", p, lat)
	return ping.Result{
		RTT:   lat,
		Error: nil,
	}

}

// TODO
func (ht *httpnet) Latency(p peer.ID) time.Duration {
	var lat time.Duration
	ht.latMapLock.RLock()
	{
		lat = ht.latMap[p]
	}
	ht.latMapLock.RUnlock()

	// Add one more latency measurement every time latency is requested
	// since we don't do it from anywhere else.
	// FIXME: too much too often?
	go func() {
		ht.Ping(context.Background(), p)
	}()

	return lat
}

// similar to LatencyIWMA from peerstore.
func (ht *httpnet) recordLatency(p peer.ID, next time.Duration) {
	nextf := float64(next)
	s := 0.1
	ht.latMapLock.Lock()
	{
		ewma, found := ht.latMap[p]
		ewmaf := float64(ewma)
		if !found {
			ht.latMap[p] = next // when no data, just take it as the mean.
		} else {
			nextf = ((1.0 - s) * ewmaf) + (s * nextf)
			ht.latMap[p] = time.Duration(nextf)
		}
	}
	ht.latMapLock.Unlock()
}

func (ht *httpnet) isInCooldown(u *url.URL) bool {
	ustr := u.String()
	ht.cooldownURLsLock.RLock()
	dl, ok := ht.cooldownURLs[ustr]
	ht.cooldownURLsLock.RUnlock()
	if !ok {
		return false
	}
	if time.Now().After(dl) {
		ht.cooldownURLsLock.Lock()
		delete(ht.cooldownURLs, ustr)
		ht.cooldownURLsLock.Unlock()
		return false
	}
	log.Debugf("%s is in cooldown until %s", u, dl)
	return true
}

func (ht *httpnet) setCooldownDuration(u *url.URL, d time.Duration) {
	ht.cooldownURLsLock.Lock()
	ht.cooldownURLs[u.String()] = time.Now().Add(d)
	ht.cooldownURLsLock.Unlock()
}

func (ht *httpnet) SendMessage(ctx context.Context, p peer.ID, msg bsmsg.BitSwapMessage) error {
	log.Debugf("SendMessage: %s", p)
	// todo opts
	sender, err := ht.NewMessageSender(ctx, p, nil)
	if err != nil {
		return err
	}
	defer sender.Close()
	return sender.SendMsg(ctx, msg)
}

func (ht *httpnet) Self() peer.ID {
	return ht.host.ID()
}

func (ht *httpnet) Connect(ctx context.Context, p peer.AddrInfo) error {
	htaddrs, _ := network.SplitHTTPAddrs(p)
	if len(htaddrs.Addrs) == 0 {
		return nil
	}
	log.Debugf("connect to %s", p)
	ht.host.Peerstore().AddAddrs(p.ID, htaddrs.Addrs, peerstore.PermanentAddrTTL)
	urls := network.ExtractURLsFromPeer(htaddrs)
	rand.Shuffle(len(urls), func(i, j int) {
		urls[i], urls[j] = urls[j], urls[i]
	})

	// We will know try to talk to this peer by making an HTTP request.
	// This allows re-using the connection that we are about to open next
	// time with the client. The dialer callbacks will call peer.Connected()
	// on success.
	for _, u := range urls {
		req, err := ht.buildRequest(ctx, p.ID, u, "GET", "bafyaabakaieac")
		if err != nil {
			log.Debug(err)
			return err
		}

		log.Debugf("connect request to %s", req.URL)
		_, err = ht.client.Do(req)
		if err != nil {
			log.Debugf("connect error %s", err)
			if ctxErr := ctx.Err(); ctxErr != nil {
				// abort when context cancelled
				return ctxErr
			}
			continue
		}
		return nil
		// otherwise keep trying other urls. We don't care about the
		// http status code as long as the request succeeded.
	}
	err := fmt.Errorf("%w: %s", ErrNoSuccess, p.ID)
	log.Debug(err)
	return err
}

func (ht *httpnet) DisconnectFrom(ctx context.Context, p peer.ID) error {
	// we noop. Idle connections will die by themselves.
	return nil
}

// ** We have no way of protecting a connection from our side other than using
// it so that it does not idle and gets closed.

func (ht *httpnet) TagPeer(p peer.ID, tag string, w int) {
}
func (ht *httpnet) UntagPeer(p peer.ID, tag string) {
}

func (ht *httpnet) Protect(p peer.ID, tag string) {
}
func (ht *httpnet) Unprotect(p peer.ID, tag string) bool {
	return false
}

// **

func (ht *httpnet) Stats() network.Stats {
	return network.Stats{
		MessagesRecvd: atomic.LoadUint64(&ht.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&ht.stats.MessagesSent),
	}
}

func (ht *httpnet) buildRequest(ctx context.Context, pid peer.ID, u *url.URL, method string, cid string) (*http.Request, error) {
	// copy url
	sendURL, _ := url.Parse(u.String())
	sendURL.RawQuery = "format=raw"
	sendURL.Path += "/ipfs/" + cid

	ctx = context.WithValue(ctx, pidCtxKey, pid)
	req, err := http.NewRequestWithContext(ctx,
		"GET",
		sendURL.String(),
		nil,
	)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	headers := make(http.Header)
	headers.Add("Accept", "application/vnd.ipld.raw")
	headers.Add("User-Agent", ht.userAgent)
	req.Header = headers
	return req, nil
}

func (ht *httpnet) NewMessageSender(ctx context.Context, p peer.ID, opts *network.MessageSenderOpts) (network.MessageSender, error) {
	log.Debugf("NewMessageSender: %s", p)
	pi := ht.host.Peerstore().PeerInfo(p)
	urls := network.ExtractURLsFromPeer(pi)
	if len(urls) == 0 {
		return nil, ErrNoHTTPAddresses
	}

	return &httpMsgSender{
		// ctx ??
		ht:      ht,
		peer:    p,
		urls:    urls,
		closing: make(chan struct{}, 1),
		// opts: todo
	}, nil
}

type httpMsgSender struct {
	peer      peer.ID
	urls      []*url.URL
	ht        *httpnet
	opts      network.MessageSenderOpts
	closing   chan struct{}
	closeOnce sync.Once
}

// SendMsg performs an http request for the wanted cids per the msg's
// Wantlist. It reads the response and records it in a reponse BitswapMessage
// which is forwarded to the receivers (in a separate goroutine).
func (sender *httpMsgSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	// SendMsg gets called from MessageQueue and returning an error
	// results in a MessageQueue shutdown. Errors are only returned when
	// we are unable to obtain a single valid Block/Has response. When a
	// URL errors in a bad way (connection, 500s), we continue checking
	// with the next available one.

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bsresp := msg.Clone()
	recvdBlocks := make(map[cid.Cid]struct{})
	recvdHas := make(map[cid.Cid]struct{})

	go func() {
		for range sender.closing {
			cancel()
		}
	}()

	// This is mostly a cosmetic action since the bitswap.Client is just
	// logging the errors.
	sendErrors := func(err error) {
		if err != nil {
			for _, recv := range sender.ht.receivers {
				recv.ReceiveError(err)
			}
		}
	}

	// We will try the HTTP URLs from peer and send the wantlist to one of
	// them. We switch to the next URL in case of failures.  We can 1)
	// loop on urls and then wantlist, by keeping track of which blocks we
	// have successfully requested already, or 2) loop on wantlist and
	// then URLs, keeping track which URLs we should skip due to errors
	// etc.  It is simpler to do 1 and offers other small advantages
	// (i.e. request object re-use).

	var merr error // collects all server errors

	for _, senderURL := range sender.urls {
		if sender.ht.isInCooldown(senderURL) {
			// this URL is cooling down due to previous
			continue // with next url
		}

		req, err := sender.ht.buildRequest(ctx, sender.peer, senderURL, "GET", "")
		if err != nil {
			log.Debug(err)
			merr = multierr.Append(merr, err)
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			continue // with next url
		}
		// Store original path in case the "/ipfs/..." url is mounted
		// on a subpath so we can just append to it.
		origPath := req.URL.Path

	WANTLIST_LOOP:
		for _, entry := range msg.Wantlist() {
			// Reset the path to append a different CID.
			req.URL.Path = origPath

			var method string
			var resp *http.Response
			var body []byte

			switch {
			case entry.Cancel: // probably should not happen?
				err = errors.New("received entry of type cancel")
				log.Debug(err)
				return err // full abort

			case entry.WantType == pb.Message_Wantlist_Block:
				if _, ok := recvdBlocks[entry.Cid]; ok {
					// previous url provided this block
					continue // with next entry in wantlist.
				}
				method = "GET"
			case entry.WantType == pb.Message_Wantlist_Have:
				if _, ok := recvdHas[entry.Cid]; ok {
					// previous url provided this Has
					continue // with next entry in wantlist.
				}
				method = "HEAD"
			default:
				continue // with next entry, given unknown type.
			}

			req.Method = method
			req.URL.Path += entry.Cid.String()

			log.Debugf("cid request to %s %s", method, req.URL)
			resp, err = sender.ht.client.Do(req)
			if err != nil {
				err = fmt.Errorf("error making request to %s: %w", req.URL, err)
				merr = multierr.Append(merr, err)
				log.Debug(err)
				break WANTLIST_LOOP // stop processing wantlist. Try with next url.
			}

			limReader := &io.LimitedReader{
				R: resp.Body,
				N: sender.ht.maxBlockSize,
			}

			body, err = io.ReadAll(limReader)
			if err != nil {
				err = fmt.Errorf("error reading body from %s: %w", req.URL, err)
				merr = multierr.Append(merr, err)
				log.Debug(err)
				break WANTLIST_LOOP // stop processing wantlist. Try with next url.
			}

			sender.ht.connEvtMgr.OnMessage(sender.peer)
			switch resp.StatusCode {
			// Valid responses signaling unavailability of the
			// content.
			case http.StatusNotFound,
				http.StatusGone,
				http.StatusForbidden,
				http.StatusUnavailableForLegalReasons:
				if entry.SendDontHave {
					bsresp.AddDontHave(entry.Cid)
				}
				continue // with next entry in wantlist.

			case http.StatusOK: // \(^°^)/
				if req.Method == "HEAD" {
					bsresp.AddHave(entry.Cid)
					continue // with next entry in wantlist
				}

				// GET
				b, err := blocks.NewBlockWithCid(body, entry.Cid)
				if err != nil {
					log.Error("block received for cid %s does not match!", entry.Cid)
					continue // with next entry in wantlist
				}
				bsresp.AddBlock(b)
				continue // with next entry in wantlist

			// For any other code, we assume we must temporally
			// backoff from the URL.
			default:
				err = fmt.Errorf("%s -> %d: %s", req.URL, resp.StatusCode, string(body))
				merr = multierr.Append(merr, err)

				cooldownPeriod := sender.ht.defaultCooldownPeriod
				retryAfter := resp.Header.Get("Retry-After")
				if d := parseRetryAfter(retryAfter); d > 0 {
					cooldownPeriod = d
				}
				sender.ht.setCooldownDuration(senderURL, cooldownPeriod)
				break WANTLIST_LOOP // and try next URL
			}
		}
	}

	// send what we got ReceiveMessage and return
	go func(receivers []network.Receiver, p peer.ID, msg bsmsg.BitSwapMessage) {
		// todo: do not hang if closing
		for i, recv := range receivers {
			log.Debugf("Calling ReceiveMessage from %s (receiver %d)", p, i)
			recv.ReceiveMessage(
				context.Background(), // todo: which context?
				p,
				msg,
			)
		}
	}(sender.ht.receivers, sender.peer, bsresp)

	sendErrors(merr)

	// If we did not manage to obtain anything, return an errors if they
	// existed.
	if len(recvdBlocks)+len(recvdHas) == 0 {
		return merr
	}
	// Otherwise errors are "ignored" (no need to disconnect). If we are
	// in cooldown period, the connection might be eventually closed or we
	// might retry later.
	return nil
}

func (sender *httpMsgSender) Close() error {
	sender.closeOnce.Do(func() {
		close(sender.closing)
	})
	return nil
}

func (sender *httpMsgSender) Reset() error {
	return nil
}

func (sender *httpMsgSender) SupportsHave() bool {
	return false
}

// defaultUserAgent returns a useful user agent version string allowing us to
// identify requests coming from official releases of this module vs forks.
func defaultUserAgent() (ua string) {
	p := reflect.ValueOf(httpnet{}).Type().PkgPath()
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

// parseRetryAfter returns how many seconds the Retry-After header header
// wants us to wait.
func parseRetryAfter(ra string) time.Duration {
	if len(ra) == 0 {
		return 0
	}
	var d time.Duration
	secs, err := strconv.ParseInt(ra, 10, 64)
	if err != nil {
		date, err := time.Parse(time.RFC1123, ra)
		if err != nil {
			return 0
		}
		d = time.Until(date)
	} else {
		d = time.Duration(secs)
	}

	if d < 0 {
		d = 0
	}
	return d
}
