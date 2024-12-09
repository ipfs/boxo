package httpnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	"github.com/ipfs/boxo/bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	probing "github.com/prometheus-community/pro-bing"
)

var log = logging.Logger("httpnet")

var ( // todo
	maxSendTimeout = 2 * time.Minute
	minSendTimeout = 10 * time.Second
	sendLatency    = 2 * time.Second
	minSendRate    = (100 * 1000) / 8 // 100kbit/s
)

var ErrNoHTTPAddresses = errors.New("AddrInfo does not contain any valid HTTP addresses")

var _ network.BitSwapNetwork = (*httpnet)(nil)

type Option func(net *httpnet)

// New returns a BitSwapNetwork supported by underlying IPFS host.
func New(host host.Host, opts ...Option) network.BitSwapNetwork {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
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
		client:    c,
		peerToURL: make(map[peer.ID][]*url.URL),
		urlToPeer: make(map[string]peer.ID),
		latMap:    make(map[peer.ID]time.Duration),
	}

	for _, opt := range opts {
		opt(&net)
	}

	return &net
}

type httpnet struct {
	// NOTE: Stats must be at the top of the heap allocation to ensure 64bit
	// alignment.
	stats network.Stats

	client *http.Client

	// inbound messages from the network are forwarded to the receiver
	receivers []network.Receiver

	urlLock   sync.RWMutex
	peerToURL map[peer.ID][]*url.URL
	urlToPeer map[string]peer.ID

	latMapLock sync.RWMutex
	latMap     map[peer.ID]time.Duration
}

func (bsnet *httpnet) Start(receivers ...network.Receiver) {
	bsnet.receivers = receivers
}

func (bsnet *httpnet) Stop() {
}

func (bsnet *httpnet) addURLs(p peer.AddrInfo) []*url.URL {
	urls := network.ExtractHTTPAddressesFromPeer(p)
	if len(urls) == 0 {
		return nil
	}
	bsnet.urlLock.Lock()
	{
		bsnet.peerToURL[p.ID] = urls
		for _, a := range urls {
			bsnet.urlToPeer[a.String()] = p.ID
		}
	}
	bsnet.urlLock.Unlock()
	return urls
}

func (bsnet *httpnet) Ping(ctx context.Context, p peer.AddrInfo) ping.Result {
	log.Debugf("Ping: %s", p)
	urls := bsnet.addURLs(p)
	if len(urls) == 0 {
		return ping.Result{
			Error: ErrNoHTTPAddresses,
		}
	}

	// pick the first one. In general there should not be more than one
	// url per peer.
	pingURL := urls[0]

	pinger, err := probing.NewPinger(pingURL.Host)
	if err != nil {
		return ping.Result{
			RTT:   0,
			Error: err,
		}
	}
	pinger.Count = 1

	err = pinger.RunWithContext(ctx)
	if err != nil {
		return ping.Result{
			RTT:   0,
			Error: err,
		}
	}
	lat := pinger.Statistics().AvgRtt
	bsnet.recordLatency(p.ID, lat)
	return ping.Result{
		RTT:   lat,
		Error: nil,
	}

}

// TODO
func (bsnet *httpnet) Latency(p peer.AddrInfo) time.Duration {
	var lat time.Duration
	bsnet.latMapLock.RLock()
	{
		lat = bsnet.latMap[p.ID]
	}
	bsnet.latMapLock.RUnlock()

	// Add one more latency measurement every time latency is requested
	// since we don't do it from anywhere else.
	// FIXME: too much too often?
	go func() {
		bsnet.Ping(context.Background(), p)
	}()

	return lat
}

// similar to LatencyIWMA from peerstore.
func (bsnet *httpnet) recordLatency(p peer.ID, next time.Duration) {
	nextf := float64(next)
	s := 0.1
	bsnet.latMapLock.Lock()
	{
		ewma, found := bsnet.latMap[p]
		ewmaf := float64(ewma)
		if !found {
			bsnet.latMap[p] = next // when no data, just take it as the mean.
		} else {
			nextf = ((1.0 - s) * ewmaf) + (s * nextf)
			bsnet.latMap[p] = time.Duration(nextf)
		}
	}
	bsnet.latMapLock.Unlock()
}

func (bsnet *httpnet) SendMessage(ctx context.Context, h peer.AddrInfo, msg bsmsg.BitSwapMessage) error {
	log.Debugf("SendMessage: %s. %s", h, msg)
	// todo opts
	sender, err := bsnet.NewMessageSender(ctx, h, nil)
	if err != nil {
		return err
	}
	return sender.SendMsg(ctx, msg)
}

func (bsnet *httpnet) Connect(ctx context.Context, p peer.AddrInfo) error {
	log.Debugf("Connect: %s", p)
	for _, r := range bsnet.receivers {
		r.PeerConnected(p)
	}
	return nil
}

func (bsnet *httpnet) DisconnectFrom(ctx context.Context, p peer.ID) error {
	return nil
}

func (bsnet *httpnet) Stats() network.Stats {
	return network.Stats{
		MessagesRecvd: atomic.LoadUint64(&bsnet.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&bsnet.stats.MessagesSent),
	}
}

func (nbsnet *httpnet) TagPeer(p peer.AddrInfo, tag string, w int) {
	return
}
func (bsnet *httpnet) UntagPeer(p peer.AddrInfo, tag string) {
	return
}

func (bsnet *httpnet) Protect(p peer.AddrInfo, tag string) {
	return
}
func (bsnet *httpnet) Unprotect(p peer.AddrInfo, tag string) bool {
	return false
}

func (bsnet *httpnet) NewMessageSender(ctx context.Context, pinfo peer.AddrInfo, opts *network.MessageSenderOpts) (network.MessageSender, error) {
	log.Debugf("NewMessageSender: %s", pinfo)
	urls := network.ExtractHTTPAddressesFromPeer(pinfo)
	if len(urls) == 0 {
		return nil, ErrNoHTTPAddresses
	}

	return &httpMsgSender{
		// ctx ??
		peer:      pinfo,
		urls:      urls,
		client:    bsnet.client,
		receivers: bsnet.receivers,
		closing:   make(chan struct{}, 1),
		// opts: todo
	}, nil
}

type httpMsgSender struct {
	client    *http.Client
	peer      peer.AddrInfo
	urls      []*url.URL
	receivers []network.Receiver
	opts      network.MessageSenderOpts
	closing   chan struct{}
	closeOnce sync.Once
}

func (sender *httpMsgSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bsresp := msg.Clone()

	go func() {
		for range sender.closing {
			cancel()
		}
	}()

	sendErrors := func(err error) {
		for _, recv := range sender.receivers {
			recv.ReceiveError(err)
		}
	}

	sendURL, _ := url.Parse(sender.urls[0].String())
	sendURL.RawQuery = "format=raw&dag-scope=block"

	// TODO: assuming we don't have to manage making concurrent
	// requests here.
	for _, entry := range msg.Wantlist() {
		var method string
		switch {
		case entry.Cancel:
			continue // todo: handle cancelling ongoing
		case entry.WantType == pb.Message_Wantlist_Block:
			method = "GET"
		case entry.WantType == pb.Message_Wantlist_Have:
			method = "HEAD"
		default:
			continue
		}

		sendURL.Path = "/ipfs/" + entry.Cid.String()
		headers := make(http.Header)
		headers.Add("Accept", "application/vnd.ipld.raw")

		req, err := http.NewRequestWithContext(ctx,
			method,
			sendURL.String(),
			nil,
		)
		if err != nil {
			log.Error(err)
			break
		}
		req.Header = headers
		log.Debugf("cid request to %s %s", method, sendURL)
		resp, err := sender.client.Do(req)
		if err != nil { // abort talking to this host
			log.Error(err)
			// send error?
			break
		}
		if resp.StatusCode == http.StatusNotFound {
			if entry.SendDontHave {
				bsresp.AddDontHave(entry.Cid)
			}
			continue
		}

		// TODO: fixme: limited reader
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Error(err)
			break
		}

		if resp.StatusCode != http.StatusOK {
			err := fmt.Errorf("%s -> %d: %s", sendURL, resp.StatusCode, string(body))
			sendErrors(err)
			log.Debug(err)
			continue
		}
		switch req.Method {
		case "GET":
			b, err := blocks.NewBlockWithCid(body, entry.Cid)
			if err != nil {
				log.Error("Block received for cid %s does not match!", entry.Cid)
				continue
			}
			bsresp.AddBlock(b)
			continue
		case "HEAD":
			bsresp.AddHave(entry.Cid)
			continue
		}
	}

	// send responses in background
	go func(receivers []network.Receiver, p peer.AddrInfo, msg bsmsg.BitSwapMessage) {
		// todo: do not hang if closing
		for i, recv := range receivers {
			log.Debugf("Calling ReceiveMessage from %s (receiver %d)", p, i)
			recv.ReceiveMessage(
				context.Background(), // todo: which context?
				p,
				msg,
			)
		}
	}(sender.receivers, sender.peer, bsresp)

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
