package httpnet

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

// pingInterval is how often the per-host ticker probes each unique HTTP
// endpoint to refresh its latency reading.
const pingInterval = 5 * time.Second

// ewmaSmoothing is the smoothing factor for the per-host latency EWMA.
// Matches the value used by the libp2p peerstore's LatencyEWMA.
const ewmaSmoothing = 0.1

// pinger probes HTTP endpoints periodically and tracks their latency.
//
// State is keyed by HTTP endpoint, not by peer ID. Multiple peer IDs that
// resolve to the same endpoint share one ticker and one latency reading.
// /dns/a-fil-http.aur.lu/tcp/443/https is currently advertised by three
// peer IDs in delegated-ipfs.dev responses; with per-peer pinging, three
// peer IDs idle on that gateway produce 36 HEAD requests per minute. The
// gateway is a single physical resource, so we ping it once.
type pinger struct {
	ht *Network

	mu sync.Mutex
	// hosts holds one entry per unique HTTP endpoint currently being
	// pinged. Keyed by endpointKey.
	hosts map[string]*hostPing
	// peerHosts maps each registered peer to the host keys it depends
	// on, so stopPinging can decrement the right refcounts.
	peerHosts map[peer.ID]map[string]struct{}
}

// hostPing tracks the periodic probe for one HTTP endpoint plus its
// latency reading. refcount counts the peer IDs registered against it;
// the ticker runs while refcount > 0.
type hostPing struct {
	url    network.ParsedURL
	method string
	// peerID is a representative peer.ID kept for connectToURL logging.
	// It is captured at first registration; later peers piggyback on
	// this one's probes.
	peerID peer.ID

	refcount int
	cancel   context.CancelFunc

	latencyMu sync.Mutex
	latency   time.Duration // EWMA, zero until the first sample
}

func newPinger(ht *Network) *pinger {
	return &pinger{
		ht:        ht,
		hosts:     make(map[string]*hostPing),
		peerHosts: make(map[peer.ID]map[string]struct{}),
	}
}

// ping issues immediate probes against every URL of p in parallel and
// returns the average RTT. It also records the per-host latency so that
// subsequent latency() calls reflect the fresh reading.
//
// ping is intended for callers that need a fresh sample (Network.Ping);
// the periodic ticker handles the steady-state case independently.
func (pngr *pinger) ping(ctx context.Context, p peer.ID) ping.Result {
	pi := pngr.ht.host.Peerstore().PeerInfo(p)
	urls := network.ExtractURLsFromPeer(pi)
	if len(urls) == 0 {
		return ping.Result{Error: ErrNoHTTPAddresses}
	}

	method := "GET"
	if supportsHave(pngr.ht.host.Peerstore(), p) {
		method = "HEAD"
	}

	results := make(chan ping.Result, len(urls))
	for _, u := range urls {
		go func(u network.ParsedURL) {
			start := time.Now()
			_, err := pngr.ht.connectToURL(ctx, p, u, method)
			if err != nil {
				log.Debug(err)
				results <- ping.Result{Error: err}
				return
			}
			rtt := time.Since(start)
			pngr.recordHostLatency(u, rtt)
			results <- ping.Result{RTT: rtt}
		}(u)
	}

	var result ping.Result
	var errs []error
	for range urls {
		r := <-results
		if r.Error != nil {
			errs = append(errs, r.Error)
			continue
		}
		result.RTT += r.RTT
	}
	close(results)

	if len(errs) == len(urls) {
		return ping.Result{Error: errors.Join(errs...)}
	}
	result.RTT = result.RTT / time.Duration(len(urls)-len(errs))
	return result
}

// recordHostLatency updates the EWMA for the host that u points to. The
// host entry must already exist (created by startPinging).
func (pngr *pinger) recordHostLatency(u network.ParsedURL, next time.Duration) {
	key := endpointKey(u.URL.Scheme, u.URL.Host, u.SNI)
	pngr.mu.Lock()
	hp := pngr.hosts[key]
	pngr.mu.Unlock()
	if hp == nil {
		return
	}
	hp.recordLatency(next)
}

// latency returns the average EWMA latency across all hosts that p is
// registered against. Returns zero when p has no registered hosts or no
// host has produced a sample yet.
func (pngr *pinger) latency(p peer.ID) time.Duration {
	pngr.mu.Lock()
	keys := pngr.peerHosts[p]
	hosts := make([]*hostPing, 0, len(keys))
	for k := range keys {
		if hp, ok := pngr.hosts[k]; ok {
			hosts = append(hosts, hp)
		}
	}
	pngr.mu.Unlock()

	var total time.Duration
	var samples int
	for _, hp := range hosts {
		if l := hp.currentLatency(); l > 0 {
			total += l
			samples++
		}
	}
	if samples == 0 {
		return 0
	}
	return total / time.Duration(samples)
}

// startPinging registers p with all of its known HTTP endpoints. New
// endpoints get a ticker; endpoints already pinged (because another peer
// shares them) just bump their refcount.
func (pngr *pinger) startPinging(p peer.ID) {
	pi := pngr.ht.host.Peerstore().PeerInfo(p)
	urls := network.ExtractURLsFromPeer(pi)
	if len(urls) == 0 {
		log.Debugf("startPinging: no HTTP URLs for %s", p)
		return
	}

	method := "GET"
	if supportsHave(pngr.ht.host.Peerstore(), p) {
		method = "HEAD"
	}

	pngr.mu.Lock()
	defer pngr.mu.Unlock()

	if _, ok := pngr.peerHosts[p]; ok {
		log.Debugf("already pinging %s", p)
		return
	}
	keys := make(map[string]struct{}, len(urls))
	for _, u := range urls {
		key := endpointKey(u.URL.Scheme, u.URL.Host, u.SNI)
		if _, dup := keys[key]; dup {
			continue // peer advertised the same endpoint twice.
		}
		keys[key] = struct{}{}

		if hp, ok := pngr.hosts[key]; ok {
			hp.refcount++
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		hp := &hostPing{
			url:      u,
			method:   method,
			peerID:   p,
			refcount: 1,
			cancel:   cancel,
		}
		pngr.hosts[key] = hp
		go pngr.tickerLoop(ctx, hp)
	}
	pngr.peerHosts[p] = keys
	log.Debugf("starting pings for %s on %d hosts", p, len(keys))
}

// stopPinging unregisters p. Hosts whose refcount drops to zero have
// their ticker cancelled and their state removed.
func (pngr *pinger) stopPinging(p peer.ID) {
	pngr.mu.Lock()
	defer pngr.mu.Unlock()

	keys, ok := pngr.peerHosts[p]
	if !ok {
		return
	}
	delete(pngr.peerHosts, p)
	log.Debugf("stopping pings for %s", p)

	for key := range keys {
		hp := pngr.hosts[key]
		if hp == nil {
			continue
		}
		hp.refcount--
		if hp.refcount > 0 {
			continue
		}
		hp.cancel()
		delete(pngr.hosts, key)
	}
}

// isPinging reports whether p has any host registrations.
func (pngr *pinger) isPinging(p peer.ID) bool {
	pngr.mu.Lock()
	defer pngr.mu.Unlock()
	_, ok := pngr.peerHosts[p]
	return ok
}

// tickerLoop probes hp on pingInterval until ctx is cancelled.
func (pngr *pinger) tickerLoop(ctx context.Context, hp *hostPing) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pngr.tickOnce(ctx, hp)
		}
	}
}

func (pngr *pinger) tickOnce(ctx context.Context, hp *hostPing) {
	start := time.Now()
	if _, err := pngr.ht.connectToURL(ctx, hp.peerID, hp.url, hp.method); err != nil {
		log.Debug(err)
		return
	}
	hp.recordLatency(time.Since(start))
}

// recordLatency updates the host's EWMA latency. The first sample is
// stored as-is; subsequent samples are smoothed.
func (hp *hostPing) recordLatency(next time.Duration) {
	hp.latencyMu.Lock()
	defer hp.latencyMu.Unlock()
	if hp.latency == 0 {
		hp.latency = next
		return
	}
	hp.latency = time.Duration((1.0-ewmaSmoothing)*float64(hp.latency) + ewmaSmoothing*float64(next))
}

func (hp *hostPing) currentLatency() time.Duration {
	hp.latencyMu.Lock()
	defer hp.latencyMu.Unlock()
	return hp.latency
}
