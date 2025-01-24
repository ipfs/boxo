package httpnet

import (
	"context"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	probing "github.com/prometheus-community/pro-bing"
	"go.uber.org/multierr"
)

// pinger pings connected hosts on regular intervals
// and tracks their latency.
type pinger struct {
	host host.Host

	latenciesLock sync.RWMutex
	latencies     map[peer.ID]time.Duration

	pingsLock sync.Mutex
	pings     map[peer.ID]context.CancelFunc
}

func newPinger(h host.Host) *pinger {
	return &pinger{
		host:      h,
		latencies: make(map[peer.ID]time.Duration),
		pings:     make(map[peer.ID]context.CancelFunc),
	}
}

// ping sends a ping packet to the first known url of the given peer and
// returns the result with the latency for this peer. The result is also
// recorded.
func (pngr *pinger) ping(ctx context.Context, p peer.ID) ping.Result {
	pi := pngr.host.Peerstore().PeerInfo(p)
	urls := network.ExtractURLsFromPeer(pi)
	if len(urls) == 0 {
		return ping.Result{
			Error: ErrNoHTTPAddresses,
		}
	}

	results := make(chan ping.Result, len(urls))
	for _, u := range urls {
		go func(u *url.URL) {
			// Remove port from url.
			host, _, err := net.SplitHostPort(u.Host)
			if err != nil {
				results <- ping.Result{
					Error: err,
				}
			}

			pinger, err := probing.NewPinger(host)
			if err != nil {
				log.Debug("pinger error ", err)
				results <- ping.Result{
					Error: err,
				}
			}
			pinger.Count = 1

			err = pinger.RunWithContext(ctx)
			if err != nil {
				log.Debug("ping error ", err)
				results <- ping.Result{
					Error: err,
				}
			}

			results <- ping.Result{
				RTT: pinger.Statistics().AvgRtt,
			}
		}(u.URL)
	}

	var result ping.Result
	var errors error
	for i := 0; i < len(urls); i++ {
		r := <-results
		if r.Error != nil {
			errors = multierr.Append(errors, r.Error)
			continue
		}
		result.RTT += r.RTT
	}
	close(results)

	lenErrors := len(multierr.Errors(errors))
	// if all urls failed return that, otherwise ignore.
	if lenErrors == len(urls) {
		return ping.Result{
			Error: errors,
		}
	}
	result.RTT = result.RTT / time.Duration(len(urls)-lenErrors)

	//log.Debugf("ping latency %s %s", p, result.RTT)
	pngr.recordLatency(p, result.RTT)
	return result
}

// latency returns the recorded latency for the given peer.
func (pngr *pinger) latency(p peer.ID) time.Duration {
	var lat time.Duration
	pngr.latenciesLock.RLock()
	{
		lat = pngr.latencies[p]
	}
	pngr.latenciesLock.RUnlock()
	return lat
}

// recordLatency stores a new latency measurement for the given peer using an
// Exponetially Weighted Moving Average similar to LatencyEWMA from the
// peerstore.
func (pngr *pinger) recordLatency(p peer.ID, next time.Duration) {
	nextf := float64(next)
	s := 0.1
	pngr.latenciesLock.Lock()
	{
		ewma, found := pngr.latencies[p]
		ewmaf := float64(ewma)
		if !found {
			pngr.latencies[p] = next // when no data, just take it as the mean.
		} else {
			nextf = ((1.0 - s) * ewmaf) + (s * nextf)
			pngr.latencies[p] = time.Duration(nextf)
		}
	}
	pngr.latenciesLock.Unlock()
}

func (pngr *pinger) startPinging(p peer.ID) {
	pngr.pingsLock.Lock()
	defer pngr.pingsLock.Unlock()

	_, ok := pngr.pings[p]
	if ok {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	pngr.pings[p] = cancel

	go func(ctx context.Context, p peer.ID) {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pngr.ping(ctx, p)
			}
		}
	}(ctx, p)

}

func (pngr *pinger) stopPinging(p peer.ID) {
	pngr.pingsLock.Lock()
	{
		cancel, ok := pngr.pings[p]
		if ok {
			cancel()
		}
		delete(pngr.pings, p)
	}
	pngr.pingsLock.Unlock()
	pngr.latenciesLock.Lock()
	delete(pngr.latencies, p)
	pngr.latenciesLock.Unlock()

}
