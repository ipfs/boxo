package bitswap

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/deque"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	iface "github.com/ipfs/boxo/bitswap/network"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	delay "github.com/ipfs/go-ipfs-delay"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"google.golang.org/protobuf/proto"
)

// VirtualNetwork generates a new testnet instance - a fake network that
// is used to simulate sending messages.
func VirtualNetwork(d delay.D) Network {
	return &network{
		latencies:          make(map[peer.ID]map[peer.ID]time.Duration),
		clients:            make(map[peer.ID]*receiverQueue),
		delay:              d,
		isRateLimited:      false,
		rateLimitGenerator: nil,
		conns:              make(map[string]struct{}),
	}
}

// RateLimitGenerator is an interface for generating rate limits across peers
type RateLimitGenerator interface {
	NextRateLimit() float64
}

// RateLimitedVirtualNetwork generates a testnet instance where nodes are rate
// limited in the upload/download speed.
func RateLimitedVirtualNetwork(d delay.D, rateLimitGenerator RateLimitGenerator) Network {
	return &network{
		latencies:          make(map[peer.ID]map[peer.ID]time.Duration),
		rateLimiters:       make(map[peer.ID]map[peer.ID]*mocknet.RateLimiter),
		clients:            make(map[peer.ID]*receiverQueue),
		delay:              d,
		isRateLimited:      true,
		rateLimitGenerator: rateLimitGenerator,
		conns:              make(map[string]struct{}),
	}
}

type network struct {
	mu                 sync.Mutex
	latencies          map[peer.ID]map[peer.ID]time.Duration
	rateLimiters       map[peer.ID]map[peer.ID]*mocknet.RateLimiter
	clients            map[peer.ID]*receiverQueue
	delay              delay.D
	isRateLimited      bool
	rateLimitGenerator RateLimitGenerator
	conns              map[string]struct{}
}

type message struct {
	from       peer.ID
	msg        bsmsg.BitSwapMessage
	shouldSend time.Time
}

// receiverQueue queues up a set of messages to be sent, and sends them *in
// order* with their delays respected as much as sending them in order allows
// for
type receiverQueue struct {
	receiver *networkClient
	queue    deque.Deque[*message]
	active   bool
	lk       sync.Mutex
}

func (n *network) Adapter(p tnet.Identity, opts ...bsnet.NetOpt) iface.BitSwapNetwork {
	n.mu.Lock()
	defer n.mu.Unlock()

	s := bsnet.Settings{
		SupportedProtocols: []protocol.ID{
			bsnet.ProtocolBitswap,
			bsnet.ProtocolBitswapOneOne,
			bsnet.ProtocolBitswapOneZero,
			bsnet.ProtocolBitswapNoVers,
		},
	}
	for _, opt := range opts {
		opt(&s)
	}

	client := &networkClient{
		local:              p.ID(),
		network:            n,
		supportedProtocols: s.SupportedProtocols,
	}
	n.clients[p.ID()] = &receiverQueue{receiver: client}
	return client
}

func (n *network) HasPeer(p peer.ID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, found := n.clients[p]
	return found
}

// TODO should this be completely asynchronous?
// TODO what does the network layer do with errors received from services?
func (n *network) SendMessage(
	ctx context.Context,
	from peer.ID,
	to peer.ID,
	mes bsmsg.BitSwapMessage,
) error {
	mes = mes.Clone()

	n.mu.Lock()
	defer n.mu.Unlock()

	latencies, ok := n.latencies[from]
	if !ok {
		latencies = make(map[peer.ID]time.Duration)
		n.latencies[from] = latencies
	}

	latency, ok := latencies[to]
	if !ok {
		latency = n.delay.NextWaitTime()
		latencies[to] = latency
	}

	var bandwidthDelay time.Duration
	if n.isRateLimited {
		rateLimiters, ok := n.rateLimiters[from]
		if !ok {
			rateLimiters = make(map[peer.ID]*mocknet.RateLimiter)
			n.rateLimiters[from] = rateLimiters
		}

		rateLimiter, ok := rateLimiters[to]
		if !ok {
			rateLimiter = mocknet.NewRateLimiter(n.rateLimitGenerator.NextRateLimit())
			rateLimiters[to] = rateLimiter
		}

		size := proto.Size(mes.ToProtoV1())
		bandwidthDelay = rateLimiter.Limit(size)
	} else {
		bandwidthDelay = 0
	}

	receiver, ok := n.clients[to]
	if !ok {
		return errors.New("cannot locate peer on network")
	}

	// nb: terminate the context since the context wouldn't actually be passed
	// over the network in a real scenario

	msg := &message{
		from:       from,
		msg:        mes,
		shouldSend: time.Now().Add(latency).Add(bandwidthDelay),
	}
	receiver.enqueue(msg)

	return nil
}

var _ iface.Receiver = (*networkClient)(nil)

type networkClient struct {
	// These need to be at the top of the struct (allocated on the heap) for alignment on 32bit platforms.
	stats iface.Stats

	local              peer.ID
	receivers          []iface.Receiver
	network            *network
	supportedProtocols []protocol.ID
}

func (nc *networkClient) ReceiveMessage(ctx context.Context, sender peer.ID, incoming bsmsg.BitSwapMessage) {
	for _, v := range nc.receivers {
		v.ReceiveMessage(ctx, sender, incoming)
	}
}

func (nc *networkClient) ReceiveError(e error) {
	for _, v := range nc.receivers {
		v.ReceiveError(e)
	}
}

func (nc *networkClient) PeerConnected(p peer.ID) {
	for _, v := range nc.receivers {
		v.PeerConnected(p)
	}
}

func (nc *networkClient) PeerDisconnected(p peer.ID) {
	for _, v := range nc.receivers {
		v.PeerDisconnected(p)
	}
}

func (nc *networkClient) Self() peer.ID {
	return nc.local
}

func (nc *networkClient) Ping(ctx context.Context, p peer.ID) ping.Result {
	return ping.Result{RTT: nc.Latency(p)}
}

func (nc *networkClient) Latency(p peer.ID) time.Duration {
	nc.network.mu.Lock()
	defer nc.network.mu.Unlock()
	return nc.network.latencies[nc.local][p]
}

func (nc *networkClient) SendMessage(
	ctx context.Context,
	to peer.ID,
	message bsmsg.BitSwapMessage,
) error {
	if err := nc.network.SendMessage(ctx, nc.local, to, message); err != nil {
		return err
	}
	atomic.AddUint64(&nc.stats.MessagesSent, 1)
	return nil
}

func (nc *networkClient) Stats() iface.Stats {
	return iface.Stats{
		MessagesRecvd: atomic.LoadUint64(&nc.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&nc.stats.MessagesSent),
	}
}

func (nc *networkClient) ConnectionManager() connmgr.ConnManager {
	return &connmgr.NullConnMgr{}
}

type messagePasser struct {
	net    *networkClient
	target peer.ID
	local  peer.ID
	ctx    context.Context
}

func (mp *messagePasser) SendMsg(ctx context.Context, m bsmsg.BitSwapMessage) error {
	return mp.net.SendMessage(ctx, mp.target, m)
}

func (mp *messagePasser) Close() error {
	return nil
}

func (mp *messagePasser) Reset() error {
	return nil
}

var oldProtos = map[protocol.ID]struct{}{
	bsnet.ProtocolBitswapNoVers:  {},
	bsnet.ProtocolBitswapOneZero: {},
	bsnet.ProtocolBitswapOneOne:  {},
}

func (mp *messagePasser) SupportsHave() bool {
	protos := mp.net.network.clients[mp.target].receiver.supportedProtocols
	for _, proto := range protos {
		if _, ok := oldProtos[proto]; !ok {
			return true
		}
	}
	return false
}

func (nc *networkClient) NewMessageSender(ctx context.Context, p peer.ID, opts *iface.MessageSenderOpts) (iface.MessageSender, error) {
	return &messagePasser{
		net:    nc,
		target: p,
		local:  nc.local,
		ctx:    ctx,
	}, nil
}

func (nc *networkClient) Start(r ...iface.Receiver) {
	nc.receivers = r
}

func (nc *networkClient) Stop() {
}

func (nc *networkClient) Connect(_ context.Context, p peer.AddrInfo) error {
	nc.network.mu.Lock()
	otherClient, ok := nc.network.clients[p.ID]
	if !ok {
		nc.network.mu.Unlock()
		return errors.New("no such peer in network")
	}

	tag := tagForPeers(nc.local, p.ID)
	if _, ok := nc.network.conns[tag]; ok {
		nc.network.mu.Unlock()
		// log.Warning("ALREADY CONNECTED TO PEER (is this a reconnect? test lib needs fixing)")
		return nil
	}
	nc.network.conns[tag] = struct{}{}
	nc.network.mu.Unlock()

	otherClient.receiver.PeerConnected(nc.local)
	nc.PeerConnected(p.ID)
	return nil
}

func (nc *networkClient) DisconnectFrom(_ context.Context, p peer.ID) error {
	nc.network.mu.Lock()
	defer nc.network.mu.Unlock()

	otherClient, ok := nc.network.clients[p]
	if !ok {
		return errors.New("no such peer in network")
	}

	tag := tagForPeers(nc.local, p)
	if _, ok := nc.network.conns[tag]; !ok {
		// Already disconnected
		return nil
	}
	delete(nc.network.conns, tag)

	otherClient.receiver.PeerDisconnected(nc.local)
	nc.PeerDisconnected(p)
	return nil
}

func (bsnet *networkClient) TagPeer(p peer.ID, tag string, w int) {
}

func (bsnet *networkClient) UntagPeer(p peer.ID, tag string) {
}

func (bsnet *networkClient) Protect(p peer.ID, tag string) {
}

func (bsnet *networkClient) Unprotect(p peer.ID, tag string) bool {
	return false
}

func (rq *receiverQueue) enqueue(m *message) {
	rq.lk.Lock()
	defer rq.lk.Unlock()
	rq.queue.PushBack(m)
	if !rq.active {
		rq.active = true
		go rq.process()
	}
}

func (rq *receiverQueue) Swap(i, j int) {
	rq.queue.Swap(i, j)
}

func (rq *receiverQueue) Len() int {
	return rq.queue.Len()
}

func (rq *receiverQueue) Less(i, j int) bool {
	return rq.queue.At(i).shouldSend.UnixNano() < rq.queue.At(j).shouldSend.UnixNano()
}

func (rq *receiverQueue) process() {
	for {
		rq.lk.Lock()
		if rq.queue.Len() == 0 {
			rq.active = false
			rq.lk.Unlock()
			return
		}
		sort.Sort(rq)
		m := rq.queue.Front()
		if time.Until(m.shouldSend).Seconds() < 0.1 {
			rq.queue.PopFront()
			rq.lk.Unlock()
			time.Sleep(time.Until(m.shouldSend))
			atomic.AddUint64(&rq.receiver.stats.MessagesRecvd, 1)
			rq.receiver.ReceiveMessage(context.TODO(), m.from, m.msg)
		} else {
			rq.lk.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func tagForPeers(a, b peer.ID) string {
	if a < b {
		return string(a + b)
	}
	return string(b + a)
}
