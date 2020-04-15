package network

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"

	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/connmgr"
	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	peerstore "github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	msgio "github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("bitswap_network")

var sendMessageTimeout = time.Minute * 10

// NewFromIpfsHost returns a BitSwapNetwork supported by underlying IPFS host.
func NewFromIpfsHost(host host.Host, r routing.ContentRouting, opts ...NetOpt) BitSwapNetwork {
	s := processSettings(opts...)

	bitswapNetwork := impl{
		host:    host,
		routing: r,

		protocolBitswapNoVers:  s.ProtocolPrefix + ProtocolBitswapNoVers,
		protocolBitswapOneZero: s.ProtocolPrefix + ProtocolBitswapOneZero,
		protocolBitswapOneOne:  s.ProtocolPrefix + ProtocolBitswapOneOne,
		protocolBitswap:        s.ProtocolPrefix + ProtocolBitswap,

		supportedProtocols: s.SupportedProtocols,
	}
	bitswapNetwork.connectEvtMgr = newConnectEventManager(&bitswapNetwork)

	return &bitswapNetwork
}

func processSettings(opts ...NetOpt) Settings {
	s := Settings{
		SupportedProtocols: []protocol.ID{
			ProtocolBitswap,
			ProtocolBitswapOneOne,
			ProtocolBitswapOneZero,
			ProtocolBitswapNoVers,
		},
	}
	for _, opt := range opts {
		opt(&s)
	}
	for i, proto := range s.SupportedProtocols {
		s.SupportedProtocols[i] = s.ProtocolPrefix + proto
	}
	return s
}

// impl transforms the ipfs network interface, which sends and receives
// NetMessage objects, into the bitswap network interface.
type impl struct {
	// NOTE: Stats must be at the top of the heap allocation to ensure 64bit
	// alignment.
	stats Stats

	host          host.Host
	routing       routing.ContentRouting
	connectEvtMgr *connectEventManager

	protocolBitswapNoVers  protocol.ID
	protocolBitswapOneZero protocol.ID
	protocolBitswapOneOne  protocol.ID
	protocolBitswap        protocol.ID

	supportedProtocols []protocol.ID

	// inbound messages from the network are forwarded to the receiver
	receiver Receiver
}

type streamMessageSender struct {
	to     peer.ID
	stream network.Stream
	bsnet  *impl
	opts   *MessageSenderOpts
	done   chan struct{}
}

func (s *streamMessageSender) Connect(ctx context.Context) (stream network.Stream, err error) {
	defer func() {
		if err != nil {
			s.bsnet.connectEvtMgr.MarkUnresponsive(s.to)
		}
	}()

	if s.stream != nil {
		return s.stream, nil
	}

	if err = s.bsnet.ConnectTo(ctx, s.to); err != nil {
		return nil, err
	}

	stream, err = s.bsnet.newStreamToPeer(ctx, s.to)
	if err == nil {
		s.stream = stream
		return s.stream, nil
	}
	return nil, err
}

func (s *streamMessageSender) Reset() error {
	err := s.stream.Reset()
	s.stream = nil
	return err
}

func (s *streamMessageSender) Close() error {
	close(s.done)
	return helpers.FullClose(s.stream)
}

func (s *streamMessageSender) SupportsHave() bool {
	return s.bsnet.SupportsHave(s.stream.Protocol())
}

func (s *streamMessageSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	// Try to send the message repeatedly
	var err error
	for i := 0; i < s.opts.MaxRetries; i++ {
		if err = s.attemptSend(ctx, msg); err == nil {
			// Sent successfully
			return nil
		}

		// If the sender has been closed or the context cancelled, just bail out
		select {
		case <-ctx.Done():
			return nil
		case <-s.done:
			return nil
		default:
		}

		// Failed to send so reset stream and try again
		_ = s.Reset()

		if i == s.opts.MaxRetries {
			s.bsnet.connectEvtMgr.MarkUnresponsive(s.to)
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-s.done:
			return nil
		case <-time.After(s.opts.SendErrorBackoff):
			// wait a short time in case disconnect notifications are still propagating
			log.Infof("send message to %s failed but context was not Done: %s", s.to, err)
		}
	}
	return err
}

func (s *streamMessageSender) attemptSend(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	sndctx, cancel := context.WithTimeout(ctx, s.opts.SendTimeout)
	defer cancel()

	stream, err := s.Connect(sndctx)
	if err != nil {
		log.Infof("failed to open stream to %s: %s", s.to, err)
		return err
	}

	if err = s.bsnet.msgToStream(sndctx, stream, msg); err != nil {
		log.Infof("failed to send message to %s: %s", s.to, err)
		return err
	}

	return nil
}

func (bsnet *impl) Self() peer.ID {
	return bsnet.host.ID()
}

func (bsnet *impl) Ping(ctx context.Context, p peer.ID) ping.Result {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	res := <-ping.Ping(ctx, bsnet.host, p)
	return res
}

func (bsnet *impl) Latency(p peer.ID) time.Duration {
	return bsnet.host.Peerstore().LatencyEWMA(p)
}

// Indicates whether the given protocol supports HAVE / DONT_HAVE messages
func (bsnet *impl) SupportsHave(proto protocol.ID) bool {
	switch proto {
	case bsnet.protocolBitswapOneOne, bsnet.protocolBitswapOneZero, bsnet.protocolBitswapNoVers:
		return false
	}
	return true
}

func (bsnet *impl) msgToStream(ctx context.Context, s network.Stream, msg bsmsg.BitSwapMessage) error {
	deadline := time.Now().Add(sendMessageTimeout)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}

	if err := s.SetWriteDeadline(deadline); err != nil {
		log.Warnf("error setting deadline: %s", err)
	}

	// Older Bitswap versions use a slightly different wire format so we need
	// to convert the message to the appropriate format depending on the remote
	// peer's Bitswap version.
	switch s.Protocol() {
	case bsnet.protocolBitswapOneOne, bsnet.protocolBitswap:
		if err := msg.ToNetV1(s); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	case bsnet.protocolBitswapOneZero, bsnet.protocolBitswapNoVers:
		if err := msg.ToNetV0(s); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	default:
		return fmt.Errorf("unrecognized protocol on remote: %s", s.Protocol())
	}

	if err := s.SetWriteDeadline(time.Time{}); err != nil {
		log.Warnf("error resetting deadline: %s", err)
	}
	return nil
}

func (bsnet *impl) NewMessageSender(ctx context.Context, p peer.ID, opts *MessageSenderOpts) (MessageSender, error) {
	sender := &streamMessageSender{
		to:    p,
		bsnet: bsnet,
		opts:  opts,
		done:  make(chan struct{}),
	}

	conctx, cancel := context.WithTimeout(ctx, sender.opts.SendTimeout)
	defer cancel()

	_, err := sender.Connect(conctx)
	if err != nil {
		return nil, err
	}
	return sender, nil
}

func (bsnet *impl) SendMessage(
	ctx context.Context,
	p peer.ID,
	outgoing bsmsg.BitSwapMessage) error {

	s, err := bsnet.newStreamToPeer(ctx, p)
	if err != nil {
		return err
	}

	if err = bsnet.msgToStream(ctx, s, outgoing); err != nil {
		_ = s.Reset()
		return err
	}
	atomic.AddUint64(&bsnet.stats.MessagesSent, 1)

	// TODO(https://github.com/libp2p/go-libp2p-net/issues/28): Avoid this goroutine.
	//nolint
	go helpers.AwaitEOF(s)
	return s.Close()
}

func (bsnet *impl) newStreamToPeer(ctx context.Context, p peer.ID) (network.Stream, error) {
	return bsnet.host.NewStream(ctx, p, bsnet.supportedProtocols...)
}

func (bsnet *impl) SetDelegate(r Receiver) {
	bsnet.receiver = r
	for _, proto := range bsnet.supportedProtocols {
		bsnet.host.SetStreamHandler(proto, bsnet.handleNewStream)
	}
	bsnet.host.Network().Notify((*netNotifiee)(bsnet))
	// TODO: StopNotify.

}

func (bsnet *impl) ConnectTo(ctx context.Context, p peer.ID) error {
	return bsnet.host.Connect(ctx, peer.AddrInfo{ID: p})
}

func (bsnet *impl) DisconnectFrom(ctx context.Context, p peer.ID) error {
	panic("Not implemented: DisconnectFrom() is only used by tests")
}

// FindProvidersAsync returns a channel of providers for the given key.
func (bsnet *impl) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.ID {
	out := make(chan peer.ID, max)
	go func() {
		defer close(out)
		providers := bsnet.routing.FindProvidersAsync(ctx, k, max)
		for info := range providers {
			if info.ID == bsnet.host.ID() {
				continue // ignore self as provider
			}
			bsnet.host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.TempAddrTTL)
			select {
			case <-ctx.Done():
				return
			case out <- info.ID:
			}
		}
	}()
	return out
}

// Provide provides the key to the network
func (bsnet *impl) Provide(ctx context.Context, k cid.Cid) error {
	return bsnet.routing.Provide(ctx, k, true)
}

// handleNewStream receives a new stream from the network.
func (bsnet *impl) handleNewStream(s network.Stream) {
	defer s.Close()

	if bsnet.receiver == nil {
		_ = s.Reset()
		return
	}

	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
	for {
		received, err := bsmsg.FromMsgReader(reader)
		if err != nil {
			if err != io.EOF {
				_ = s.Reset()
				go bsnet.receiver.ReceiveError(err)
				log.Debugf("bitswap net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		p := s.Conn().RemotePeer()
		ctx := context.Background()
		log.Debugf("bitswap net handleNewStream from %s", s.Conn().RemotePeer())
		bsnet.connectEvtMgr.OnMessage(s.Conn().RemotePeer())
		bsnet.receiver.ReceiveMessage(ctx, p, received)
		atomic.AddUint64(&bsnet.stats.MessagesRecvd, 1)
	}
}

func (bsnet *impl) ConnectionManager() connmgr.ConnManager {
	return bsnet.host.ConnManager()
}

func (bsnet *impl) Stats() Stats {
	return Stats{
		MessagesRecvd: atomic.LoadUint64(&bsnet.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&bsnet.stats.MessagesSent),
	}
}

type connectEventManager struct {
	bsnet *impl
	lk    sync.Mutex
	conns map[peer.ID]*connState
}

type connState struct {
	refs       int
	responsive bool
}

func newConnectEventManager(bsnet *impl) *connectEventManager {
	return &connectEventManager{
		bsnet: bsnet,
		conns: make(map[peer.ID]*connState),
	}
}

func (c *connectEventManager) Connected(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	state, ok := c.conns[p]
	if !ok {
		state = &connState{responsive: true}
	}
	state.refs++

	if state.refs == 1 && state.responsive {
		c.bsnet.receiver.PeerConnected(p)
	}
}

func (c *connectEventManager) Disconnected(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	state, ok := c.conns[p]
	if !ok {
		// Should never happen
		return
	}
	state.refs--
	c.conns[p] = state

	if state.refs == 0 && state.responsive {
		c.bsnet.receiver.PeerDisconnected(p)
	}
}

func (c *connectEventManager) MarkUnresponsive(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	state, ok := c.conns[p]
	if !ok {
		return
	}
	state.responsive = false
	c.conns[p] = state

	c.bsnet.receiver.PeerDisconnected(p)
}

func (c *connectEventManager) OnMessage(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	state, ok := c.conns[p]
	if ok && !state.responsive {
		state.responsive = true
		c.conns[p] = state
		c.bsnet.receiver.PeerConnected(p)
	}
}

type netNotifiee impl

func (nn *netNotifiee) impl() *impl {
	return (*impl)(nn)
}

func (nn *netNotifiee) Connected(n network.Network, v network.Conn) {
	nn.impl().connectEvtMgr.Connected(v.RemotePeer())
}
func (nn *netNotifiee) Disconnected(n network.Network, v network.Conn) {
	nn.impl().connectEvtMgr.Disconnected(v.RemotePeer())
}
func (nn *netNotifiee) OpenedStream(n network.Network, s network.Stream) {}
func (nn *netNotifiee) ClosedStream(n network.Network, v network.Stream) {}
func (nn *netNotifiee) Listen(n network.Network, a ma.Multiaddr)         {}
func (nn *netNotifiee) ListenClose(n network.Network, a ma.Multiaddr)    {}
