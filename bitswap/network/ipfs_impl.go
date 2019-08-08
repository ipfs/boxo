package network

import (
	"context"
	"fmt"
	"io"
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
	msgio "github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("bitswap_network")

var sendMessageTimeout = time.Minute * 10

// NewFromIpfsHost returns a BitSwapNetwork supported by underlying IPFS host.
func NewFromIpfsHost(host host.Host, r routing.ContentRouting, opts ...NetOpt) BitSwapNetwork {
	s := Settings{}
	for _, opt := range opts {
		opt(&s)
	}

	bitswapNetwork := impl{
		host:    host,
		routing: r,

		protocolBitswap:       s.ProtocolPrefix + ProtocolBitswap,
		protocolBitswapOne:    s.ProtocolPrefix + ProtocolBitswapOne,
		protocolBitswapNoVers: s.ProtocolPrefix + ProtocolBitswapNoVers,
	}
	return &bitswapNetwork
}

// impl transforms the ipfs network interface, which sends and receives
// NetMessage objects, into the bitswap network interface.
type impl struct {
	host    host.Host
	routing routing.ContentRouting

	protocolBitswap       protocol.ID
	protocolBitswapOne    protocol.ID
	protocolBitswapNoVers protocol.ID

	// inbound messages from the network are forwarded to the receiver
	receiver Receiver

	stats Stats
}

type streamMessageSender struct {
	s     network.Stream
	bsnet *impl
}

func (s *streamMessageSender) Close() error {
	return helpers.FullClose(s.s)
}

func (s *streamMessageSender) Reset() error {
	return s.s.Reset()
}

func (s *streamMessageSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	return s.bsnet.msgToStream(ctx, s.s, msg)
}

func (bsnet *impl) msgToStream(ctx context.Context, s network.Stream, msg bsmsg.BitSwapMessage) error {
	deadline := time.Now().Add(sendMessageTimeout)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}

	if err := s.SetWriteDeadline(deadline); err != nil {
		log.Warningf("error setting deadline: %s", err)
	}

	switch s.Protocol() {
	case bsnet.protocolBitswap:
		if err := msg.ToNetV1(s); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	case bsnet.protocolBitswapOne, bsnet.protocolBitswapNoVers:
		if err := msg.ToNetV0(s); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	default:
		return fmt.Errorf("unrecognized protocol on remote: %s", s.Protocol())
	}

	if err := s.SetWriteDeadline(time.Time{}); err != nil {
		log.Warningf("error resetting deadline: %s", err)
	}
	return nil
}

func (bsnet *impl) NewMessageSender(ctx context.Context, p peer.ID) (MessageSender, error) {
	s, err := bsnet.newStreamToPeer(ctx, p)
	if err != nil {
		return nil, err
	}

	return &streamMessageSender{s: s, bsnet: bsnet}, nil
}

func (bsnet *impl) newStreamToPeer(ctx context.Context, p peer.ID) (network.Stream, error) {
	return bsnet.host.NewStream(ctx, p, bsnet.protocolBitswap, bsnet.protocolBitswapOne, bsnet.protocolBitswapNoVers)
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
		s.Reset()
		return err
	}
	atomic.AddUint64(&bsnet.stats.MessagesSent, 1)

	// TODO(https://github.com/libp2p/go-libp2p-net/issues/28): Avoid this goroutine.
	go helpers.AwaitEOF(s)
	return s.Close()

}

func (bsnet *impl) SetDelegate(r Receiver) {
	bsnet.receiver = r
	bsnet.host.SetStreamHandler(bsnet.protocolBitswap, bsnet.handleNewStream)
	bsnet.host.SetStreamHandler(bsnet.protocolBitswapOne, bsnet.handleNewStream)
	bsnet.host.SetStreamHandler(bsnet.protocolBitswapNoVers, bsnet.handleNewStream)
	bsnet.host.Network().Notify((*netNotifiee)(bsnet))
	// TODO: StopNotify.

}

func (bsnet *impl) ConnectTo(ctx context.Context, p peer.ID) error {
	return bsnet.host.Connect(ctx, peer.AddrInfo{ID: p})
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
		s.Reset()
		return
	}

	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
	for {
		received, err := bsmsg.FromMsgReader(reader)
		if err != nil {
			if err != io.EOF {
				s.Reset()
				go bsnet.receiver.ReceiveError(err)
				log.Debugf("bitswap net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		p := s.Conn().RemotePeer()
		ctx := context.Background()
		log.Debugf("bitswap net handleNewStream from %s", s.Conn().RemotePeer())
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

type netNotifiee impl

func (nn *netNotifiee) impl() *impl {
	return (*impl)(nn)
}

func (nn *netNotifiee) Connected(n network.Network, v network.Conn) {
	nn.impl().receiver.PeerConnected(v.RemotePeer())
}

func (nn *netNotifiee) Disconnected(n network.Network, v network.Conn) {
	nn.impl().receiver.PeerDisconnected(v.RemotePeer())
}

func (nn *netNotifiee) OpenedStream(n network.Network, v network.Stream) {}
func (nn *netNotifiee) ClosedStream(n network.Network, v network.Stream) {}
func (nn *netNotifiee) Listen(n network.Network, a ma.Multiaddr)         {}
func (nn *netNotifiee) ListenClose(n network.Network, a ma.Multiaddr)    {}
