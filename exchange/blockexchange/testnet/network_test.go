package bitswap

import (
	"context"
	"sync"
	"testing"

	bsnet "github.com/ipfs/boxo/swap"
	swapmsg "github.com/ipfs/boxo/swap/message"
	blocks "github.com/ipfs/go-block-format"
	delay "github.com/ipfs/go-ipfs-delay"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestSendMessageAsyncButWaitForResponse(t *testing.T) {
	net := VirtualNetwork(delay.Fixed(0))
	responderPeer := tnet.RandIdentityOrFatal(t)
	waiter := net.Adapter(tnet.RandIdentityOrFatal(t))
	responder := net.Adapter(responderPeer)

	var wg sync.WaitGroup

	wg.Add(1)

	expectedStr := "received async"

	responder.Start(lambda(func(
		ctx context.Context,
		fromWaiter peer.ID,
		msgFromWaiter swapmsg.Wantlist,
	) {
		msgToWaiter := swapmsg.New(true)
		msgToWaiter.AddBlock(blocks.NewBlock([]byte(expectedStr)))
		err := waiter.SendMessage(ctx, fromWaiter, msgToWaiter)
		if err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(responder.Stop)

	waiter.Start(lambda(func(
		ctx context.Context,
		fromResponder peer.ID,
		msgFromResponder swapmsg.Wantlist,
	) {
		// TODO assert that this came from the correct peer and that the message contents are as expected
		ok := false
		for _, b := range msgFromResponder.Blocks() {
			if string(b.RawData()) == expectedStr {
				wg.Done()
				ok = true
			}
		}

		if !ok {
			t.Fatal("Message not received from the responder")
		}
	}))
	t.Cleanup(waiter.Stop)

	messageSentAsync := swapmsg.New(true)
	messageSentAsync.AddBlock(blocks.NewBlock([]byte("data")))
	errSending := waiter.SendMessage(
		context.Background(), responderPeer.ID(), messageSentAsync)
	if errSending != nil {
		t.Fatal(errSending)
	}

	wg.Wait() // until waiter delegate function is executed
}

type receiverFunc func(ctx context.Context, p peer.ID,
	incoming swapmsg.Wantlist)

// lambda returns a Receiver instance given a receiver function
func lambda(f receiverFunc) bsnet.Receiver {
	return &lambdaImpl{
		f: f,
	}
}

type lambdaImpl struct {
	f func(ctx context.Context, p peer.ID, incoming swapmsg.Wantlist)
}

func (lam *lambdaImpl) ReceiveMessage(ctx context.Context,
	p peer.ID, incoming swapmsg.Wantlist,
) {
	lam.f(ctx, p, incoming)
}

func (lam *lambdaImpl) ReceiveError(err error) {
	// TODO log error
}

func (lam *lambdaImpl) PeerConnected(p peer.ID) {
	// TODO
}

func (lam *lambdaImpl) PeerDisconnected(peer.ID) {
	// TODO
}
