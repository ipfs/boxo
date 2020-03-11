package network_test

import (
	"context"
	"testing"
	"time"

	tn "github.com/ipfs/go-bitswap/testnet"
	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	bsnet "github.com/ipfs/go-bitswap/network"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type receiver struct {
	peers           map[peer.ID]struct{}
	messageReceived chan struct{}
	connectionEvent chan struct{}
	lastMessage     bsmsg.BitSwapMessage
	lastSender      peer.ID
}

func newReceiver() *receiver {
	return &receiver{
		peers:           make(map[peer.ID]struct{}),
		messageReceived: make(chan struct{}),
		connectionEvent: make(chan struct{}, 1),
	}
}

func (r *receiver) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming bsmsg.BitSwapMessage) {
	r.lastSender = sender
	r.lastMessage = incoming
	select {
	case <-ctx.Done():
	case r.messageReceived <- struct{}{}:
	}
}

func (r *receiver) ReceiveError(err error) {
}

func (r *receiver) PeerConnected(p peer.ID) {
	r.peers[p] = struct{}{}
	r.connectionEvent <- struct{}{}
}

func (r *receiver) PeerDisconnected(p peer.ID) {
	delete(r.peers, p)
	r.connectionEvent <- struct{}{}
}

func TestMessageSendAndReceive(t *testing.T) {
	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)
	mr := mockrouting.NewServer()
	streamNet, err := tn.StreamNet(ctx, mn, mr)
	if err != nil {
		t.Fatal("Unable to setup network")
	}
	p1 := tnet.RandIdentityOrFatal(t)
	p2 := tnet.RandIdentityOrFatal(t)

	bsnet1 := streamNet.Adapter(p1)
	bsnet2 := streamNet.Adapter(p2)
	r1 := newReceiver()
	r2 := newReceiver()
	bsnet1.SetDelegate(r1)
	bsnet2.SetDelegate(r2)

	err = mn.LinkAll()
	if err != nil {
		t.Fatal(err)
	}
	err = bsnet1.ConnectTo(ctx, p2.ID())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		t.Fatal("did not connect peer")
	case <-r1.connectionEvent:
	}
	err = bsnet2.ConnectTo(ctx, p1.ID())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		t.Fatal("did not connect peer")
	case <-r2.connectionEvent:
	}
	if _, ok := r1.peers[p2.ID()]; !ok {
		t.Fatal("did to connect to correct peer")
	}
	if _, ok := r2.peers[p1.ID()]; !ok {
		t.Fatal("did to connect to correct peer")
	}
	blockGenerator := blocksutil.NewBlockGenerator()
	block1 := blockGenerator.Next()
	block2 := blockGenerator.Next()
	sent := bsmsg.New(false)
	sent.AddEntry(block1.Cid(), 1, pb.Message_Wantlist_Block, true)
	sent.AddBlock(block2)

	err = bsnet1.SendMessage(ctx, p2.ID(), sent)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case <-r2.messageReceived:
	}

	sender := r2.lastSender
	if sender != p1.ID() {
		t.Fatal("received message from wrong node")
	}

	received := r2.lastMessage

	sentWants := sent.Wantlist()
	if len(sentWants) != 1 {
		t.Fatal("Did not add want to sent message")
	}
	sentWant := sentWants[0]
	receivedWants := received.Wantlist()
	if len(receivedWants) != 1 {
		t.Fatal("Did not add want to received message")
	}
	receivedWant := receivedWants[0]
	if receivedWant.Cid != sentWant.Cid ||
		receivedWant.Priority != sentWant.Priority ||
		receivedWant.Cancel != sentWant.Cancel {
		t.Fatal("Sent message wants did not match received message wants")
	}
	sentBlocks := sent.Blocks()
	if len(sentBlocks) != 1 {
		t.Fatal("Did not add block to sent message")
	}
	sentBlock := sentBlocks[0]
	receivedBlocks := received.Blocks()
	if len(receivedBlocks) != 1 {
		t.Fatal("Did not add response to received message")
	}
	receivedBlock := receivedBlocks[0]
	if receivedBlock.Cid() != sentBlock.Cid() {
		t.Fatal("Sent message blocks did not match received message blocks")
	}
}

func TestSupportsHave(t *testing.T) {
	ctx := context.Background()
	mn := mocknet.New(ctx)
	mr := mockrouting.NewServer()
	streamNet, err := tn.StreamNet(ctx, mn, mr)
	if err != nil {
		t.Fatal("Unable to setup network")
	}

	type testCase struct {
		proto           protocol.ID
		expSupportsHave bool
	}

	testCases := []testCase{
		testCase{bsnet.ProtocolBitswap, true},
		testCase{bsnet.ProtocolBitswapOneOne, false},
		testCase{bsnet.ProtocolBitswapOneZero, false},
		testCase{bsnet.ProtocolBitswapNoVers, false},
	}

	for _, tc := range testCases {
		p1 := tnet.RandIdentityOrFatal(t)
		bsnet1 := streamNet.Adapter(p1)
		bsnet1.SetDelegate(newReceiver())

		p2 := tnet.RandIdentityOrFatal(t)
		bsnet2 := streamNet.Adapter(p2, bsnet.SupportedProtocols([]protocol.ID{tc.proto}))
		bsnet2.SetDelegate(newReceiver())

		err = mn.LinkAll()
		if err != nil {
			t.Fatal(err)
		}

		senderCurrent, err := bsnet1.NewMessageSender(ctx, p2.ID())
		if err != nil {
			t.Fatal(err)
		}

		if senderCurrent.SupportsHave() != tc.expSupportsHave {
			t.Fatal("Expected sender HAVE message support", tc.proto, tc.expSupportsHave)
		}
	}
}
