package messagequeue

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"
	"github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	cid "github.com/ipfs/go-cid"

	bsmsg "github.com/ipfs/go-bitswap/message"
	bsnet "github.com/ipfs/go-bitswap/network"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

type fakeMessageNetwork struct {
	connectError       error
	messageSenderError error
	messageSender      bsnet.MessageSender
}

func (fmn *fakeMessageNetwork) ConnectTo(context.Context, peer.ID) error {
	return fmn.connectError
}

func (fmn *fakeMessageNetwork) NewMessageSender(context.Context, peer.ID, *bsnet.MessageSenderOpts) (bsnet.MessageSender, error) {
	if fmn.messageSenderError == nil {
		return fmn.messageSender, nil
	}
	return nil, fmn.messageSenderError
}

func (fms *fakeMessageNetwork) Self() peer.ID                 { return "" }
func (fms *fakeMessageNetwork) Latency(peer.ID) time.Duration { return 0 }
func (fms *fakeMessageNetwork) Ping(context.Context, peer.ID) ping.Result {
	return ping.Result{Error: fmt.Errorf("ping error")}
}

type fakeDontHaveTimeoutMgr struct {
	lk sync.Mutex
	ks []cid.Cid
}

func (fp *fakeDontHaveTimeoutMgr) Start()    {}
func (fp *fakeDontHaveTimeoutMgr) Shutdown() {}
func (fp *fakeDontHaveTimeoutMgr) AddPending(ks []cid.Cid) {
	fp.lk.Lock()
	defer fp.lk.Unlock()

	s := cid.NewSet()
	for _, c := range append(fp.ks, ks...) {
		s.Add(c)
	}
	fp.ks = s.Keys()
}
func (fp *fakeDontHaveTimeoutMgr) CancelPending(ks []cid.Cid) {
	fp.lk.Lock()
	defer fp.lk.Unlock()

	s := cid.NewSet()
	for _, c := range fp.ks {
		s.Add(c)
	}
	for _, c := range ks {
		s.Remove(c)
	}
	fp.ks = s.Keys()
}
func (fp *fakeDontHaveTimeoutMgr) pendingCount() int {
	fp.lk.Lock()
	defer fp.lk.Unlock()

	return len(fp.ks)
}

type fakeMessageSender struct {
	lk           sync.Mutex
	reset        chan<- struct{}
	messagesSent chan<- []bsmsg.Entry
	supportsHave bool
}

func newFakeMessageSender(reset chan<- struct{},
	messagesSent chan<- []bsmsg.Entry, supportsHave bool) *fakeMessageSender {

	return &fakeMessageSender{
		reset:        reset,
		messagesSent: messagesSent,
		supportsHave: supportsHave,
	}
}

func (fms *fakeMessageSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	fms.lk.Lock()
	defer fms.lk.Unlock()

	fms.messagesSent <- msg.Wantlist()
	return nil
}
func (fms *fakeMessageSender) Close() error       { return nil }
func (fms *fakeMessageSender) Reset() error       { fms.reset <- struct{}{}; return nil }
func (fms *fakeMessageSender) SupportsHave() bool { return fms.supportsHave }

func mockTimeoutCb(peer.ID, []cid.Cid) {}

func collectMessages(ctx context.Context,
	t *testing.T,
	messagesSent <-chan []bsmsg.Entry,
	timeout time.Duration) [][]bsmsg.Entry {
	var messagesReceived [][]bsmsg.Entry
	timeoutctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case messageReceived := <-messagesSent:
			messagesReceived = append(messagesReceived, messageReceived)
		case <-timeoutctx.Done():
			return messagesReceived
		}
	}
}

func totalEntriesLength(messages [][]bsmsg.Entry) int {
	totalLength := 0
	for _, m := range messages {
		totalLength += len(m)
	}
	return totalLength
}

func TestStartupAndShutdown(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, true)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet, mockTimeoutCb)
	bcstwh := testutil.GenerateCids(10)

	messageQueue.Startup()
	messageQueue.AddBroadcastWantHaves(bcstwh)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were sent for broadcast want-haves")
	}

	firstMessage := messages[0]
	if len(firstMessage) != len(bcstwh) {
		t.Fatal("did not add all wants to want list")
	}
	for _, entry := range firstMessage {
		if entry.Cancel {
			t.Fatal("initial add sent cancel entry when it should not have")
		}
	}

	messageQueue.Shutdown()

	timeoutctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	select {
	case <-resetChan:
	case <-timeoutctx.Done():
		t.Fatal("message sender should have been reset but wasn't")
	}
}

func TestSendingMessagesDeduped(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, true)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet, mockTimeoutCb)
	wantHaves := testutil.GenerateCids(10)
	wantBlocks := testutil.GenerateCids(10)

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks, wantHaves)
	messageQueue.AddWants(wantBlocks, wantHaves)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if totalEntriesLength(messages) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("Messages were not deduped")
	}
}

func TestSendingMessagesPartialDupe(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, true)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet, mockTimeoutCb)
	wantHaves := testutil.GenerateCids(10)
	wantBlocks := testutil.GenerateCids(10)

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks[:8], wantHaves[:8])
	messageQueue.AddWants(wantBlocks[3:], wantHaves[3:])
	messages := collectMessages(ctx, t, messagesSent, 20*time.Millisecond)

	if totalEntriesLength(messages) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("messages were not correctly deduped")
	}
}

func TestSendingMessagesPriority(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, true)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet, mockTimeoutCb)
	wantHaves1 := testutil.GenerateCids(5)
	wantHaves2 := testutil.GenerateCids(5)
	wantHaves := append(wantHaves1, wantHaves2...)
	wantBlocks1 := testutil.GenerateCids(5)
	wantBlocks2 := testutil.GenerateCids(5)
	wantBlocks := append(wantBlocks1, wantBlocks2...)

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks1, wantHaves1)
	messageQueue.AddWants(wantBlocks2, wantHaves2)
	messages := collectMessages(ctx, t, messagesSent, 20*time.Millisecond)

	if totalEntriesLength(messages) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("wrong number of wants")
	}
	byCid := make(map[cid.Cid]message.Entry)
	for _, entry := range messages[0] {
		byCid[entry.Cid] = entry
	}

	// Check that earliest want-haves have highest priority
	for i := range wantHaves {
		if i > 0 {
			if byCid[wantHaves[i]].Priority > byCid[wantHaves[i-1]].Priority {
				t.Fatal("earliest want-haves should have higher priority")
			}
		}
	}

	// Check that earliest want-blocks have highest priority
	for i := range wantBlocks {
		if i > 0 {
			if byCid[wantBlocks[i]].Priority > byCid[wantBlocks[i-1]].Priority {
				t.Fatal("earliest want-blocks should have higher priority")
			}
		}
	}

	// Check that want-haves have higher priority than want-blocks within
	// same group
	for i := range wantHaves1 {
		if i > 0 {
			if byCid[wantHaves[i]].Priority <= byCid[wantBlocks[0]].Priority {
				t.Fatal("want-haves should have higher priority than want-blocks")
			}
		}
	}

	// Check that all items in first group have higher priority than first item
	// in second group
	for i := range wantHaves1 {
		if i > 0 {
			if byCid[wantHaves[i]].Priority <= byCid[wantHaves2[0]].Priority {
				t.Fatal("items in first group should have higher priority than items in second group")
			}
		}
	}
}

func TestCancelOverridesPendingWants(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, true)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet, mockTimeoutCb)

	wantHaves := testutil.GenerateCids(2)
	wantBlocks := testutil.GenerateCids(2)
	cancels := []cid.Cid{wantBlocks[0], wantHaves[0]}

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks, wantHaves)
	messageQueue.AddCancels(cancels)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if totalEntriesLength(messages) != len(wantHaves)+len(wantBlocks)-len(cancels) {
		t.Fatal("Wrong message count")
	}

	// Cancelled 1 want-block and 1 want-have before they were sent
	// so that leaves 1 want-block and 1 want-have
	wb, wh, cl := filterWantTypes(messages[0])
	if len(wb) != 1 || !wb[0].Equals(wantBlocks[1]) {
		t.Fatal("Expected 1 want-block")
	}
	if len(wh) != 1 || !wh[0].Equals(wantHaves[1]) {
		t.Fatal("Expected 1 want-have")
	}
	// Cancelled wants before they were sent, so no cancel should be sent
	// to the network
	if len(cl) != 0 {
		t.Fatal("Expected no cancels")
	}

	// Cancel the remaining want-blocks and want-haves
	cancels = append(wantHaves, wantBlocks...)
	messageQueue.AddCancels(cancels)
	messages = collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	// The remaining 2 cancels should be sent to the network as they are for
	// wants that were sent to the network
	_, _, cl = filterWantTypes(messages[0])
	if len(cl) != 2 {
		t.Fatal("Expected 2 cancels")
	}
}

func TestWantOverridesPendingCancels(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, true)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet, mockTimeoutCb)

	cids := testutil.GenerateCids(3)
	wantBlocks := cids[:1]
	wantHaves := cids[1:]

	messageQueue.Startup()

	// Add 1 want-block and 2 want-haves
	messageQueue.AddWants(wantBlocks, wantHaves)

	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if totalEntriesLength(messages) != len(wantBlocks)+len(wantHaves) {
		t.Fatal("Wrong message count", totalEntriesLength(messages))
	}

	// Cancel existing wants
	messageQueue.AddCancels(cids)
	// Override one cancel with a want-block (before cancel is sent to network)
	messageQueue.AddWants(cids[:1], []cid.Cid{})

	messages = collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if totalEntriesLength(messages) != 3 {
		t.Fatal("Wrong message count", totalEntriesLength(messages))
	}

	// Should send 1 want-block and 2 cancels
	wb, wh, cl := filterWantTypes(messages[0])
	if len(wb) != 1 {
		t.Fatal("Expected 1 want-block")
	}
	if len(wh) != 0 {
		t.Fatal("Expected 0 want-have")
	}
	if len(cl) != 2 {
		t.Fatal("Expected 2 cancels")
	}
}

func TestWantlistRebroadcast(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, true)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet, mockTimeoutCb)
	bcstwh := testutil.GenerateCids(10)
	wantHaves := testutil.GenerateCids(10)
	wantBlocks := testutil.GenerateCids(10)

	// Add some broadcast want-haves
	messageQueue.Startup()
	messageQueue.AddBroadcastWantHaves(bcstwh)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were sent for initial wants")
	}

	// All broadcast want-haves should have been sent
	firstMessage := messages[0]
	if len(firstMessage) != len(bcstwh) {
		t.Fatal("wrong number of wants")
	}

	// Tell message queue to rebroadcast after 5ms, then wait 8ms
	messageQueue.SetRebroadcastInterval(5 * time.Millisecond)
	messages = collectMessages(ctx, t, messagesSent, 8*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were rebroadcast")
	}

	// All the want-haves should have been rebroadcast
	firstMessage = messages[0]
	if len(firstMessage) != len(bcstwh) {
		t.Fatal("did not rebroadcast all wants")
	}

	// Tell message queue to rebroadcast after a long time (so it doesn't
	// interfere with the next message collection), then send out some
	// regular wants and collect them
	messageQueue.SetRebroadcastInterval(1 * time.Second)
	messageQueue.AddWants(wantBlocks, wantHaves)
	messages = collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were rebroadcast")
	}

	// All new wants should have been sent
	firstMessage = messages[0]
	if len(firstMessage) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("wrong number of wants")
	}

	// Tell message queue to rebroadcast after 10ms, then wait 15ms
	messageQueue.SetRebroadcastInterval(10 * time.Millisecond)
	messages = collectMessages(ctx, t, messagesSent, 15*time.Millisecond)
	firstMessage = messages[0]

	// Both original and new wants should have been rebroadcast
	totalWants := len(bcstwh) + len(wantHaves) + len(wantBlocks)
	if len(firstMessage) != totalWants {
		t.Fatal("did not rebroadcast all wants")
	}

	// Cancel some of the wants
	messageQueue.SetRebroadcastInterval(1 * time.Second)
	cancels := append([]cid.Cid{bcstwh[0]}, wantHaves[0], wantBlocks[0])
	messageQueue.AddCancels(cancels)
	messages = collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were rebroadcast")
	}

	// Cancels for each want should have been sent
	firstMessage = messages[0]
	if len(firstMessage) != len(cancels) {
		t.Fatal("wrong number of cancels")
	}
	for _, entry := range firstMessage {
		if !entry.Cancel {
			t.Fatal("expected cancels")
		}
	}

	// Tell message queue to rebroadcast after 10ms, then wait 15ms
	messageQueue.SetRebroadcastInterval(10 * time.Millisecond)
	messages = collectMessages(ctx, t, messagesSent, 15*time.Millisecond)
	firstMessage = messages[0]
	if len(firstMessage) != totalWants-len(cancels) {
		t.Fatal("did not rebroadcast all wants")
	}
}

func TestSendingLargeMessages(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, true)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	dhtm := &fakeDontHaveTimeoutMgr{}
	peerID := testutil.GeneratePeers(1)[0]

	wantBlocks := testutil.GenerateCids(10)
	entrySize := 44
	maxMsgSize := entrySize * 3 // 3 wants
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMsgSize, sendErrorBackoff, dhtm)

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks, []cid.Cid{})
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	// want-block has size 44, so with maxMsgSize 44 * 3 (3 want-blocks), then if
	// we send 10 want-blocks we should expect 4 messages:
	// [***] [***] [***] [*]
	if len(messages) != 4 {
		t.Fatal("expected 4 messages to be sent, got", len(messages))
	}
	if totalEntriesLength(messages) != len(wantBlocks) {
		t.Fatal("wrong number of wants")
	}
}

func TestSendToPeerThatDoesntSupportHave(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, false)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]

	messageQueue := New(ctx, peerID, fakenet, mockTimeoutCb)
	messageQueue.Startup()

	// If the remote peer doesn't support HAVE / DONT_HAVE messages
	// - want-blocks should be sent normally
	// - want-haves should not be sent
	// - broadcast want-haves should be sent as want-blocks

	// Check broadcast want-haves
	bcwh := testutil.GenerateCids(10)
	messageQueue.AddBroadcastWantHaves(bcwh)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if len(messages) != 1 {
		t.Fatal("wrong number of messages were sent", len(messages))
	}
	wl := messages[0]
	if len(wl) != len(bcwh) {
		t.Fatal("wrong number of entries in wantlist", len(wl))
	}
	for _, entry := range wl {
		if entry.WantType != pb.Message_Wantlist_Block {
			t.Fatal("broadcast want-haves should be sent as want-blocks")
		}
	}

	// Check regular want-haves and want-blocks
	wbs := testutil.GenerateCids(10)
	whs := testutil.GenerateCids(10)
	messageQueue.AddWants(wbs, whs)
	messages = collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if len(messages) != 1 {
		t.Fatal("wrong number of messages were sent", len(messages))
	}
	wl = messages[0]
	if len(wl) != len(wbs) {
		t.Fatal("should only send want-blocks (no want-haves)", len(wl))
	}
	for _, entry := range wl {
		if entry.WantType != pb.Message_Wantlist_Block {
			t.Fatal("should only send want-blocks")
		}
	}
}

func TestSendToPeerThatDoesntSupportHaveMonitorsTimeouts(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, false)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]

	dhtm := &fakeDontHaveTimeoutMgr{}
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrorBackoff, dhtm)
	messageQueue.Startup()

	wbs := testutil.GenerateCids(10)
	messageQueue.AddWants(wbs, nil)
	collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	// Check want-blocks are added to DontHaveTimeoutMgr
	if dhtm.pendingCount() != len(wbs) {
		t.Fatal("want-blocks not added to DontHaveTimeoutMgr")
	}

	cancelCount := 2
	messageQueue.AddCancels(wbs[:cancelCount])
	collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	// Check want-blocks are removed from DontHaveTimeoutMgr
	if dhtm.pendingCount() != len(wbs)-cancelCount {
		t.Fatal("want-blocks not removed from DontHaveTimeoutMgr")
	}
}

func filterWantTypes(wantlist []bsmsg.Entry) ([]cid.Cid, []cid.Cid, []cid.Cid) {
	var wbs []cid.Cid
	var whs []cid.Cid
	var cls []cid.Cid
	for _, e := range wantlist {
		if e.Cancel {
			cls = append(cls, e.Cid)
		} else if e.WantType == pb.Message_Wantlist_Block {
			wbs = append(wbs, e.Cid)
		} else {
			whs = append(whs, e.Cid)
		}
	}
	return wbs, whs, cls
}

// Simplistic benchmark to allow us to simulate conditions on the gateways
func BenchmarkMessageQueue(b *testing.B) {
	ctx := context.Background()

	createQueue := func() *MessageQueue {
		messagesSent := make(chan []bsmsg.Entry)
		resetChan := make(chan struct{}, 1)
		fakeSender := newFakeMessageSender(resetChan, messagesSent, true)
		fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
		dhtm := &fakeDontHaveTimeoutMgr{}
		peerID := testutil.GeneratePeers(1)[0]

		messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrorBackoff, dhtm)
		messageQueue.Startup()

		go func() {
			for {
				<-messagesSent
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
			}
		}()

		return messageQueue
	}

	// Create a handful of message queues to start with
	var qs []*MessageQueue
	for i := 0; i < 5; i++ {
		qs = append(qs, createQueue())
	}

	for n := 0; n < b.N; n++ {
		// Create a new message queue every 10 ticks
		if n%10 == 0 {
			qs = append(qs, createQueue())
		}

		// Pick a random message queue, favoring those created later
		qn := len(qs)
		i := int(math.Floor(float64(qn) * float64(1-rand.Float32()*rand.Float32())))
		if i >= qn { // because of floating point math
			i = qn - 1
		}

		// Alternately add either a few wants or a lot of broadcast wants
		if rand.Intn(2) == 0 {
			wants := testutil.GenerateCids(10)
			qs[i].AddWants(wants[:2], wants[2:])
		} else {
			wants := testutil.GenerateCids(60)
			qs[i].AddBroadcastWantHaves(wants)
		}
	}
}
