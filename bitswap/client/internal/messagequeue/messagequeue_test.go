package messagequeue

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-bitswap/internal/testutil"
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
	lk          sync.Mutex
	ks          []cid.Cid
	latencyUpds []time.Duration
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
func (fp *fakeDontHaveTimeoutMgr) UpdateMessageLatency(elapsed time.Duration) {
	fp.lk.Lock()
	defer fp.lk.Unlock()

	fp.latencyUpds = append(fp.latencyUpds, elapsed)
}
func (fp *fakeDontHaveTimeoutMgr) latencyUpdates() []time.Duration {
	fp.lk.Lock()
	defer fp.lk.Unlock()

	return fp.latencyUpds
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

func expectEvent(t *testing.T, events <-chan messageEvent, expectedEvent messageEvent) {
	evt := <-events
	if evt != expectedEvent {
		t.Fatal("message not queued")
	}
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
	byCid := make(map[cid.Cid]bsmsg.Entry)
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
	dhtm := &fakeDontHaveTimeoutMgr{}
	clock := clock.NewMock()
	events := make(chan messageEvent)
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrorBackoff, maxValidLatency, dhtm, clock, events)
	bcstwh := testutil.GenerateCids(10)
	wantHaves := testutil.GenerateCids(10)
	wantBlocks := testutil.GenerateCids(10)

	// Add some broadcast want-haves
	messageQueue.Startup()
	messageQueue.AddBroadcastWantHaves(bcstwh)
	expectEvent(t, events, messageQueued)
	clock.Add(sendMessageDebounce)
	message := <-messagesSent
	expectEvent(t, events, messageFinishedSending)

	// All broadcast want-haves should have been sent
	if len(message) != len(bcstwh) {
		t.Fatal("wrong number of wants")
	}

	// Tell message queue to rebroadcast after 5ms, then wait 8ms
	messageQueue.SetRebroadcastInterval(5 * time.Millisecond)
	clock.Add(8 * time.Millisecond)
	message = <-messagesSent
	expectEvent(t, events, messageFinishedSending)

	// All the want-haves should have been rebroadcast
	if len(message) != len(bcstwh) {
		t.Fatal("did not rebroadcast all wants")
	}

	// Tell message queue to rebroadcast after a long time (so it doesn't
	// interfere with the next message collection), then send out some
	// regular wants and collect them
	messageQueue.SetRebroadcastInterval(1 * time.Second)
	messageQueue.AddWants(wantBlocks, wantHaves)
	expectEvent(t, events, messageQueued)
	clock.Add(10 * time.Millisecond)
	message = <-messagesSent
	expectEvent(t, events, messageFinishedSending)

	// All new wants should have been sent
	if len(message) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("wrong number of wants")
	}

	select {
	case <-messagesSent:
		t.Fatal("should only be one message in queue")
	default:
	}

	// Tell message queue to rebroadcast after 10ms, then wait 15ms
	messageQueue.SetRebroadcastInterval(10 * time.Millisecond)
	clock.Add(15 * time.Millisecond)
	message = <-messagesSent
	expectEvent(t, events, messageFinishedSending)

	// Both original and new wants should have been rebroadcast
	totalWants := len(bcstwh) + len(wantHaves) + len(wantBlocks)
	if len(message) != totalWants {
		t.Fatal("did not rebroadcast all wants")
	}

	// Cancel some of the wants
	messageQueue.SetRebroadcastInterval(1 * time.Second)
	cancels := append([]cid.Cid{bcstwh[0]}, wantHaves[0], wantBlocks[0])
	messageQueue.AddCancels(cancels)
	expectEvent(t, events, messageQueued)
	clock.Add(10 * time.Millisecond)
	message = <-messagesSent
	expectEvent(t, events, messageFinishedSending)

	select {
	case <-messagesSent:
		t.Fatal("should only be one message in queue")
	default:
	}

	// Cancels for each want should have been sent
	if len(message) != len(cancels) {
		t.Fatal("wrong number of cancels")
	}
	for _, entry := range message {
		if !entry.Cancel {
			t.Fatal("expected cancels")
		}
	}

	// Tell message queue to rebroadcast after 10ms, then wait 15ms
	messageQueue.SetRebroadcastInterval(10 * time.Millisecond)
	clock.Add(15 * time.Millisecond)
	message = <-messagesSent
	expectEvent(t, events, messageFinishedSending)

	if len(message) != totalWants-len(cancels) {
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
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMsgSize, sendErrorBackoff, maxValidLatency, dhtm, clock.New(), nil)

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks, []cid.Cid{})
	messages := collectMessages(ctx, t, messagesSent, 100*time.Millisecond)

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
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrorBackoff, maxValidLatency, dhtm, clock.New(), nil)
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

func TestResponseReceived(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, false)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]

	dhtm := &fakeDontHaveTimeoutMgr{}
	clock := clock.NewMock()
	events := make(chan messageEvent)
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrorBackoff, maxValidLatency, dhtm, clock, events)
	messageQueue.Startup()

	cids := testutil.GenerateCids(10)

	// Add some wants
	messageQueue.AddWants(cids[:5], nil)
	expectEvent(t, events, messageQueued)
	clock.Add(sendMessageDebounce)
	<-messagesSent
	expectEvent(t, events, messageFinishedSending)

	// simulate 10 milliseconds passing
	clock.Add(10 * time.Millisecond)

	// Add some wants and wait another 10ms
	messageQueue.AddWants(cids[5:8], nil)
	expectEvent(t, events, messageQueued)
	clock.Add(10 * time.Millisecond)
	<-messagesSent
	expectEvent(t, events, messageFinishedSending)

	// Receive a response for some of the wants from both groups
	messageQueue.ResponseReceived([]cid.Cid{cids[0], cids[6], cids[9]})

	// Check that message queue informs DHTM of received responses
	expectEvent(t, events, latenciesRecorded)
	upds := dhtm.latencyUpdates()
	if len(upds) != 1 {
		t.Fatal("expected one latency update")
	}
	// Elapsed time should be between when the first want was sent and the
	// response received (about 20ms)
	if upds[0] != 20*time.Millisecond {
		t.Fatal("expected latency to be time since oldest message sent")
	}
}

func TestResponseReceivedAppliesForFirstResponseOnly(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, false)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]

	dhtm := &fakeDontHaveTimeoutMgr{}
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrorBackoff, maxValidLatency, dhtm, clock.New(), nil)
	messageQueue.Startup()

	cids := testutil.GenerateCids(2)

	// Add some wants and wait 10ms
	messageQueue.AddWants(cids, nil)
	collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	// Receive a response for the wants
	messageQueue.ResponseReceived(cids)

	// Wait another 10ms
	time.Sleep(10 * time.Millisecond)

	// Message queue should inform DHTM of first response
	upds := dhtm.latencyUpdates()
	if len(upds) != 1 {
		t.Fatal("expected one latency update")
	}

	// Receive a second response for the same wants
	messageQueue.ResponseReceived(cids)

	// Wait for the response to be processed by the message queue
	time.Sleep(10 * time.Millisecond)

	// Message queue should not inform DHTM of second response because the
	// CIDs are a subset of the first response
	upds = dhtm.latencyUpdates()
	if len(upds) != 1 {
		t.Fatal("expected one latency update")
	}
}

func TestResponseReceivedDiscardsOutliers(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan []bsmsg.Entry)
	resetChan := make(chan struct{}, 1)
	fakeSender := newFakeMessageSender(resetChan, messagesSent, false)
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]

	maxValLatency := 30 * time.Millisecond
	dhtm := &fakeDontHaveTimeoutMgr{}
	clock := clock.NewMock()
	events := make(chan messageEvent)
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrorBackoff, maxValLatency, dhtm, clock, events)
	messageQueue.Startup()

	cids := testutil.GenerateCids(4)

	// Add some wants and wait 20ms
	messageQueue.AddWants(cids[:2], nil)
	expectEvent(t, events, messageQueued)
	clock.Add(sendMessageDebounce)
	<-messagesSent
	expectEvent(t, events, messageFinishedSending)

	clock.Add(20 * time.Millisecond)

	// Add some more wants and wait long enough that the first wants will be
	// outside the maximum valid latency, but the second wants will be inside
	messageQueue.AddWants(cids[2:], nil)
	expectEvent(t, events, messageQueued)
	clock.Add(sendMessageDebounce)
	<-messagesSent
	expectEvent(t, events, messageFinishedSending)

	clock.Add(maxValLatency - 10*time.Millisecond + sendMessageDebounce)
	// Receive a response for the wants
	messageQueue.ResponseReceived(cids)

	// Check that the latency calculation excludes the first wants
	// (because they're older than max valid latency)
	expectEvent(t, events, latenciesRecorded)
	upds := dhtm.latencyUpdates()
	if len(upds) != 1 {
		t.Fatal("expected one latency update")
	}
	// Elapsed time should not include outliers
	if upds[0] > maxValLatency {
		t.Fatal("expected latency calculation to discard outliers")
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

		messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrorBackoff, maxValidLatency, dhtm, clock.New(), nil)
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
