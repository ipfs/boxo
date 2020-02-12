package messagequeue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"
	"github.com/ipfs/go-bitswap/message"
	cid "github.com/ipfs/go-cid"

	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
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

func (fmn *fakeMessageNetwork) NewMessageSender(context.Context, peer.ID) (bsnet.MessageSender, error) {
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
	ks []cid.Cid
}

func (fp *fakeDontHaveTimeoutMgr) Start()    {}
func (fp *fakeDontHaveTimeoutMgr) Shutdown() {}
func (fp *fakeDontHaveTimeoutMgr) AddPending(ks []cid.Cid) {
	s := cid.NewSet()
	for _, c := range append(fp.ks, ks...) {
		s.Add(c)
	}
	fp.ks = s.Keys()
}
func (fp *fakeDontHaveTimeoutMgr) CancelPending(ks []cid.Cid) {
	s := cid.NewSet()
	for _, c := range fp.ks {
		s.Add(c)
	}
	for _, c := range ks {
		s.Remove(c)
	}
	fp.ks = s.Keys()
}

type fakeMessageSender struct {
	sendError    error
	fullClosed   chan<- struct{}
	reset        chan<- struct{}
	messagesSent chan<- bsmsg.BitSwapMessage
	sendErrors   chan<- error
	supportsHave bool
}

func (fms *fakeMessageSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	if fms.sendError != nil {
		fms.sendErrors <- fms.sendError
		return fms.sendError
	}
	fms.messagesSent <- msg
	return nil
}
func (fms *fakeMessageSender) Close() error       { fms.fullClosed <- struct{}{}; return nil }
func (fms *fakeMessageSender) Reset() error       { fms.reset <- struct{}{}; return nil }
func (fms *fakeMessageSender) SupportsHave() bool { return fms.supportsHave }

func mockTimeoutCb(peer.ID, []cid.Cid) {}

func collectMessages(ctx context.Context,
	t *testing.T,
	messagesSent <-chan bsmsg.BitSwapMessage,
	timeout time.Duration) []bsmsg.BitSwapMessage {
	var messagesReceived []bsmsg.BitSwapMessage
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

func totalEntriesLength(messages []bsmsg.BitSwapMessage) int {
	totalLength := 0
	for _, messages := range messages {
		totalLength += len(messages.Wantlist())
	}
	return totalLength
}

func TestStartupAndShutdown(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, true}
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
	if len(firstMessage.Wantlist()) != len(bcstwh) {
		t.Fatal("did not add all wants to want list")
	}
	for _, entry := range firstMessage.Wantlist() {
		if entry.Cancel {
			t.Fatal("initial add sent cancel entry when it should not have")
		}
	}

	messageQueue.Shutdown()

	timeoutctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	select {
	case <-fullClosedChan:
	case <-resetChan:
		t.Fatal("message sender should have been closed but was reset")
	case <-timeoutctx.Done():
		t.Fatal("message sender should have been closed but wasn't")
	}
}

func TestSendingMessagesDeduped(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, true}
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
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, true}
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
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, true}
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
	for _, entry := range messages[0].Wantlist() {
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
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, true}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet, mockTimeoutCb)
	wantHaves := testutil.GenerateCids(2)
	wantBlocks := testutil.GenerateCids(2)

	messageQueue.Startup()
	messageQueue.AddWants(wantBlocks, wantHaves)
	messageQueue.AddCancels([]cid.Cid{wantBlocks[0], wantHaves[0]})
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if totalEntriesLength(messages) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("Wrong message count")
	}

	wb, wh, cl := filterWantTypes(messages[0].Wantlist())
	if len(wb) != 1 || !wb[0].Equals(wantBlocks[1]) {
		t.Fatal("Expected 1 want-block")
	}
	if len(wh) != 1 || !wh[0].Equals(wantHaves[1]) {
		t.Fatal("Expected 1 want-have")
	}
	if len(cl) != 2 {
		t.Fatal("Expected 2 cancels")
	}
}

func TestWantOverridesPendingCancels(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, true}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet, mockTimeoutCb)
	cancels := testutil.GenerateCids(3)

	messageQueue.Startup()
	messageQueue.AddCancels(cancels)
	messageQueue.AddWants([]cid.Cid{cancels[0]}, []cid.Cid{cancels[1]})
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if totalEntriesLength(messages) != len(cancels) {
		t.Fatal("Wrong message count")
	}

	wb, wh, cl := filterWantTypes(messages[0].Wantlist())
	if len(wb) != 1 || !wb[0].Equals(cancels[0]) {
		t.Fatal("Expected 1 want-block")
	}
	if len(wh) != 1 || !wh[0].Equals(cancels[1]) {
		t.Fatal("Expected 1 want-have")
	}
	if len(cl) != 1 || !cl[0].Equals(cancels[2]) {
		t.Fatal("Expected 1 cancel")
	}
}

func TestWantlistRebroadcast(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, true}
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
	if len(firstMessage.Wantlist()) != len(bcstwh) {
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
	if len(firstMessage.Wantlist()) != len(bcstwh) {
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
	if len(firstMessage.Wantlist()) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("wrong number of wants")
	}

	// Tell message queue to rebroadcast after 5ms, then wait 8ms
	messageQueue.SetRebroadcastInterval(5 * time.Millisecond)
	messages = collectMessages(ctx, t, messagesSent, 8*time.Millisecond)
	firstMessage = messages[0]

	// Both original and new wants should have been rebroadcast
	totalWants := len(bcstwh) + len(wantHaves) + len(wantBlocks)
	if len(firstMessage.Wantlist()) != totalWants {
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
	if len(firstMessage.Wantlist()) != len(cancels) {
		t.Fatal("wrong number of cancels")
	}
	for _, entry := range firstMessage.Wantlist() {
		if !entry.Cancel {
			t.Fatal("expected cancels")
		}
	}

	// Tell message queue to rebroadcast after 5ms, then wait 8ms
	messageQueue.SetRebroadcastInterval(5 * time.Millisecond)
	messages = collectMessages(ctx, t, messagesSent, 8*time.Millisecond)
	firstMessage = messages[0]
	if len(firstMessage.Wantlist()) != totalWants-len(cancels) {
		t.Fatal("did not rebroadcast all wants")
	}
}

func TestSendingLargeMessages(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, true}
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
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, false}
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
	wl := messages[0].Wantlist()
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
	wl = messages[0].Wantlist()
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
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, false}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]

	dhtm := &fakeDontHaveTimeoutMgr{}
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrorBackoff, dhtm)
	messageQueue.Startup()

	wbs := testutil.GenerateCids(10)
	messageQueue.AddWants(wbs, nil)
	collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	// Check want-blocks are added to DontHaveTimeoutMgr
	if len(dhtm.ks) != len(wbs) {
		t.Fatal("want-blocks not added to DontHaveTimeoutMgr")
	}

	cancelCount := 2
	messageQueue.AddCancels(wbs[:cancelCount])
	collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	// Check want-blocks are removed from DontHaveTimeoutMgr
	if len(dhtm.ks) != len(wbs)-cancelCount {
		t.Fatal("want-blocks not removed from DontHaveTimeoutMgr")
	}
}

func TestResendAfterError(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, true}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	dhtm := &fakeDontHaveTimeoutMgr{}
	peerID := testutil.GeneratePeers(1)[0]
	sendErrBackoff := 5 * time.Millisecond
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrBackoff, dhtm)
	wantBlocks := testutil.GenerateCids(10)
	wantHaves := testutil.GenerateCids(10)

	messageQueue.Startup()

	var errs []error
	go func() {
		// After the first error is received, clear sendError so that
		// subsequent sends will not error
		errs = append(errs, <-sendErrors)
		fakeSender.sendError = nil
	}()

	// Make the first send error out
	fakeSender.sendError = errors.New("send err")
	messageQueue.AddWants(wantBlocks, wantHaves)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if len(errs) != 1 {
		t.Fatal("Expected first send to error")
	}

	if totalEntriesLength(messages) != len(wantHaves)+len(wantBlocks) {
		t.Fatal("Expected subsequent send to succeed")
	}
}

func TestResendAfterMaxRetries(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	sendErrors := make(chan error)
	resetChan := make(chan struct{}, maxRetries*2)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent, sendErrors, true}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	dhtm := &fakeDontHaveTimeoutMgr{}
	peerID := testutil.GeneratePeers(1)[0]
	sendErrBackoff := 2 * time.Millisecond
	messageQueue := newMessageQueue(ctx, peerID, fakenet, maxMessageSize, sendErrBackoff, dhtm)
	wantBlocks := testutil.GenerateCids(10)
	wantHaves := testutil.GenerateCids(10)
	wantBlocks2 := testutil.GenerateCids(10)
	wantHaves2 := testutil.GenerateCids(10)

	messageQueue.Startup()

	var errs []error
	go func() {
		for len(errs) < maxRetries {
			err := <-sendErrors
			errs = append(errs, err)
		}
	}()

	// Make the first group of send attempts error out
	fakeSender.sendError = errors.New("send err")
	messageQueue.AddWants(wantBlocks, wantHaves)
	messages := collectMessages(ctx, t, messagesSent, 50*time.Millisecond)

	if len(errs) != maxRetries {
		t.Fatal("Expected maxRetries errors, got", len(errs))
	}

	// No successful send after max retries, so expect no messages sent
	if totalEntriesLength(messages) != 0 {
		t.Fatal("Expected no messages")
	}

	// Clear sendError so that subsequent sends will not error
	fakeSender.sendError = nil

	// Add a new batch of wants
	messageQueue.AddWants(wantBlocks2, wantHaves2)
	messages = collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	// All wants from previous and new send should be sent
	if totalEntriesLength(messages) != len(wantHaves)+len(wantBlocks)+len(wantHaves2)+len(wantBlocks2) {
		t.Fatal("Expected subsequent send to send first and second batches of wants")
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
