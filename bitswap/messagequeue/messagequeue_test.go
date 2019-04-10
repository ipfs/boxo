package messagequeue

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"

	bsmsg "github.com/ipfs/go-bitswap/message"
	bsnet "github.com/ipfs/go-bitswap/network"
	peer "github.com/libp2p/go-libp2p-peer"
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

type fakeMessageSender struct {
	sendError    error
	fullClosed   chan<- struct{}
	reset        chan<- struct{}
	messagesSent chan<- bsmsg.BitSwapMessage
}

func (fms *fakeMessageSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	fms.messagesSent <- msg
	return fms.sendError
}
func (fms *fakeMessageSender) Close() error { fms.fullClosed <- struct{}{}; return nil }
func (fms *fakeMessageSender) Reset() error { fms.reset <- struct{}{}; return nil }

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
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet)
	ses := testutil.GenerateSessionID()
	wl := testutil.GenerateWantlist(10, ses)

	messageQueue.Startup()
	messageQueue.AddWantlist(wl)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were sent for initial wants")
	}

	firstMessage := messages[0]
	if len(firstMessage.Wantlist()) != wl.Len() {
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
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet)
	ses1 := testutil.GenerateSessionID()
	ses2 := testutil.GenerateSessionID()
	entries := testutil.GenerateMessageEntries(10, false)
	messageQueue.Startup()

	messageQueue.AddMessage(entries, ses1)
	messageQueue.AddMessage(entries, ses2)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)

	if totalEntriesLength(messages) != len(entries) {
		t.Fatal("Messages were not deduped")
	}
}

func TestSendingMessagesPartialDupe(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet)
	ses1 := testutil.GenerateSessionID()
	ses2 := testutil.GenerateSessionID()
	entries := testutil.GenerateMessageEntries(10, false)
	moreEntries := testutil.GenerateMessageEntries(5, false)
	secondEntries := append(entries[5:], moreEntries...)
	messageQueue.Startup()

	messageQueue.AddMessage(entries, ses1)
	messageQueue.AddMessage(secondEntries, ses2)
	messages := collectMessages(ctx, t, messagesSent, 20*time.Millisecond)

	if totalEntriesLength(messages) != len(entries)+len(moreEntries) {
		t.Fatal("messages were not correctly deduped")
	}

}

func TestWantlistRebroadcast(t *testing.T) {

	ctx := context.Background()
	messagesSent := make(chan bsmsg.BitSwapMessage)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	fakeSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	fakenet := &fakeMessageNetwork{nil, nil, fakeSender}
	peerID := testutil.GeneratePeers(1)[0]
	messageQueue := New(ctx, peerID, fakenet)
	ses := testutil.GenerateSessionID()
	wl := testutil.GenerateWantlist(10, ses)

	messageQueue.Startup()
	messageQueue.AddWantlist(wl)
	messages := collectMessages(ctx, t, messagesSent, 10*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were sent for initial wants")
	}

	messageQueue.SetRebroadcastInterval(5 * time.Millisecond)
	messages = collectMessages(ctx, t, messagesSent, 8*time.Millisecond)
	if len(messages) != 1 {
		t.Fatal("wrong number of messages were rebroadcast")
	}

	firstMessage := messages[0]
	if len(firstMessage.Wantlist()) != wl.Len() {
		t.Fatal("did not add all wants to want list")
	}
	for _, entry := range firstMessage.Wantlist() {
		if entry.Cancel {
			t.Fatal("initial add sent cancel entry when it should not have")
		}
	}
}
