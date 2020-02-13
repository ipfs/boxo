package messagequeue

import (
	"context"
	"math"
	"sync"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	bsnet "github.com/ipfs/go-bitswap/network"
	bswl "github.com/ipfs/go-bitswap/wantlist"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

var log = logging.Logger("bitswap")

const (
	defaultRebroadcastInterval = 30 * time.Second
	// maxRetries is the number of times to attempt to send a message before
	// giving up
	maxRetries = 10
	// maxMessageSize is the maximum message size in bytes
	maxMessageSize = 1024 * 1024 * 2
	// sendErrorBackoff is the time to wait before retrying to connect after
	// an error when trying to send a message
	sendErrorBackoff = 100 * time.Millisecond
	// maxPriority is the max priority as defined by the bitswap protocol
	maxPriority = math.MaxInt32
	// sendMessageDebounce is the debounce duration when calling sendMessage()
	sendMessageDebounce = time.Millisecond
	// when we reach sendMessageCutoff wants/cancels, we'll send the message immediately.
	sendMessageCutoff = 256
	// when we debounce for more than sendMessageMaxDelay, we'll send the
	// message immediately.
	sendMessageMaxDelay = 20 * time.Millisecond
)

// MessageNetwork is any network that can connect peers and generate a message
// sender.
type MessageNetwork interface {
	ConnectTo(context.Context, peer.ID) error
	NewMessageSender(context.Context, peer.ID) (bsnet.MessageSender, error)
	Latency(peer.ID) time.Duration
	Ping(context.Context, peer.ID) ping.Result
}

// MessageQueue implements queue of want messages to send to peers.
type MessageQueue struct {
	ctx              context.Context
	p                peer.ID
	network          MessageNetwork
	dhTimeoutMgr     DontHaveTimeoutManager
	maxMessageSize   int
	sendErrorBackoff time.Duration

	outgoingWork chan time.Time
	done         chan struct{}

	// Take lock whenever any of these variables are modified
	wllock    sync.Mutex
	bcstWants recallWantlist
	peerWants recallWantlist
	cancels   *cid.Set
	priority  int

	// Dont touch any of these variables outside of run loop
	sender                bsnet.MessageSender
	rebroadcastIntervalLk sync.RWMutex
	rebroadcastInterval   time.Duration
	rebroadcastTimer      *time.Timer
}

// recallWantlist keeps a list of pending wants, and a list of all wants that
// have ever been requested
type recallWantlist struct {
	// The list of all wants that have been requested, including wants that
	// have been sent and wants that have not yet been sent
	allWants *bswl.Wantlist
	// The list of wants that have not yet been sent
	pending *bswl.Wantlist
}

func newRecallWantList() recallWantlist {
	return recallWantlist{
		allWants: bswl.New(),
		pending:  bswl.New(),
	}
}

// Add want to both the pending list and the list of all wants
func (r *recallWantlist) Add(c cid.Cid, priority int, wtype pb.Message_Wantlist_WantType) {
	r.allWants.Add(c, priority, wtype)
	r.pending.Add(c, priority, wtype)
}

// Remove wants from both the pending list and the list of all wants
func (r *recallWantlist) Remove(c cid.Cid) {
	r.allWants.Remove(c)
	r.pending.Remove(c)
}

// Remove wants by type from both the pending list and the list of all wants
func (r *recallWantlist) RemoveType(c cid.Cid, wtype pb.Message_Wantlist_WantType) {
	r.allWants.RemoveType(c, wtype)
	r.pending.RemoveType(c, wtype)
}

type peerConn struct {
	p       peer.ID
	network MessageNetwork
}

func newPeerConnection(p peer.ID, network MessageNetwork) *peerConn {
	return &peerConn{p, network}
}

func (pc *peerConn) Ping(ctx context.Context) ping.Result {
	return pc.network.Ping(ctx, pc.p)
}

func (pc *peerConn) Latency() time.Duration {
	return pc.network.Latency(pc.p)
}

// Fires when a timeout occurs waiting for a response from a peer running an
// older version of Bitswap that doesn't support DONT_HAVE messages.
type OnDontHaveTimeout func(peer.ID, []cid.Cid)

// DontHaveTimeoutManager pings a peer to estimate latency so it can set a reasonable
// upper bound on when to consider a DONT_HAVE request as timed out (when connected to
// a peer that doesn't support DONT_HAVE messages)
type DontHaveTimeoutManager interface {
	// Start the manager (idempotent)
	Start()
	// Shutdown the manager (Shutdown is final, manager cannot be restarted)
	Shutdown()
	// AddPending adds the wants as pending a response. If the are not
	// cancelled before the timeout, the OnDontHaveTimeout method will be called.
	AddPending([]cid.Cid)
	// CancelPending removes the wants
	CancelPending([]cid.Cid)
}

// New creates a new MessageQueue.
func New(ctx context.Context, p peer.ID, network MessageNetwork, onDontHaveTimeout OnDontHaveTimeout) *MessageQueue {
	onTimeout := func(ks []cid.Cid) {
		onDontHaveTimeout(p, ks)
	}
	dhTimeoutMgr := newDontHaveTimeoutMgr(ctx, newPeerConnection(p, network), onTimeout)
	return newMessageQueue(ctx, p, network, maxMessageSize, sendErrorBackoff, dhTimeoutMgr)
}

// This constructor is used by the tests
func newMessageQueue(ctx context.Context, p peer.ID, network MessageNetwork,
	maxMsgSize int, sendErrorBackoff time.Duration, dhTimeoutMgr DontHaveTimeoutManager) *MessageQueue {

	mq := &MessageQueue{
		ctx:                 ctx,
		p:                   p,
		network:             network,
		dhTimeoutMgr:        dhTimeoutMgr,
		maxMessageSize:      maxMsgSize,
		bcstWants:           newRecallWantList(),
		peerWants:           newRecallWantList(),
		cancels:             cid.NewSet(),
		outgoingWork:        make(chan time.Time, 1),
		done:                make(chan struct{}),
		rebroadcastInterval: defaultRebroadcastInterval,
		sendErrorBackoff:    sendErrorBackoff,
		priority:            maxPriority,
	}

	return mq
}

// Add want-haves that are part of a broadcast to all connected peers
func (mq *MessageQueue) AddBroadcastWantHaves(wantHaves []cid.Cid) {
	if len(wantHaves) == 0 {
		return
	}

	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	for _, c := range wantHaves {
		mq.bcstWants.Add(c, mq.priority, pb.Message_Wantlist_Have)
		mq.priority--

		// We're adding a want-have for the cid, so clear any pending cancel
		// for the cid
		mq.cancels.Remove(c)
	}

	// Schedule a message send
	mq.signalWorkReady()
}

// Add want-haves and want-blocks for the peer for this message queue.
func (mq *MessageQueue) AddWants(wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	if len(wantBlocks) == 0 && len(wantHaves) == 0 {
		return
	}

	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	for _, c := range wantHaves {
		mq.peerWants.Add(c, mq.priority, pb.Message_Wantlist_Have)
		mq.priority--

		// We're adding a want-have for the cid, so clear any pending cancel
		// for the cid
		mq.cancels.Remove(c)
	}
	for _, c := range wantBlocks {
		mq.peerWants.Add(c, mq.priority, pb.Message_Wantlist_Block)
		mq.priority--

		// We're adding a want-block for the cid, so clear any pending cancel
		// for the cid
		mq.cancels.Remove(c)
	}

	// Schedule a message send
	mq.signalWorkReady()
}

// Add cancel messages for the given keys.
func (mq *MessageQueue) AddCancels(cancelKs []cid.Cid) {
	if len(cancelKs) == 0 {
		return
	}

	// Cancel any outstanding DONT_HAVE timers
	mq.dhTimeoutMgr.CancelPending(cancelKs)

	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	// Remove keys from broadcast and peer wants, and add to cancels
	for _, c := range cancelKs {
		mq.bcstWants.Remove(c)
		mq.peerWants.Remove(c)
		mq.cancels.Add(c)
	}

	// Schedule a message send
	mq.signalWorkReady()
}

// SetRebroadcastInterval sets a new interval on which to rebroadcast the full wantlist
func (mq *MessageQueue) SetRebroadcastInterval(delay time.Duration) {
	mq.rebroadcastIntervalLk.Lock()
	mq.rebroadcastInterval = delay
	if mq.rebroadcastTimer != nil {
		mq.rebroadcastTimer.Reset(delay)
	}
	mq.rebroadcastIntervalLk.Unlock()
}

// Startup starts the processing of messages and rebroadcasting.
func (mq *MessageQueue) Startup() {
	mq.rebroadcastIntervalLk.RLock()
	mq.rebroadcastTimer = time.NewTimer(mq.rebroadcastInterval)
	mq.rebroadcastIntervalLk.RUnlock()
	go mq.runQueue()
}

// Shutdown stops the processing of messages for a message queue.
func (mq *MessageQueue) Shutdown() {
	close(mq.done)
}

func (mq *MessageQueue) onShutdown() {
	// Shut down the DONT_HAVE timeout manager
	mq.dhTimeoutMgr.Shutdown()
}

func (mq *MessageQueue) runQueue() {
	defer mq.onShutdown()

	// Create a timer for debouncing scheduled work.
	scheduleWork := time.NewTimer(0)
	if !scheduleWork.Stop() {
		// Need to drain the timer if Stop() returns false
		// See: https://golang.org/pkg/time/#Timer.Stop
		<-scheduleWork.C
	}

	var workScheduled time.Time
	for {
		select {
		case <-mq.rebroadcastTimer.C:
			mq.rebroadcastWantlist()
		case when := <-mq.outgoingWork:
			// If we have work scheduled, cancel the timer. If we
			// don't, record when the work was scheduled.
			// We send the time on the channel so we accurately
			// track delay.
			if workScheduled.IsZero() {
				workScheduled = when
			} else if !scheduleWork.Stop() {
				// Need to drain the timer if Stop() returns false
				<-scheduleWork.C
			}

			// If we have too many updates and/or we've waited too
			// long, send immediately.
			if mq.pendingWorkCount() > sendMessageCutoff ||
				time.Since(workScheduled) >= sendMessageMaxDelay {
				mq.sendIfReady()
				workScheduled = time.Time{}
			} else {
				// Otherwise, extend the timer.
				scheduleWork.Reset(sendMessageDebounce)
			}
		case <-scheduleWork.C:
			// We have work scheduled and haven't seen any updates
			// in sendMessageDebounce. Send immediately.
			workScheduled = time.Time{}
			mq.sendIfReady()
		case <-mq.done:
			if mq.sender != nil {
				mq.sender.Close()
			}
			return
		case <-mq.ctx.Done():
			if mq.sender != nil {
				_ = mq.sender.Reset()
			}
			return
		}
	}
}

// Periodically resend the list of wants to the peer
func (mq *MessageQueue) rebroadcastWantlist() {
	mq.rebroadcastIntervalLk.RLock()
	mq.rebroadcastTimer.Reset(mq.rebroadcastInterval)
	mq.rebroadcastIntervalLk.RUnlock()

	// If some wants were transferred from the rebroadcast list
	if mq.transferRebroadcastWants() {
		// Send them out
		mq.sendMessage()
	}
}

// Transfer wants from the rebroadcast lists into the pending lists.
func (mq *MessageQueue) transferRebroadcastWants() bool {
	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	// Check if there are any wants to rebroadcast
	if mq.bcstWants.allWants.Len() == 0 && mq.peerWants.allWants.Len() == 0 {
		return false
	}

	// Copy all wants into pending wants lists
	mq.bcstWants.pending.Absorb(mq.bcstWants.allWants)
	mq.peerWants.pending.Absorb(mq.peerWants.allWants)

	return true
}

func (mq *MessageQueue) signalWorkReady() {
	select {
	case mq.outgoingWork <- time.Now():
	default:
	}
}

func (mq *MessageQueue) sendIfReady() {
	if mq.hasPendingWork() {
		mq.sendMessage()
	}
}

func (mq *MessageQueue) sendMessage() {
	err := mq.initializeSender()
	if err != nil {
		log.Infof("cant open message sender to peer %s: %s", mq.p, err)
		// TODO: cant connect, what now?
		// TODO: should we stop using this connection and clear the want list
		// to avoid using up memory?
		return
	}

	// Make sure the DONT_HAVE timeout manager has started
	if !mq.sender.SupportsHave() {
		// Note: Start is idempotent
		mq.dhTimeoutMgr.Start()
	}

	// Convert want lists to a Bitswap Message
	message, onSent := mq.extractOutgoingMessage(mq.sender.SupportsHave())
	if message == nil || message.Empty() {
		return
	}

	// mq.logOutgoingMessage(message)

	// Try to send this message repeatedly
	for i := 0; i < maxRetries; i++ {
		if mq.attemptSendAndRecovery(message) {
			// We were able to send successfully.
			onSent()

			mq.simulateDontHaveWithTimeout(message)

			// If the message was too big and only a subset of wants could be
			// sent, schedule sending the rest of the wants in the next
			// iteration of the event loop.
			if mq.hasPendingWork() {
				mq.signalWorkReady()
			}

			return
		}
	}
}

// If the peer is running an older version of Bitswap that doesn't support the
// DONT_HAVE response, watch for timeouts on any want-blocks we sent the peer,
// and if there is a timeout simulate a DONT_HAVE response.
func (mq *MessageQueue) simulateDontHaveWithTimeout(msg bsmsg.BitSwapMessage) {
	// If the peer supports DONT_HAVE responses, we don't need to simulate
	if mq.sender.SupportsHave() {
		return
	}

	mq.wllock.Lock()

	// Get the CID of each want-block that expects a DONT_HAVE response
	wantlist := msg.Wantlist()
	wants := make([]cid.Cid, 0, len(wantlist))
	for _, entry := range wantlist {
		if entry.WantType == pb.Message_Wantlist_Block && entry.SendDontHave {
			// Unlikely, but just in case check that the block hasn't been
			// received in the interim
			c := entry.Cid
			if _, ok := mq.peerWants.allWants.Contains(c); ok {
				wants = append(wants, c)
			}
		}
	}

	mq.wllock.Unlock()

	// Add wants to DONT_HAVE timeout manager
	mq.dhTimeoutMgr.AddPending(wants)
}

// func (mq *MessageQueue) logOutgoingMessage(msg bsmsg.BitSwapMessage) {
// 	entries := msg.Wantlist()
// 	for _, e := range entries {
// 		if e.Cancel {
// 			if e.WantType == pb.Message_Wantlist_Have {
// 				log.Debugf("send %s->%s: cancel-have %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
// 			} else {
// 				log.Debugf("send %s->%s: cancel-block %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
// 			}
// 		} else {
// 			if e.WantType == pb.Message_Wantlist_Have {
// 				log.Debugf("send %s->%s: want-have %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
// 			} else {
// 				log.Debugf("send %s->%s: want-block %s\n", lu.P(mq.network.Self()), lu.P(mq.p), lu.C(e.Cid))
// 			}
// 		}
// 	}
// }

func (mq *MessageQueue) hasPendingWork() bool {
	return mq.pendingWorkCount() > 0
}

func (mq *MessageQueue) pendingWorkCount() int {
	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	return mq.bcstWants.pending.Len() + mq.peerWants.pending.Len() + mq.cancels.Len()
}

func (mq *MessageQueue) extractOutgoingMessage(supportsHave bool) (bsmsg.BitSwapMessage, func()) {
	// Create a new message
	msg := bsmsg.New(false)

	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	// Get broadcast and regular wantlist entries
	bcstEntries := mq.bcstWants.pending.SortedEntries()
	peerEntries := mq.peerWants.pending.SortedEntries()

	// Size of the message so far
	msgSize := 0

	// Add each broadcast want-have to the message
	for i := 0; i < len(bcstEntries) && msgSize < mq.maxMessageSize; i++ {
		// Broadcast wants are sent as want-have
		wantType := pb.Message_Wantlist_Have

		// If the remote peer doesn't support HAVE / DONT_HAVE messages,
		// send a want-block instead
		if !supportsHave {
			wantType = pb.Message_Wantlist_Block
		}

		e := bcstEntries[i]
		msgSize += msg.AddEntry(e.Cid, e.Priority, wantType, false)
	}

	// Add each regular want-have / want-block to the message
	for i := 0; i < len(peerEntries) && msgSize < mq.maxMessageSize; i++ {
		e := peerEntries[i]
		// If the remote peer doesn't support HAVE / DONT_HAVE messages,
		// don't send want-haves (only send want-blocks)
		if !supportsHave && e.WantType == pb.Message_Wantlist_Have {
			mq.peerWants.RemoveType(e.Cid, pb.Message_Wantlist_Have)
		} else {
			msgSize += msg.AddEntry(e.Cid, e.Priority, e.WantType, true)
		}
	}

	// Add each cancel to the message
	cancels := mq.cancels.Keys()
	for i := 0; i < len(cancels) && msgSize < mq.maxMessageSize; i++ {
		c := cancels[i]

		msgSize += msg.Cancel(c)

		// Clear the cancel - we make a best effort to let peers know about
		// cancels but won't save them to resend if there's a failure.
		mq.cancels.Remove(c)
	}

	// Called when the message has been successfully sent.
	// Remove the sent keys from the broadcast and regular wantlists.
	onSent := func() {
		mq.wllock.Lock()
		defer mq.wllock.Unlock()

		for _, e := range msg.Wantlist() {
			mq.bcstWants.pending.Remove(e.Cid)
			mq.peerWants.pending.RemoveType(e.Cid, e.WantType)
		}
	}

	return msg, onSent
}

func (mq *MessageQueue) initializeSender() error {
	if mq.sender != nil {
		return nil
	}
	nsender, err := openSender(mq.ctx, mq.network, mq.p)
	if err != nil {
		return err
	}
	mq.sender = nsender
	return nil
}

func (mq *MessageQueue) attemptSendAndRecovery(message bsmsg.BitSwapMessage) bool {
	err := mq.sender.SendMsg(mq.ctx, message)
	if err == nil {
		return true
	}

	log.Infof("bitswap send error: %s", err)
	_ = mq.sender.Reset()
	mq.sender = nil

	select {
	case <-mq.done:
		return true
	case <-mq.ctx.Done():
		return true
	case <-time.After(mq.sendErrorBackoff):
		// wait 100ms in case disconnect notifications are still propagating
		log.Warning("SendMsg errored but neither 'done' nor context.Done() were set")
	}

	err = mq.initializeSender()
	if err != nil {
		log.Infof("couldnt open sender again after SendMsg(%s) failed: %s", mq.p, err)
		return true
	}

	// TODO: Is this the same instance for the remote peer?
	// If its not, we should resend our entire wantlist to them
	/*
		if mq.sender.InstanceID() != mq.lastSeenInstanceID {
			wlm = mq.getFullWantlistMessage()
		}
	*/
	return false
}

func openSender(ctx context.Context, network MessageNetwork, p peer.ID) (bsnet.MessageSender, error) {
	// allow ten minutes for connections this includes looking them up in the
	// dht dialing them, and handshaking
	conctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()

	err := network.ConnectTo(conctx, p)
	if err != nil {
		return nil, err
	}

	nsender, err := network.NewMessageSender(ctx, p)
	if err != nil {
		return nil, err
	}

	return nsender, nil
}
