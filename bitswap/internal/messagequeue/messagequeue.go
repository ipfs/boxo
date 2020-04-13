package messagequeue

import (
	"context"
	"math"
	"sync"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/wantlist"
	bswl "github.com/ipfs/go-bitswap/wantlist"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"go.uber.org/zap"
)

var log = logging.Logger("bitswap")
var sflog = log.Desugar()

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
	Self() peer.ID
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
	priority  int32

	// Dont touch any of these variables outside of run loop
	sender                bsnet.MessageSender
	rebroadcastIntervalLk sync.RWMutex
	rebroadcastInterval   time.Duration
	rebroadcastTimer      *time.Timer
	// For performance reasons we just clear out the fields of the message
	// instead of creating a new one every time.
	msg bsmsg.BitSwapMessage
}

// recallWantlist keeps a list of pending wants and a list of sent wants
type recallWantlist struct {
	// The list of wants that have not yet been sent
	pending *bswl.Wantlist
	// The list of wants that have been sent
	sent *bswl.Wantlist
}

func newRecallWantList() recallWantlist {
	return recallWantlist{
		pending: bswl.New(),
		sent:    bswl.New(),
	}
}

// Add want to the pending list
func (r *recallWantlist) Add(c cid.Cid, priority int32, wtype pb.Message_Wantlist_WantType) {
	r.pending.Add(c, priority, wtype)
}

// Remove wants from both the pending list and the list of sent wants
func (r *recallWantlist) Remove(c cid.Cid) {
	r.sent.Remove(c)
	r.pending.Remove(c)
}

// Remove wants by type from both the pending list and the list of sent wants
func (r *recallWantlist) RemoveType(c cid.Cid, wtype pb.Message_Wantlist_WantType) {
	r.sent.RemoveType(c, wtype)
	r.pending.RemoveType(c, wtype)
}

// MarkSent moves the want from the pending to the sent list
func (r *recallWantlist) MarkSent(e wantlist.Entry) {
	r.pending.RemoveType(e.Cid, e.WantType)
	r.sent.Add(e.Cid, e.Priority, e.WantType)
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
		log.Infow("Bitswap: timeout waiting for blocks", "cids", ks, "peer", p)
		onDontHaveTimeout(p, ks)
	}
	dhTimeoutMgr := newDontHaveTimeoutMgr(newPeerConnection(p, network), onTimeout)
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
		// For performance reasons we just clear out the fields of the message
		// after using it, instead of creating a new one every time.
		msg: bsmsg.New(false),
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

	workReady := false

	// Remove keys from broadcast and peer wants, and add to cancels
	for _, c := range cancelKs {
		// Check if a want for the key was sent
		_, wasSentBcst := mq.bcstWants.sent.Contains(c)
		_, wasSentPeer := mq.peerWants.sent.Contains(c)

		// Remove the want from tracking wantlists
		mq.bcstWants.Remove(c)
		mq.peerWants.Remove(c)

		// Only send a cancel if a want was sent
		if wasSentBcst || wasSentPeer {
			mq.cancels.Add(c)
			workReady = true
		}
	}

	// Schedule a message send
	if workReady {
		mq.signalWorkReady()
	}
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
	if mq.bcstWants.sent.Len() == 0 && mq.peerWants.sent.Len() == 0 {
		return false
	}

	// Copy sent wants into pending wants lists
	mq.bcstWants.pending.Absorb(mq.bcstWants.sent)
	mq.peerWants.pending.Absorb(mq.peerWants.sent)

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
	// Note: Start is idempotent
	mq.dhTimeoutMgr.Start()

	// Convert want lists to a Bitswap Message
	message, onSent := mq.extractOutgoingMessage(mq.sender.SupportsHave())

	// After processing the message, clear out its fields to save memory
	defer mq.msg.Reset(false)

	if message.Empty() {
		return
	}

	wantlist := message.Wantlist()
	mq.logOutgoingMessage(wantlist)

	// Try to send this message repeatedly
	for i := 0; i < maxRetries; i++ {
		if mq.attemptSendAndRecovery(message) {
			// We were able to send successfully.
			onSent(wantlist)

			mq.simulateDontHaveWithTimeout(wantlist)

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

// If want-block times out, simulate a DONT_HAVE reponse.
// This is necessary when making requests to peers running an older version of
// Bitswap that doesn't support the DONT_HAVE response, and is also useful to
// mitigate getting blocked by a peer that takes a long time to respond.
func (mq *MessageQueue) simulateDontHaveWithTimeout(wantlist []bsmsg.Entry) {
	// Get the CID of each want-block that expects a DONT_HAVE response
	wants := make([]cid.Cid, 0, len(wantlist))

	mq.wllock.Lock()

	for _, entry := range wantlist {
		if entry.WantType == pb.Message_Wantlist_Block && entry.SendDontHave {
			// Unlikely, but just in case check that the block hasn't been
			// received in the interim
			c := entry.Cid
			if _, ok := mq.peerWants.sent.Contains(c); ok {
				wants = append(wants, c)
			}
		}
	}

	mq.wllock.Unlock()

	// Add wants to DONT_HAVE timeout manager
	mq.dhTimeoutMgr.AddPending(wants)
}

func (mq *MessageQueue) logOutgoingMessage(wantlist []bsmsg.Entry) {
	// Save some CPU cycles and allocations if log level is higher than debug
	if ce := sflog.Check(zap.DebugLevel, "sent message"); ce == nil {
		return
	}

	self := mq.network.Self()
	for _, e := range wantlist {
		if e.Cancel {
			if e.WantType == pb.Message_Wantlist_Have {
				log.Debugw("sent message",
					"type", "CANCEL_WANT_HAVE",
					"cid", e.Cid,
					"local", self,
					"to", mq.p,
				)
			} else {
				log.Debugw("sent message",
					"type", "CANCEL_WANT_BLOCK",
					"cid", e.Cid,
					"local", self,
					"to", mq.p,
				)
			}
		} else {
			if e.WantType == pb.Message_Wantlist_Have {
				log.Debugw("sent message",
					"type", "WANT_HAVE",
					"cid", e.Cid,
					"local", self,
					"to", mq.p,
				)
			} else {
				log.Debugw("sent message",
					"type", "WANT_BLOCK",
					"cid", e.Cid,
					"local", self,
					"to", mq.p,
				)
			}
		}
	}
}

// Whether there is work to be processed
func (mq *MessageQueue) hasPendingWork() bool {
	return mq.pendingWorkCount() > 0
}

// The amount of work that is waiting to be processed
func (mq *MessageQueue) pendingWorkCount() int {
	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	return mq.bcstWants.pending.Len() + mq.peerWants.pending.Len() + mq.cancels.Len()
}

// Convert the lists of wants into a Bitswap message
func (mq *MessageQueue) extractOutgoingMessage(supportsHave bool) (bsmsg.BitSwapMessage, func([]bsmsg.Entry)) {
	mq.wllock.Lock()
	defer mq.wllock.Unlock()

	// Get broadcast and regular wantlist entries
	bcstEntries := mq.bcstWants.pending.SortedEntries()
	peerEntries := mq.peerWants.pending.SortedEntries()

	// Size of the message so far
	msgSize := 0

	// Always prioritize cancels, then targeted, then broadcast.

	// Add each cancel to the message
	cancels := mq.cancels.Keys()
	for i := 0; i < len(cancels) && msgSize < mq.maxMessageSize; i++ {
		c := cancels[i]

		msgSize += mq.msg.Cancel(c)

		// Clear the cancel - we make a best effort to let peers know about
		// cancels but won't save them to resend if there's a failure.
		mq.cancels.Remove(c)
	}

	// Add each regular want-have / want-block to the message
	peerSentCount := 0
	for ; peerSentCount < len(peerEntries) && msgSize < mq.maxMessageSize; peerSentCount++ {
		e := peerEntries[peerSentCount]
		// If the remote peer doesn't support HAVE / DONT_HAVE messages,
		// don't send want-haves (only send want-blocks)
		if !supportsHave && e.WantType == pb.Message_Wantlist_Have {
			mq.peerWants.RemoveType(e.Cid, pb.Message_Wantlist_Have)
		} else {
			msgSize += mq.msg.AddEntry(e.Cid, e.Priority, e.WantType, true)
		}
	}

	// Add each broadcast want-have to the message
	bcstSentCount := 0
	for ; bcstSentCount < len(bcstEntries) && msgSize < mq.maxMessageSize; bcstSentCount++ {
		// Broadcast wants are sent as want-have
		wantType := pb.Message_Wantlist_Have

		// If the remote peer doesn't support HAVE / DONT_HAVE messages,
		// send a want-block instead
		if !supportsHave {
			wantType = pb.Message_Wantlist_Block
		}

		e := bcstEntries[bcstSentCount]
		msgSize += mq.msg.AddEntry(e.Cid, e.Priority, wantType, false)
	}

	// Called when the message has been successfully sent.
	onMessageSent := func(wantlist []bsmsg.Entry) {
		mq.wllock.Lock()
		defer mq.wllock.Unlock()

		// Move the keys from pending to sent
		for i := 0; i < bcstSentCount; i++ {
			mq.bcstWants.MarkSent(bcstEntries[i])
		}
		for i := 0; i < peerSentCount; i++ {
			mq.peerWants.MarkSent(peerEntries[i])
		}
	}

	return mq.msg, onMessageSent
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
		log.Warn("SendMsg errored but neither 'done' nor context.Done() were set")
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
