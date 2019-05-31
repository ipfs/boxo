package messagequeue

import (
	"context"
	"sync"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"
	bsnet "github.com/ipfs/go-bitswap/network"
	wantlist "github.com/ipfs/go-bitswap/wantlist"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bitswap")

const (
	defaultRebroadcastInterval = 30 * time.Second
	maxRetries                 = 10
)

// MessageNetwork is any network that can connect peers and generate a message
// sender.
type MessageNetwork interface {
	ConnectTo(context.Context, peer.ID) error
	NewMessageSender(context.Context, peer.ID) (bsnet.MessageSender, error)
}

// MessageQueue implements queue of want messages to send to peers.
type MessageQueue struct {
	ctx     context.Context
	p       peer.ID
	network MessageNetwork

	outgoingWork chan struct{}
	done         chan struct{}

	// do not touch out of run loop
	wl                    *wantlist.SessionTrackedWantlist
	nextMessage           bsmsg.BitSwapMessage
	nextMessageLk         sync.RWMutex
	sender                bsnet.MessageSender
	rebroadcastIntervalLk sync.RWMutex
	rebroadcastInterval   time.Duration
	rebroadcastTimer      *time.Timer
}

// New creats a new MessageQueue.
func New(ctx context.Context, p peer.ID, network MessageNetwork) *MessageQueue {
	return &MessageQueue{
		ctx:                 ctx,
		wl:                  wantlist.NewSessionTrackedWantlist(),
		network:             network,
		p:                   p,
		outgoingWork:        make(chan struct{}, 1),
		done:                make(chan struct{}),
		rebroadcastInterval: defaultRebroadcastInterval,
	}
}

// AddMessage adds new entries to an outgoing message for a given session.
func (mq *MessageQueue) AddMessage(entries []bsmsg.Entry, ses uint64) {
	if !mq.addEntries(entries, ses) {
		return
	}
	select {
	case mq.outgoingWork <- struct{}{}:
	default:
	}
}

// AddWantlist adds a complete session tracked want list to a message queue
func (mq *MessageQueue) AddWantlist(initialWants *wantlist.SessionTrackedWantlist) {
	initialWants.CopyWants(mq.wl)
	mq.addWantlist()
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

// Startup starts the processing of messages, and creates an initial message
// based on the given initial wantlist.
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

func (mq *MessageQueue) runQueue() {
	for {
		select {
		case <-mq.rebroadcastTimer.C:
			mq.rebroadcastWantlist()
		case <-mq.outgoingWork:
			mq.sendMessage()
		case <-mq.done:
			if mq.sender != nil {
				mq.sender.Close()
			}
			return
		case <-mq.ctx.Done():
			if mq.sender != nil {
				mq.sender.Reset()
			}
			return
		}
	}
}

func (mq *MessageQueue) addWantlist() {

	mq.nextMessageLk.Lock()
	defer mq.nextMessageLk.Unlock()

	if mq.wl.Len() > 0 {
		if mq.nextMessage == nil {
			mq.nextMessage = bsmsg.New(false)
		}
		for _, e := range mq.wl.Entries() {
			mq.nextMessage.AddEntry(e.Cid, e.Priority)
		}
		select {
		case mq.outgoingWork <- struct{}{}:
		default:
		}
	}
}

func (mq *MessageQueue) rebroadcastWantlist() {
	mq.rebroadcastIntervalLk.RLock()
	mq.rebroadcastTimer.Reset(mq.rebroadcastInterval)
	mq.rebroadcastIntervalLk.RUnlock()

	mq.addWantlist()
}

func (mq *MessageQueue) addEntries(entries []bsmsg.Entry, ses uint64) bool {
	var work bool
	mq.nextMessageLk.Lock()
	defer mq.nextMessageLk.Unlock()
	// if we have no message held allocate a new one
	if mq.nextMessage == nil {
		mq.nextMessage = bsmsg.New(false)
	}

	for _, e := range entries {
		if e.Cancel {
			if mq.wl.Remove(e.Cid, ses) {
				work = true
				mq.nextMessage.Cancel(e.Cid)
			}
		} else {
			if mq.wl.Add(e.Cid, e.Priority, ses) {
				work = true
				mq.nextMessage.AddEntry(e.Cid, e.Priority)
			}
		}
	}
	return work
}

func (mq *MessageQueue) extractOutgoingMessage() bsmsg.BitSwapMessage {
	// grab outgoing message
	mq.nextMessageLk.Lock()
	message := mq.nextMessage
	mq.nextMessage = nil
	mq.nextMessageLk.Unlock()
	return message
}

func (mq *MessageQueue) sendMessage() {
	message := mq.extractOutgoingMessage()
	if message == nil || message.Empty() {
		return
	}

	err := mq.initializeSender()
	if err != nil {
		log.Infof("cant open message sender to peer %s: %s", mq.p, err)
		// TODO: cant connect, what now?
		return
	}

	for i := 0; i < maxRetries; i++ { // try to send this message until we fail.
		if mq.attemptSendAndRecovery(message) {
			return
		}
	}
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
	mq.sender.Reset()
	mq.sender = nil

	select {
	case <-mq.done:
		return true
	case <-mq.ctx.Done():
		return true
	case <-time.After(time.Millisecond * 100):
		// wait 100ms in case disconnect notifications are still propogating
		log.Warning("SendMsg errored but neither 'done' nor context.Done() were set")
	}

	err = mq.initializeSender()
	if err != nil {
		log.Infof("couldnt open sender again after SendMsg(%s) failed: %s", mq.p, err)
		// TODO(why): what do we do now?
		// I think the *right* answer is to probably put the message we're
		// trying to send back, and then return to waiting for new work or
		// a disconnect.
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
