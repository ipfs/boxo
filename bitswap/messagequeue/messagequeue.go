package messagequeue

import (
	"context"
	"sync"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"
	bsnet "github.com/ipfs/go-bitswap/network"
	wantlist "github.com/ipfs/go-bitswap/wantlist"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-peer"
)

var log = logging.Logger("bitswap")

// MessageNetwork is any network that can connect peers and generate a message
// sender.
type MessageNetwork interface {
	ConnectTo(context.Context, peer.ID) error
	NewMessageSender(context.Context, peer.ID) (bsnet.MessageSender, error)
}

// MessageQueue implements queue of want messages to send to peers.
type MessageQueue struct {
	p peer.ID

	outlk   sync.Mutex
	out     bsmsg.BitSwapMessage
	network MessageNetwork
	wl      *wantlist.ThreadSafe

	sender bsnet.MessageSender

	refcnt int

	work chan struct{}
	done chan struct{}
}

// New creats a new MessageQueue.
func New(p peer.ID, network MessageNetwork) *MessageQueue {
	return &MessageQueue{
		done:    make(chan struct{}),
		work:    make(chan struct{}, 1),
		wl:      wantlist.NewThreadSafe(),
		network: network,
		p:       p,
		refcnt:  1,
	}
}

// RefIncrement increments the refcount for a message queue.
func (mq *MessageQueue) RefIncrement() {
	mq.refcnt++
}

// RefDecrement decrements the refcount for a message queue and returns true
// if the refcount is now 0.
func (mq *MessageQueue) RefDecrement() bool {
	mq.refcnt--
	return mq.refcnt > 0
}

// AddMessage adds new entries to an outgoing message for a given session.
func (mq *MessageQueue) AddMessage(entries []*bsmsg.Entry, ses uint64) {
	if !mq.addEntries(entries, ses) {
		return
	}
	select {
	case mq.work <- struct{}{}:
	default:
	}
}

// Startup starts the processing of messages, and creates an initial message
// based on the given initial wantlist.
func (mq *MessageQueue) Startup(ctx context.Context, initialEntries []*wantlist.Entry) {

	// new peer, we will want to give them our full wantlist
	if len(initialEntries) > 0 {
		fullwantlist := bsmsg.New(true)
		for _, e := range initialEntries {
			for k := range e.SesTrk {
				mq.wl.AddEntry(e, k)
			}
			fullwantlist.AddEntry(e.Cid, e.Priority)
		}
		mq.out = fullwantlist
		mq.work <- struct{}{}
	}
	go mq.runQueue(ctx)

}

// Shutdown stops the processing of messages for a message queue.
func (mq *MessageQueue) Shutdown() {
	close(mq.done)
}

func (mq *MessageQueue) runQueue(ctx context.Context) {
	for {
		select {
		case <-mq.work: // there is work to be done
			mq.doWork(ctx)
		case <-mq.done:
			if mq.sender != nil {
				mq.sender.Close()
			}
			return
		case <-ctx.Done():
			if mq.sender != nil {
				mq.sender.Reset()
			}
			return
		}
	}
}

func (mq *MessageQueue) addEntries(entries []*bsmsg.Entry, ses uint64) bool {
	var work bool
	mq.outlk.Lock()
	defer mq.outlk.Unlock()
	// if we have no message held allocate a new one
	if mq.out == nil {
		mq.out = bsmsg.New(false)
	}

	// TODO: add a msg.Combine(...) method
	// otherwise, combine the one we are holding with the
	// one passed in
	for _, e := range entries {
		if e.Cancel {
			if mq.wl.Remove(e.Cid, ses) {
				work = true
				mq.out.Cancel(e.Cid)
			}
		} else {
			if mq.wl.Add(e.Cid, e.Priority, ses) {
				work = true
				mq.out.AddEntry(e.Cid, e.Priority)
			}
		}
	}

	return work
}

func (mq *MessageQueue) doWork(ctx context.Context) {

	wlm := mq.extractOutgoingMessage()
	if wlm == nil || wlm.Empty() {
		return
	}

	// NB: only open a stream if we actually have data to send
	err := mq.initializeSender(ctx)
	if err != nil {
		log.Infof("cant open message sender to peer %s: %s", mq.p, err)
		// TODO: cant connect, what now?
		return
	}

	// send wantlist updates
	for { // try to send this message until we fail.
		if mq.attemptSendAndRecovery(ctx, wlm) {
			return
		}
	}
}

func (mq *MessageQueue) initializeSender(ctx context.Context) error {
	if mq.sender != nil {
		return nil
	}
	nsender, err := openSender(ctx, mq.network, mq.p)
	if err != nil {
		return err
	}
	mq.sender = nsender
	return nil
}

func (mq *MessageQueue) attemptSendAndRecovery(ctx context.Context, wlm bsmsg.BitSwapMessage) bool {
	err := mq.sender.SendMsg(ctx, wlm)
	if err == nil {
		return true
	}

	log.Infof("bitswap send error: %s", err)
	mq.sender.Reset()
	mq.sender = nil

	select {
	case <-mq.done:
		return true
	case <-ctx.Done():
		return true
	case <-time.After(time.Millisecond * 100):
		// wait 100ms in case disconnect notifications are still propogating
		log.Warning("SendMsg errored but neither 'done' nor context.Done() were set")
	}

	err = mq.initializeSender(ctx)
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

func (mq *MessageQueue) extractOutgoingMessage() bsmsg.BitSwapMessage {
	// grab outgoing message
	mq.outlk.Lock()
	wlm := mq.out
	mq.out = nil
	mq.outlk.Unlock()
	return wlm
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
