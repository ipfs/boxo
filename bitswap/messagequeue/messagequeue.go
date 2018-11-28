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

type MessageNetwork interface {
	ConnectTo(context.Context, peer.ID) error
	NewMessageSender(context.Context, peer.ID) (bsnet.MessageSender, error)
}

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

func (mq *MessageQueue) RefIncrement() {
	mq.refcnt++
}

func (mq *MessageQueue) RefDecrement() bool {
	mq.refcnt--
	return mq.refcnt > 0
}

func (mq *MessageQueue) AddMessage(entries []*bsmsg.Entry, ses uint64) {
	var work bool
	mq.outlk.Lock()
	defer func() {
		mq.outlk.Unlock()
		if !work {
			return
		}
		select {
		case mq.work <- struct{}{}:
		default:
		}
	}()

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
}

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

func (mq *MessageQueue) doWork(ctx context.Context) {
	// grab outgoing message
	mq.outlk.Lock()
	wlm := mq.out
	if wlm == nil || wlm.Empty() {
		mq.outlk.Unlock()
		return
	}
	mq.out = nil
	mq.outlk.Unlock()

	// NB: only open a stream if we actually have data to send
	if mq.sender == nil {
		err := mq.openSender(ctx)
		if err != nil {
			log.Infof("cant open message sender to peer %s: %s", mq.p, err)
			// TODO: cant connect, what now?
			return
		}
	}

	// send wantlist updates
	for { // try to send this message until we fail.
		err := mq.sender.SendMsg(ctx, wlm)
		if err == nil {
			return
		}

		log.Infof("bitswap send error: %s", err)
		mq.sender.Reset()
		mq.sender = nil

		select {
		case <-mq.done:
			return
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond * 100):
			// wait 100ms in case disconnect notifications are still propogating
			log.Warning("SendMsg errored but neither 'done' nor context.Done() were set")
		}

		err = mq.openSender(ctx)
		if err != nil {
			log.Infof("couldnt open sender again after SendMsg(%s) failed: %s", mq.p, err)
			// TODO(why): what do we do now?
			// I think the *right* answer is to probably put the message we're
			// trying to send back, and then return to waiting for new work or
			// a disconnect.
			return
		}

		// TODO: Is this the same instance for the remote peer?
		// If its not, we should resend our entire wantlist to them
		/*
			if mq.sender.InstanceID() != mq.lastSeenInstanceID {
				wlm = mq.getFullWantlistMessage()
			}
		*/
	}
}

func (mq *MessageQueue) openSender(ctx context.Context) error {
	// allow ten minutes for connections this includes looking them up in the
	// dht dialing them, and handshaking
	conctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()

	err := mq.network.ConnectTo(conctx, mq.p)
	if err != nil {
		return err
	}

	nsender, err := mq.network.NewMessageSender(ctx, mq.p)
	if err != nil {
		return err
	}

	mq.sender = nsender
	return nil
}
