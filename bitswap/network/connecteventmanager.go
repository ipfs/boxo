package network

import (
	"sync"

	"github.com/gammazero/deque"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("bitswap/connevtman")

type ConnectionListener interface {
	PeerConnected(peer.ID)
	PeerDisconnected(peer.ID)
}

type state byte

const (
	stateDisconnected = iota
	stateResponsive
	stateUnresponsive
)

type ConnectEventManager struct {
	connListeners []ConnectionListener
	lk            sync.RWMutex
	cond          sync.Cond
	peers         map[peer.ID]*peerState

	changeQueue deque.Deque[peer.ID]
	stop        bool
	done        chan struct{}
}

type peerState struct {
	newState, curState state
	pending            bool
}

func NewConnectEventManager(connListeners ...ConnectionListener) *ConnectEventManager {
	evtManager := &ConnectEventManager{
		connListeners: connListeners,
		peers:         make(map[peer.ID]*peerState),
		done:          make(chan struct{}),
	}
	evtManager.cond = sync.Cond{L: &evtManager.lk}
	return evtManager
}

// SetListeners sets or replaces the current listeners. Not safe to call after
// Start().
func (c *ConnectEventManager) SetListeners(connListeners ...ConnectionListener) {
	c.connListeners = connListeners
}

func (c *ConnectEventManager) Start() {
	go c.worker()
}

func (c *ConnectEventManager) Stop() {
	c.lk.Lock()
	c.stop = true
	c.lk.Unlock()
	c.cond.Broadcast()

	<-c.done
}

func (c *ConnectEventManager) getState(p peer.ID) state {
	if state, ok := c.peers[p]; ok {
		return state.newState
	} else {
		return stateDisconnected
	}
}

func (c *ConnectEventManager) setState(p peer.ID, newState state) {
	state, ok := c.peers[p]
	if !ok {
		state = new(peerState)
		c.peers[p] = state
	}
	state.newState = newState
	if !state.pending && state.newState != state.curState {
		state.pending = true
		c.changeQueue.PushBack(p)
		c.cond.Broadcast()
	}
}

// Waits for a change to be enqueued, or for the event manager to be stopped. Returns false if the
// connect event manager has been stopped.
func (c *ConnectEventManager) waitChange() bool {
	for !c.stop && c.changeQueue.Len() == 0 {
		c.cond.Wait()
	}
	return !c.stop
}

func (c *ConnectEventManager) worker() {
	c.lk.Lock()
	defer c.lk.Unlock()
	defer close(c.done)

	for c.waitChange() {
		pid := c.changeQueue.PopFront()

		state, ok := c.peers[pid]
		// If we've disconnected and forgotten, continue.
		if !ok {
			// This shouldn't be possible because _this_ thread is responsible for
			// removing peers from this map, and we shouldn't get duplicate entries in
			// the change queue.
			log.Error("a change was enqueued for a peer we're not tracking")
			continue
		}

		// Record the fact that this "state" is no longer in the queue.
		state.pending = false

		// Then, if there's nothing to do, continue.
		if state.curState == state.newState {
			continue
		}

		// Or record the state update, then apply it.
		oldState := state.curState
		state.curState = state.newState

		switch state.newState {
		case stateDisconnected:
			delete(c.peers, pid)
			fallthrough
		case stateUnresponsive:
			// Only trigger a disconnect event if the peer was responsive.
			// We could be transitioning from unresponsive to disconnected.
			if oldState == stateResponsive {
				c.lk.Unlock()
				for _, v := range c.connListeners {
					v.PeerDisconnected(pid)
				}
				c.lk.Lock()
			}
		case stateResponsive:
			c.lk.Unlock()
			for _, v := range c.connListeners {
				v.PeerConnected(pid)
			}
			c.lk.Lock()
		}
	}
}

// Called whenever we receive a new connection. May be called many times.
func (c *ConnectEventManager) Connected(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	// !responsive -> responsive

	if c.getState(p) == stateResponsive {
		return
	}
	c.setState(p, stateResponsive)
}

// Called when we drop the final connection to a peer.
func (c *ConnectEventManager) Disconnected(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	// !disconnected -> disconnected

	if c.getState(p) == stateDisconnected {
		return
	}

	c.setState(p, stateDisconnected)
}

// Called whenever a peer is unresponsive.
func (c *ConnectEventManager) MarkUnresponsive(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	// responsive -> unresponsive

	if c.getState(p) != stateResponsive {
		return
	}

	c.setState(p, stateUnresponsive)
}

// Called whenever we receive a message from a peer.
//
// - When we're connected to the peer, this will mark the peer as responsive (from unresponsive).
// - When not connected, we ignore this call. Unfortunately, a peer may disconnect before we process
//
//	the "on message" event, so we can't treat this as evidence of a connection.
func (c *ConnectEventManager) OnMessage(p peer.ID) {
	c.lk.RLock()
	unresponsive := c.getState(p) == stateUnresponsive
	c.lk.RUnlock()

	// Only continue if both connected, and unresponsive.
	if !unresponsive {
		return
	}

	// unresponsive -> responsive

	// We need to make a modification so now take a write lock
	c.lk.Lock()
	defer c.lk.Unlock()

	// Note: state may have changed in the time between when read lock
	// was released and write lock taken, so check again
	if c.getState(p) != stateUnresponsive {
		return
	}

	c.setState(p, stateResponsive)
}
