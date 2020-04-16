package network

import (
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
)

type ConnectionListener interface {
	PeerConnected(peer.ID)
	PeerDisconnected(peer.ID)
}

type connectEventManager struct {
	connListener ConnectionListener
	lk           sync.Mutex
	conns        map[peer.ID]*connState
}

type connState struct {
	refs       int
	responsive bool
}

func newConnectEventManager(connListener ConnectionListener) *connectEventManager {
	return &connectEventManager{
		connListener: connListener,
		conns:        make(map[peer.ID]*connState),
	}
}

func (c *connectEventManager) Connected(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	state, ok := c.conns[p]
	if !ok {
		state = &connState{responsive: true}
		c.conns[p] = state
	}
	state.refs++

	if state.refs == 1 && state.responsive {
		c.connListener.PeerConnected(p)
	}
}

func (c *connectEventManager) Disconnected(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	state, ok := c.conns[p]
	if !ok {
		// Should never happen
		return
	}
	state.refs--
	c.conns[p] = state

	if state.refs == 0 {
		if state.responsive {
			c.connListener.PeerDisconnected(p)
		}
		delete(c.conns, p)
	}
}

func (c *connectEventManager) MarkUnresponsive(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	state, ok := c.conns[p]
	if !ok {
		return
	}
	state.responsive = false
	c.conns[p] = state

	c.connListener.PeerDisconnected(p)
}

func (c *connectEventManager) OnMessage(p peer.ID) {
	c.lk.Lock()
	defer c.lk.Unlock()

	state, ok := c.conns[p]
	if ok && !state.responsive {
		state.responsive = true
		c.conns[p] = state
		c.connListener.PeerConnected(p)
	}
}
