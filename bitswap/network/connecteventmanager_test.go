package network

import (
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type mockConnEvent struct {
	connected bool
	peer      peer.ID
}

type mockConnListener struct {
	sync.Mutex
	events      []mockConnEvent
	eventSignal chan struct{}
}

func newMockConnListener() *mockConnListener {
	return &mockConnListener{
		eventSignal: make(chan struct{}, 1),
	}
}

func (cl *mockConnListener) PeerConnected(p peer.ID) {
	cl.Lock()
	defer cl.Unlock()
	cl.events = append(cl.events, mockConnEvent{connected: true, peer: p})
	cl.signalEvent()
}

func (cl *mockConnListener) PeerDisconnected(p peer.ID) {
	cl.Lock()
	defer cl.Unlock()
	cl.events = append(cl.events, mockConnEvent{connected: false, peer: p})
	cl.signalEvent()
}

func (cl *mockConnListener) signalEvent() {
	select {
	case cl.eventSignal <- struct{}{}:
	default:
	}
}

func (cl *mockConnListener) waitEvent(t *testing.T) {
	t.Helper()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	select {
	case <-cl.eventSignal:
	case <-timeout.C:
		t.Fatal("timed out waiting for ConnListener event")
	}
}

func wait(t *testing.T, c *ConnectEventManager) {
	require.Eventually(t, func() bool {
		c.lk.RLock()
		defer c.lk.RUnlock()
		return c.changeQueue.Len() == 0
	}, time.Second, time.Millisecond, "connection event manager never processed events")
}

func TestConnectEventManagerConnectDisconnect(t *testing.T) {
	connListener := newMockConnListener()
	peers := random.Peers(2)
	cem := NewConnectEventManager(connListener)
	cem.Start()
	t.Cleanup(cem.Stop)

	var expectedEvents []mockConnEvent

	// Connect A twice, should only see one event
	cem.Connected(peers[0])
	cem.Connected(peers[0])
	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      peers[0],
		connected: true,
	})

	// Flush the event queue.
	wait(t, cem)
	connListener.waitEvent(t)
	require.Equal(t, expectedEvents, connListener.events)

	// Block up the event loop.
	connListener.Lock()
	cem.Connected(peers[1])
	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      peers[1],
		connected: true,
	})

	// We don't expect this to show up.
	cem.Disconnected(peers[0])
	cem.Connected(peers[0])

	connListener.Unlock()

	wait(t, cem)
	connListener.waitEvent(t)
	require.Equal(t, expectedEvents, connListener.events)
}

func TestConnectEventManagerMarkUnresponsive(t *testing.T) {
	connListener := newMockConnListener()
	p := random.Peers(1)[0]
	cem := NewConnectEventManager(connListener)
	cem.Start()
	t.Cleanup(cem.Stop)

	var expectedEvents []mockConnEvent

	// Don't mark as connected when we receive a message (could have been delayed).
	cem.OnMessage(p)
	wait(t, cem)
	require.Equal(t, expectedEvents, connListener.events)

	// Handle connected event.
	cem.Connected(p)
	wait(t, cem)
	connListener.waitEvent(t)

	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      p,
		connected: true,
	})
	require.Equal(t, expectedEvents, connListener.events)

	// Becomes unresponsive.
	cem.MarkUnresponsive(p)
	wait(t, cem)
	connListener.waitEvent(t)

	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      p,
		connected: false,
	})
	require.Equal(t, expectedEvents, connListener.events)

	// We have a new connection, mark them responsive.
	cem.Connected(p)
	wait(t, cem)
	connListener.waitEvent(t)
	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      p,
		connected: true,
	})
	require.Equal(t, expectedEvents, connListener.events)

	// No duplicate event.
	cem.OnMessage(p)
	wait(t, cem)
	require.Equal(t, expectedEvents, connListener.events)
}

func TestConnectEventManagerDisconnectAfterMarkUnresponsive(t *testing.T) {
	connListener := newMockConnListener()
	p := random.Peers(1)[0]
	cem := NewConnectEventManager(connListener)
	cem.Start()
	t.Cleanup(cem.Stop)

	var expectedEvents []mockConnEvent

	// Handle connected event.
	cem.Connected(p)
	wait(t, cem)
	connListener.waitEvent(t)

	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      p,
		connected: true,
	})
	require.Equal(t, expectedEvents, connListener.events)

	// Becomes unresponsive.
	cem.MarkUnresponsive(p)
	wait(t, cem)
	connListener.waitEvent(t)

	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      p,
		connected: false,
	})
	require.Equal(t, expectedEvents, connListener.events)

	cem.Disconnected(p)
	wait(t, cem)
	require.Empty(t, cem.peers) // all disconnected
	require.Equal(t, expectedEvents, connListener.events)
}
