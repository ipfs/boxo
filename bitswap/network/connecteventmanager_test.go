package network

import (
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/bitswap/internal/testutil"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type mockConnEvent struct {
	connected bool
	peer      peer.ID
}

type mockConnListener struct {
	sync.Mutex
	events          []mockConnEvent
	peerConnectedCb func(p peer.ID)
}

func newMockConnListener() *mockConnListener {
	return new(mockConnListener)
}

func (cl *mockConnListener) PeerConnected(p peer.ID) {
	cl.Lock()
	defer cl.Unlock()
	cl.events = append(cl.events, mockConnEvent{connected: true, peer: p})
	if cl.peerConnectedCb != nil {
		cl.peerConnectedCb(p)
	}
}

func (cl *mockConnListener) PeerDisconnected(p peer.ID) {
	cl.Lock()
	defer cl.Unlock()
	cl.events = append(cl.events, mockConnEvent{connected: false, peer: p})
}

func wait(t *testing.T, c *connectEventManager) {
	require.Eventually(t, func() bool {
		c.lk.RLock()
		defer c.lk.RUnlock()
		return len(c.changeQueue) == 0
	}, time.Second, time.Millisecond, "connection event manager never processed events")
}

func TestConnectEventManagerConnectDisconnect(t *testing.T) {
	connListener := newMockConnListener()
	peers := testutil.GeneratePeers(2)
	cem := newConnectEventManager(connListener)
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

	require.Equal(t, expectedEvents, connListener.events)

	cem.Connected(peers[1])
	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      peers[1],
		connected: true,
	})
	require.Equal(t, expectedEvents, connListener.events)

	cem.Disconnected(peers[0])
	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      peers[0],
		connected: false,
	})
	// Flush the event queue.
	wait(t, cem)
	require.Equal(t, expectedEvents, connListener.events)
}

func TestConnectEventManagerMarkUnresponsive(t *testing.T) {
	connListener := newMockConnListener()
	p := testutil.GeneratePeers(1)[0]
	cem := newConnectEventManager(connListener)
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

	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      p,
		connected: true,
	})
	require.Equal(t, expectedEvents, connListener.events)

	// Becomes unresponsive.
	cem.MarkUnresponsive(p)
	wait(t, cem)

	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      p,
		connected: false,
	})
	require.Equal(t, expectedEvents, connListener.events)

	// We have a new connection, mark them responsive.
	cem.Connected(p)
	wait(t, cem)
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
	p := testutil.GeneratePeers(1)[0]
	cem := newConnectEventManager(connListener)
	cem.Start()
	t.Cleanup(cem.Stop)

	var expectedEvents []mockConnEvent

	// Handle connected event.
	cem.Connected(p)
	wait(t, cem)

	expectedEvents = append(expectedEvents, mockConnEvent{
		peer:      p,
		connected: true,
	})
	require.Equal(t, expectedEvents, connListener.events)

	// Becomes unresponsive.
	cem.MarkUnresponsive(p)
	wait(t, cem)

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

func TestConnectEventManagerConnectFlowSynchronous(t *testing.T) {
	connListener := newMockConnListener()
	actionsCh := make(chan string)
	connListener.peerConnectedCb = func(p peer.ID) {
		actionsCh <- "PeerConnected:" + p.String()
		time.Sleep(time.Millisecond * 50)
	}

	peers := testutil.GeneratePeers(2)
	cem := newConnectEventManager(connListener)
	cem.Start()
	t.Cleanup(cem.Stop)

	go func() {
		actionsCh <- "Connected:" + peers[0].String()
		cem.Connected(peers[0])
		actionsCh <- "Done:" + peers[0].String()
		actionsCh <- "Connected:" + peers[1].String()
		cem.Connected(peers[1])
		actionsCh <- "Done:" + peers[1].String()
		close(actionsCh)
	}()

	// We expect Done to be sent _after_ PeerConnected, which demonstrates the
	// call to Connected() blocks until PeerConnected() returns.
	gotActions := make([]string, 0, 3)
	for event := range actionsCh {
		gotActions = append(gotActions, event)
	}
	expectedActions := []string{
		"Connected:" + peers[0].String(),
		"PeerConnected:" + peers[0].String(),
		"Done:" + peers[0].String(),
		"Connected:" + peers[1].String(),
		"PeerConnected:" + peers[1].String(),
		"Done:" + peers[1].String(),
	}
	require.Equal(t, expectedActions, gotActions)

	// Flush the event queue.
	wait(t, cem)
	expectedEvents := []mockConnEvent{
		{peer: peers[0], connected: true},
		{peer: peers[1], connected: true},
	}
	require.Equal(t, expectedEvents, connListener.events)
}
