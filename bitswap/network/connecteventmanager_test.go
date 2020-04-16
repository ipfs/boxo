package network

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
	"github.com/libp2p/go-libp2p-core/peer"
)

type mockConnListener struct {
	conns map[peer.ID]int
}

func newMockConnListener() *mockConnListener {
	return &mockConnListener{
		conns: make(map[peer.ID]int),
	}
}

func (cl *mockConnListener) PeerConnected(p peer.ID) {
	cl.conns[p]++
}

func (cl *mockConnListener) PeerDisconnected(p peer.ID) {
	cl.conns[p]--
}

func TestConnectEventManagerConnectionCount(t *testing.T) {
	connListener := newMockConnListener()
	peers := testutil.GeneratePeers(2)
	cem := newConnectEventManager(connListener)

	// Peer A: 1 Connection
	cem.Connected(peers[0])
	if connListener.conns[peers[0]] != 1 {
		t.Fatal("Expected Connected event")
	}

	// Peer A: 2 Connections
	cem.Connected(peers[0])
	if connListener.conns[peers[0]] != 1 {
		t.Fatal("Unexpected no Connected event for the same peer")
	}

	// Peer A: 2 Connections
	// Peer B: 1 Connection
	cem.Connected(peers[1])
	if connListener.conns[peers[1]] != 1 {
		t.Fatal("Expected Connected event")
	}

	// Peer A: 2 Connections
	// Peer B: 0 Connections
	cem.Disconnected(peers[1])
	if connListener.conns[peers[1]] != 0 {
		t.Fatal("Expected Disconnected event")
	}

	// Peer A: 1 Connection
	// Peer B: 0 Connections
	cem.Disconnected(peers[0])
	if connListener.conns[peers[0]] != 1 {
		t.Fatal("Expected no Disconnected event for peer with one remaining conn")
	}

	// Peer A: 0 Connections
	// Peer B: 0 Connections
	cem.Disconnected(peers[0])
	if connListener.conns[peers[0]] != 0 {
		t.Fatal("Expected Disconnected event")
	}
}

func TestConnectEventManagerMarkUnresponsive(t *testing.T) {
	connListener := newMockConnListener()
	p := testutil.GeneratePeers(1)[0]
	cem := newConnectEventManager(connListener)

	// Peer A: 1 Connection
	cem.Connected(p)
	if connListener.conns[p] != 1 {
		t.Fatal("Expected Connected event")
	}

	// Peer A: 1 Connection <Unresponsive>
	cem.MarkUnresponsive(p)
	if connListener.conns[p] != 0 {
		t.Fatal("Expected Disconnected event")
	}

	// Peer A: 2 Connections <Unresponsive>
	cem.Connected(p)
	if connListener.conns[p] != 0 {
		t.Fatal("Expected no Connected event for unresponsive peer")
	}

	// Peer A: 2 Connections <Becomes responsive>
	cem.OnMessage(p)
	if connListener.conns[p] != 1 {
		t.Fatal("Expected Connected event for newly responsive peer")
	}

	// Peer A: 2 Connections
	cem.OnMessage(p)
	if connListener.conns[p] != 1 {
		t.Fatal("Expected no further Connected event for subsequent messages")
	}

	// Peer A: 1 Connection
	cem.Disconnected(p)
	if connListener.conns[p] != 1 {
		t.Fatal("Expected no Disconnected event for peer with one remaining conn")
	}

	// Peer A: 0 Connections
	cem.Disconnected(p)
	if connListener.conns[p] != 0 {
		t.Fatal("Expected Disconnected event")
	}
}

func TestConnectEventManagerDisconnectAfterMarkUnresponsive(t *testing.T) {
	connListener := newMockConnListener()
	p := testutil.GeneratePeers(1)[0]
	cem := newConnectEventManager(connListener)

	// Peer A: 1 Connection
	cem.Connected(p)
	if connListener.conns[p] != 1 {
		t.Fatal("Expected Connected event")
	}

	// Peer A: 1 Connection <Unresponsive>
	cem.MarkUnresponsive(p)
	if connListener.conns[p] != 0 {
		t.Fatal("Expected Disconnected event")
	}

	// Peer A: 0 Connections
	cem.Disconnected(p)
	if connListener.conns[p] != 0 {
		t.Fatal("Expected not to receive a second Disconnected event")
	}
}
