package bootstrap

import (
	"context"
	"crypto/rand"
	"reflect"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	ma "github.com/multiformats/go-multiaddr"
)

func TestRandomizeAddressList(t *testing.T) {
	var ps []peer.AddrInfo
	sizeofSlice := 10
	for i := 0; i < sizeofSlice; i++ {
		pid, err := test.RandPeerID()
		if err != nil {
			t.Fatal(err)
		}

		ps = append(ps, peer.AddrInfo{ID: pid})
	}
	out := randomizeList(ps)
	if len(out) != len(ps) {
		t.Fail()
	}
}

func TestLoadAndSaveOptions(t *testing.T) {
	loadFunc := func(_ context.Context) []peer.AddrInfo { return nil }
	saveFunc := func(_ context.Context, _ []peer.AddrInfo) {}

	bootCfg := BootstrapConfigWithPeers(nil, WithBackupPeers(loadFunc, saveFunc))
	load, save := bootCfg.BackupPeers()
	if load == nil {
		t.Fatal("load function not assigned")
	}
	if reflect.ValueOf(load).Pointer() != reflect.ValueOf(loadFunc).Pointer() {
		t.Fatal("load not assigned correct function")
	}
	if save == nil {
		t.Fatal("save function not assigned")
	}
	if reflect.ValueOf(save).Pointer() != reflect.ValueOf(saveFunc).Pointer() {
		t.Fatal("save not assigned correct function")
	}

	assertPanics(t, "with only load func", func() {
		BootstrapConfigWithPeers(nil, WithBackupPeers(loadFunc, nil))
	})

	assertPanics(t, "with only save func", func() {
		BootstrapConfigWithPeers(nil, WithBackupPeers(nil, saveFunc))
	})

	bootCfg = BootstrapConfigWithPeers(nil, WithBackupPeers(nil, nil))
	load, save = bootCfg.BackupPeers()
	if load != nil || save != nil {
		t.Fatal("load and save functions should both be nil")
	}
}

func TestSetBackupPeers(t *testing.T) {
	loadFunc := func(_ context.Context) []peer.AddrInfo { return nil }
	saveFunc := func(_ context.Context, _ []peer.AddrInfo) {}

	bootCfg := DefaultBootstrapConfig
	bootCfg.SetBackupPeers(loadFunc, saveFunc)
	load, save := bootCfg.BackupPeers()
	if load == nil {
		t.Fatal("load function not assigned")
	}
	if reflect.ValueOf(load).Pointer() != reflect.ValueOf(loadFunc).Pointer() {
		t.Fatal("load not assigned correct function")
	}
	if save == nil {
		t.Fatal("save function not assigned")
	}
	if reflect.ValueOf(save).Pointer() != reflect.ValueOf(saveFunc).Pointer() {
		t.Fatal("save not assigned correct function")
	}

	assertPanics(t, "with only load func", func() {
		bootCfg.SetBackupPeers(loadFunc, nil)
	})

	assertPanics(t, "with only save func", func() {
		bootCfg.SetBackupPeers(nil, saveFunc)
	})

	bootCfg.SetBackupPeers(nil, nil)
	load, save = bootCfg.BackupPeers()
	if load != nil || save != nil {
		t.Fatal("load and save functions should both be nil")
	}
}

func TestNoTempPeersLoadAndSave(t *testing.T) {
	period := 500 * time.Millisecond
	bootCfg := BootstrapConfigWithPeers(nil)
	bootCfg.MinPeerThreshold = 2
	bootCfg.Period = period

	priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	p2pHost, err := libp2p.New(libp2p.Identity(priv))
	if err != nil {
		t.Fatal(err)
	}

	bootstrapper, err := Bootstrap(peerID, p2pHost, nil, bootCfg)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(4 * period)
	bootstrapper.Close()
}

func assertPanics(t *testing.T, name string, f func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: did not panic as expected", name)
		}
	}()

	f()
}

func TestHasDirectAddresses(t *testing.T) {
	// Create valid peer IDs for testing
	testPeer1, err := test.RandPeerID()
	if err != nil {
		t.Fatal(err)
	}
	testPeer2, err := test.RandPeerID()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		addrs    []string
		expected bool
	}{
		{
			name:     "direct addresses only",
			addrs:    []string{"/ip4/127.0.0.1/tcp/4001", "/ip6/::1/tcp/4001"},
			expected: true,
		},
		{
			name:     "relay addresses only",
			addrs:    []string{"/p2p-circuit/p2p/" + testPeer1.String()},
			expected: false,
		},
		{
			name:     "mixed addresses",
			addrs:    []string{"/ip4/127.0.0.1/tcp/4001", "/p2p-circuit/p2p/" + testPeer1.String()},
			expected: true,
		},
		{
			name:     "empty addresses",
			addrs:    []string{},
			expected: false,
		},
		{
			name:     "multiple relay addresses",
			addrs:    []string{"/p2p-circuit/p2p/" + testPeer1.String(), "/p2p-circuit/p2p/" + testPeer2.String()},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert strings to multiaddrs
			addrs := make([]ma.Multiaddr, len(tt.addrs))
			for i, addrStr := range tt.addrs {
				addr, err := ma.NewMultiaddr(addrStr)
				if err != nil {
					t.Fatalf("failed to create multiaddr from %s: %v", addrStr, err)
				}
				addrs[i] = addr
			}

			peerInfo := peer.AddrInfo{
				ID:    "QmTest",
				Addrs: addrs,
			}

			result := hasDirectAddresses(peerInfo)
			if result != tt.expected {
				t.Errorf("hasDirectAddresses() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestHasCircuitProtocol(t *testing.T) {
	// Create valid peer IDs for testing
	testPeer1, err := test.RandPeerID()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		addr     string
		expected bool
	}{
		{
			name:     "direct TCP address",
			addr:     "/ip4/192.168.1.100/tcp/4001",
			expected: false,
		},
		{
			name:     "direct QUIC address",
			addr:     "/ip4/10.0.0.50/udp/4001/quic",
			expected: false,
		},
		{
			name:     "circuit relay address",
			addr:     "/ip4/203.0.113.1/tcp/4001/p2p/" + testPeer1.String() + "/p2p-circuit",
			expected: true,
		},
		{
			name:     "IPv6 direct address",
			addr:     "/ip6/2001:db8::1/tcp/4001",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := ma.NewMultiaddr(tt.addr)
			if err != nil {
				t.Fatalf("failed to create multiaddr from %s: %v", tt.addr, err)
			}

			result := hasCircuitProtocol(addr)
			if result != tt.expected {
				t.Errorf("hasCircuitProtocol(%s) = %v, expected %v", tt.addr, result, tt.expected)
			}
		})
	}
}
