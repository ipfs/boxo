package bootstrap

import (
	"context"
	"crypto/rand"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/core/test"
	ma "github.com/multiformats/go-multiaddr"
)

func TestRandomizeAddressList(t *testing.T) {
	var ps []peer.AddrInfo
	sizeofSlice := 10
	for range sizeofSlice {
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

// TestBootstrapRoundSkipsBackupWhenNoBootstrapPeers verifies that bootstrapRound
// does not consult the backup peer list when no bootstrap peers are configured.
// The backup list exists only as a recovery mechanism for when configured
// bootstrap peers are down; with no configured peers there is nothing to
// recover from, so dialing stale backup peers from previous runs would be
// unwanted network traffic. See ipfs/kubo#11452.
func TestBootstrapRoundSkipsBackupWhenNoBootstrapPeers(t *testing.T) {
	backupCalled := false
	loadFunc := func(_ context.Context) []peer.AddrInfo {
		backupCalled = true
		return nil
	}
	saveFunc := func(_ context.Context, _ []peer.AddrInfo) {}

	bootCfg := BootstrapConfigWithPeers(nil, WithBackupPeers(loadFunc, saveFunc))
	bootCfg.MinPeerThreshold = 2

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p2pHost, err := libp2p.New(libp2p.Identity(priv))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p2pHost.Close() })

	if err := bootstrapRound(t.Context(), p2pHost, bootCfg); err != nil {
		t.Fatalf("bootstrapRound returned error: %v", err)
	}
	if backupCalled {
		t.Fatal("bootstrapRound consulted the backup peer list despite no bootstrap peers being configured")
	}
}

// TestBootstrapRoundDialsBackupWhenBootstrapPeersPresent confirms the backup
// list is still consulted when configured bootstrap peers fail to connect,
// preserving the recovery mechanism from ipfs/kubo#8856.
func TestBootstrapRoundDialsBackupWhenBootstrapPeersPresent(t *testing.T) {
	backupCalled := false
	loadFunc := func(_ context.Context) []peer.AddrInfo {
		backupCalled = true
		return nil
	}
	saveFunc := func(_ context.Context, _ []peer.AddrInfo) {}

	// A fake, undialable bootstrap peer so peersConnect attempts and fails
	// rather than short-circuiting on an empty list.
	fakeID, err := test.RandPeerID()
	if err != nil {
		t.Fatal(err)
	}
	fakePeer := peer.AddrInfo{ID: fakeID}
	bootCfg := BootstrapConfigWithPeers([]peer.AddrInfo{fakePeer}, WithBackupPeers(loadFunc, saveFunc))
	bootCfg.MinPeerThreshold = 2
	// Keep the round snappy: the dial will fail fast against a random peer ID.
	bootCfg.ConnectionTimeout = 500 * time.Millisecond

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p2pHost, err := libp2p.New(libp2p.Identity(priv))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p2pHost.Close() })

	_ = bootstrapRound(t.Context(), p2pHost, bootCfg)
	if !backupCalled {
		t.Fatal("bootstrapRound did not consult the backup peer list despite configured bootstrap peers failing to connect")
	}
}

const testPeriod = time.Minute

// fakeHost implements only what bootstrapRound and
// saveConnectedPeersAsTemporaryBootstrap use when cfg.BootstrapPeers returns
// no peers, so no real libp2p goroutines run inside the synctest bubble.
type fakeHost struct {
	host.Host
	network fakeNetwork
}

func (fakeHost) ID() peer.ID                { return "" }
func (h fakeHost) Network() network.Network { return h.network }

type fakeNetwork struct {
	network.Network
	peers []peer.ID
}

func (n fakeNetwork) Peers() []peer.ID             { return n.peers }
func (fakeNetwork) Peerstore() peerstore.Peerstore { return fakePeerstore{} }

type fakePeerstore struct{ peerstore.Peerstore }

func (fakePeerstore) Addrs(peer.ID) []ma.Multiaddr {
	return []ma.Multiaddr{ma.StringCast("/ip4/192.0.2.1/tcp/4001")}
}

type failingRouting struct {
	routing.Routing
	err error
}

func (r failingRouting) Bootstrap(context.Context) error { return r.err }

// roundCounter counts calls to cfg.BootstrapPeers. Bootstrap calls it once up
// front for its warning, then each round with too few connections calls it
// once more. Each call takes delay, so a caller that does not wait for a call
// observes it as not yet completed.
type roundCounter struct {
	delay     time.Duration
	completed atomic.Int64
}

func (c *roundCounter) bootstrapPeers() []peer.AddrInfo {
	time.Sleep(c.delay)
	c.completed.Add(1)
	return nil
}

// rounds returns the number of completed rounds, excluding the warning call.
func (c *roundCounter) rounds() int {
	return int(c.completed.Load()) - 1
}

func newTestConfig(counter *roundCounter, withBackup bool) BootstrapConfig {
	cfg := DefaultBootstrapConfig
	cfg.Period = testPeriod
	cfg.BootstrapPeers = counter.bootstrapPeers
	if withBackup {
		load := func(context.Context) []peer.AddrInfo { return nil }
		save := func(context.Context, []peer.AddrInfo) {}
		cfg.SetBackupPeers(load, save)
	}
	return cfg
}

func startBootstrap(t *testing.T, h host.Host, cfg BootstrapConfig) {
	t.Helper()
	closer, err := Bootstrap("", h, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := closer.Close(); err != nil {
			t.Error(err)
		}
	})
}

var backupCases = []struct {
	name       string
	withBackup bool
}{
	{name: "without backup peers", withBackup: false},
	{name: "with backup peers", withBackup: true},
}

func TestBootstrapReturnsAfterFirstRound(t *testing.T) {
	for _, tt := range backupCases {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				counter := &roundCounter{delay: time.Second}
				startBootstrap(t, fakeHost{}, newTestConfig(counter, tt.withBackup))

				if got := counter.rounds(); got != 1 {
					t.Fatalf("Bootstrap returned after %d completed rounds, want 1", got)
				}
			})
		})
	}
}

func TestBootstrapRunsPeriodicRounds(t *testing.T) {
	const periods = 5
	for _, tt := range backupCases {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				counter := &roundCounter{}
				startBootstrap(t, fakeHost{}, newTestConfig(counter, tt.withBackup))

				// Sleep off the tick boundary so the count is exact.
				time.Sleep(periods*testPeriod + testPeriod/2)
				synctest.Wait()

				if got, want := counter.rounds(), 1+periods; got != want {
					t.Fatalf("got %d rounds over %d periods, want %d: the first round plus one per period", got, periods, want)
				}
			})
		})
	}
}

// TestBootstrapCloseStopsRounds relies on synctest.Test failing when a
// goroutine in the bubble is still blocked after the test function returns:
// a supervisor that outlives Close fails the test.
func TestBootstrapCloseStopsRounds(t *testing.T) {
	for _, tt := range backupCases {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				counter := &roundCounter{}
				closer, err := Bootstrap("", fakeHost{}, nil, newTestConfig(counter, tt.withBackup))
				if err != nil {
					t.Fatal(err)
				}

				time.Sleep(testPeriod + testPeriod/2)
				synctest.Wait()
				if err := closer.Close(); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				roundsAtClose := counter.rounds()

				time.Sleep(5 * testPeriod)
				synctest.Wait()

				if ran := counter.rounds() - roundsAtClose; ran != 0 {
					t.Fatalf("%d rounds ran after Close", ran)
				}
			})
		})
	}
}

func TestBootstrapRoutingErrorStopsSupervisor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wantErr := errors.New("routing bootstrap failed")
		counter := &roundCounter{}
		rt := failingRouting{err: wantErr}

		closer, err := Bootstrap("", fakeHost{}, rt, newTestConfig(counter, false))
		if !errors.Is(err, wantErr) {
			t.Fatalf("got error %v, want %v", err, wantErr)
		}
		if closer != nil {
			t.Fatal("got a closer from a failed Bootstrap")
		}

		time.Sleep(5 * testPeriod)
		synctest.Wait()

		if got := counter.rounds(); got != 1 {
			t.Fatalf("got %d rounds, want 1: rounds ran after Bootstrap failed", got)
		}
	})
}

// TestBootstrapStartsBackupSaving checks that, with backup peers configured,
// Bootstrap still starts the process that saves connected peers as backups.
func TestBootstrapStartsBackupSaving(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		connected := test.RandPeerIDFatal(t)
		h := fakeHost{network: fakeNetwork{peers: []peer.ID{connected}}}

		var saved [][]peer.AddrInfo
		cfg := newTestConfig(&roundCounter{}, false)
		cfg.SetBackupPeers(
			func(context.Context) []peer.AddrInfo { return nil },
			func(_ context.Context, peers []peer.AddrInfo) { saved = append(saved, peers) },
		)

		startBootstrap(t, h, cfg)
		synctest.Wait()

		if len(saved) != 1 || len(saved[0]) != 1 || saved[0][0].ID != connected {
			t.Fatalf("saved %v, want a single save of %s", saved, connected)
		}
	})
}
