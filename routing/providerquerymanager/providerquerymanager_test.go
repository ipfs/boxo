package providerquerymanager

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type fakeProviderDialer struct {
	connectError error
	connectDelay time.Duration
}

type fakeProviderDiscovery struct {
	peersFound       []peer.ID
	delay            time.Duration
	queriesMadeMutex sync.RWMutex
	queriesMade      int
	liveQueries      int
}

func (fpd *fakeProviderDialer) Connect(context.Context, peer.AddrInfo) error {
	time.Sleep(fpd.connectDelay)
	return fpd.connectError
}

func (fpn *fakeProviderDiscovery) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.AddrInfo {
	fpn.queriesMadeMutex.Lock()
	fpn.queriesMade++
	fpn.liveQueries++
	fpn.queriesMadeMutex.Unlock()
	incomingPeers := make(chan peer.AddrInfo)
	go func() {
		defer close(incomingPeers)
		for _, p := range fpn.peersFound {
			time.Sleep(fpn.delay)
			select {
			case <-ctx.Done():
				return
			default:
			}
			select {
			case incomingPeers <- peer.AddrInfo{ID: p}:
			case <-ctx.Done():
				return
			}
		}
		fpn.queriesMadeMutex.Lock()
		fpn.liveQueries--
		fpn.queriesMadeMutex.Unlock()
	}()

	return incomingPeers
}

func mustNotErr[T any](out T, err error) T {
	if err != nil {
		panic(err)
	}
	return out
}

func TestNormalSimultaneousFetch(t *testing.T) {
	peers := random.Peers(10)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn))
	defer providerQueryManager.Close()
	keys := random.Cids(2)

	sessionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[0], 0)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[1], 0)

	var firstPeersReceived []peer.AddrInfo
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.AddrInfo
	for p := range secondRequestChan {
		secondPeersReceived = append(secondPeersReceived, p)
	}

	if len(firstPeersReceived) != len(peers) || len(secondPeersReceived) != len(peers) {
		t.Fatal("Did not collect all peers for request that was completed")
	}

	fpn.queriesMadeMutex.Lock()
	defer fpn.queriesMadeMutex.Unlock()
	if fpn.queriesMade != 2 {
		t.Fatal("Did not dedup provider requests running simultaneously")
	}
}

func TestDedupingProviderRequests(t *testing.T) {
	peers := random.Peers(10)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn))
	defer providerQueryManager.Close()
	key := random.Cids(1)[0]

	sessionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, 0)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, 0)

	var firstPeersReceived []peer.AddrInfo
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.AddrInfo
	for p := range secondRequestChan {
		secondPeersReceived = append(secondPeersReceived, p)
	}

	if len(firstPeersReceived) != len(peers) || len(secondPeersReceived) != len(peers) {
		t.Fatal("Did not collect all peers for request that was completed")
	}

	if !reflect.DeepEqual(firstPeersReceived, secondPeersReceived) {
		t.Fatal("Did not receive the same response to both find provider requests")
	}
	fpn.queriesMadeMutex.Lock()
	defer fpn.queriesMadeMutex.Unlock()
	if fpn.queriesMade != 1 {
		t.Fatal("Did not dedup provider requests running simultaneously")
	}
}

func TestCancelOneRequestDoesNotTerminateAnother(t *testing.T) {
	peers := random.Peers(10)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn))
	defer providerQueryManager.Close()

	key := random.Cids(1)[0]

	// first session will cancel before done
	ctx := context.Background()
	firstSessionCtx, firstCancel := context.WithTimeout(ctx, 3*time.Millisecond)
	defer firstCancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(firstSessionCtx, key, 0)
	secondSessionCtx, secondCancel := context.WithTimeout(ctx, 5*time.Second)
	defer secondCancel()
	secondRequestChan := providerQueryManager.FindProvidersAsync(secondSessionCtx, key, 0)

	var firstPeersReceived []peer.AddrInfo
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.AddrInfo
	for p := range secondRequestChan {
		secondPeersReceived = append(secondPeersReceived, p)
	}

	if len(secondPeersReceived) != len(peers) {
		t.Fatal("Did not collect all peers for request that was completed")
	}

	if len(firstPeersReceived) >= len(peers) {
		t.Fatal("Collected all peers on cancelled peer, should have been cancelled immediately")
	}
	fpn.queriesMadeMutex.Lock()
	defer fpn.queriesMadeMutex.Unlock()
	if fpn.queriesMade != 1 {
		t.Fatal("Did not dedup provider requests running simultaneously")
	}
}

func TestCancelManagerExitsGracefully(t *testing.T) {
	peers := random.Peers(10)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn))
	defer providerQueryManager.Close()
	time.AfterFunc(5*time.Millisecond, providerQueryManager.Close)

	key := random.Cids(1)[0]

	sessionCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, 0)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, 0)

	var firstPeersReceived []peer.AddrInfo
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.AddrInfo
	for p := range secondRequestChan {
		secondPeersReceived = append(secondPeersReceived, p)
	}

	if len(firstPeersReceived) >= len(peers) ||
		len(secondPeersReceived) >= len(peers) {
		t.Fatal("Did not cancel requests in progress correctly")
	}
}

func TestPeersWithConnectionErrorsNotAddedToPeerList(t *testing.T) {
	peers := random.Peers(10)
	fpd := &fakeProviderDialer{
		connectError: errors.New("not able to connect"),
	}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn))
	defer providerQueryManager.Close()

	key := random.Cids(1)[0]

	sessionCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, 0)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, 0)

	var firstPeersReceived []peer.AddrInfo
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.AddrInfo
	for p := range secondRequestChan {
		secondPeersReceived = append(secondPeersReceived, p)
	}

	if len(firstPeersReceived) != 0 || len(secondPeersReceived) != 0 {
		t.Fatal("Did not filter out peers with connection issues")
	}
}

func TestRateLimitingRequests(t *testing.T) {
	const maxInProcessRequests = 6

	peers := random.Peers(10)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      5 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn, WithMaxInProcessRequests(maxInProcessRequests)))
	defer providerQueryManager.Close()

	keys := random.Cids(maxInProcessRequests + 1)
	sessionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var requestChannels []<-chan peer.AddrInfo
	for i := range maxInProcessRequests + 1 {
		requestChannels = append(requestChannels, providerQueryManager.FindProvidersAsync(sessionCtx, keys[i], 0))
	}
	time.Sleep(20 * time.Millisecond)
	fpn.queriesMadeMutex.Lock()
	if fpn.liveQueries != maxInProcessRequests {
		t.Logf("Queries made: %d\n", fpn.liveQueries)
		t.Fatal("Did not limit parallel requests to rate limit")
	}
	fpn.queriesMadeMutex.Unlock()
	for i := range maxInProcessRequests + 1 {
		for range requestChannels[i] {
		}
	}

	fpn.queriesMadeMutex.Lock()
	defer fpn.queriesMadeMutex.Unlock()
	if fpn.queriesMade != maxInProcessRequests+1 {
		t.Logf("Queries made: %d\n", fpn.queriesMade)
		t.Fatal("Did not make all separate requests")
	}
}

func TestUnlimitedRequests(t *testing.T) {
	const inProcessRequests = 11

	peers := random.Peers(10)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      5 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn, WithMaxInProcessRequests(0)))
	defer providerQueryManager.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	keys := random.Cids(inProcessRequests)
	sessionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var requestChannels []<-chan peer.AddrInfo
	for i := range inProcessRequests {
		requestChannels = append(requestChannels, providerQueryManager.FindProvidersAsync(sessionCtx, keys[i], 0))
	}
	time.Sleep(20 * time.Millisecond)
	fpn.queriesMadeMutex.Lock()
	if fpn.liveQueries != inProcessRequests {
		t.Logf("Queries made: %d\n", fpn.liveQueries)
		t.Fatal("Parallel requests appear to be rate limited")
	}
	fpn.queriesMadeMutex.Unlock()
	for i := range inProcessRequests {
		for range requestChannels[i] {
		}
	}

	fpn.queriesMadeMutex.Lock()
	defer fpn.queriesMadeMutex.Unlock()
	if fpn.queriesMade != inProcessRequests {
		t.Logf("Queries made: %d\n", fpn.queriesMade)
		t.Fatal("Did not make all separate requests")
	}
}

func TestFindProviderTimeout(t *testing.T) {
	peers := random.Peers(10)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      10 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn, WithMaxTimeout(2*time.Millisecond)))
	defer providerQueryManager.Close()
	keys := random.Cids(1)

	sessionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[0], 0)
	var firstPeersReceived []peer.AddrInfo
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}
	if len(firstPeersReceived) >= len(peers) {
		t.Fatal("Find provider request should have timed out, did not")
	}
}

func TestFindProviderPreCanceled(t *testing.T) {
	peers := random.Peers(10)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn, WithMaxTimeout(100*time.Millisecond)))
	defer providerQueryManager.Close()
	keys := random.Cids(1)

	sessionCtx, cancel := context.WithCancel(context.Background())
	cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[0], 0)
	if firstRequestChan == nil {
		t.Fatal("expected non-nil channel")
	}
	select {
	case <-firstRequestChan:
	case <-time.After(10 * time.Millisecond):
		t.Fatal("shouldn't have blocked waiting on a closed context")
	}
}

func TestCancelFindProvidersAfterCompletion(t *testing.T) {
	peers := random.Peers(2)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn, WithMaxTimeout(100*time.Millisecond)))
	defer providerQueryManager.Close()
	keys := random.Cids(1)

	sessionCtx, cancel := context.WithCancel(context.Background())
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[0], 0)
	<-firstRequestChan                // wait for everything to start.
	time.Sleep(10 * time.Millisecond) // wait for the incoming providres to stop.
	cancel()                          // cancel the context.

	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-firstRequestChan:
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatal("should have finished receiving responses within timeout")
		}
	}
}

func TestLimitedProviders(t *testing.T) {
	max := 5
	peers := random.Peers(10)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	providerQueryManager := mustNotErr(New(fpd, fpn, WithMaxProviders(max), WithMaxTimeout(100*time.Millisecond)))
	defer providerQueryManager.Close()
	keys := random.Cids(1)

	providersChan := providerQueryManager.FindProvidersAsync(context.Background(), keys[0], 0)
	total := 0
	for range providersChan {
		total++
	}
	if total != max {
		t.Fatal("returned more providers than requested")
	}
}

func TestIgnorePeers(t *testing.T) {
	peers := random.Peers(5)
	fpd := &fakeProviderDialer{}
	fpn := &fakeProviderDiscovery{
		peersFound: peers,
	}

	providerQueryManager := mustNotErr(New(fpd, fpn,
		WithIgnoreProviders(peers[0:4]...),
	))
	defer providerQueryManager.Close()
	keys := random.Cids(1)

	providersChan := providerQueryManager.FindProvidersAsync(context.Background(), keys[0], 0)
	total := 0
	for range providersChan {
		total++
	}
	if total != 1 {
		t.Fatal("did not ignore 4 of the peers")
	}
}

// callCountingDialer fails the first Connect call for each target peer and
// succeeds on subsequent ones. Used to exercise the WithFindPeerFallback retry.
type callCountingDialer struct {
	mu        sync.Mutex
	callsByID map[peer.ID]int
}

func (d *callCountingDialer) Connect(_ context.Context, p peer.AddrInfo) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.callsByID == nil {
		d.callsByID = map[peer.ID]int{}
	}
	d.callsByID[p.ID]++
	if d.callsByID[p.ID] == 1 {
		return errors.New("first attempt fails")
	}
	return nil
}

func (d *callCountingDialer) callCount(id peer.ID) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.callsByID[id]
}

// fakePeerRouter satisfies ProviderQueryPeerRouter for tests. If findErr is
// non-nil it is returned and findResult is ignored; otherwise findResult is
// returned (use Addrs: nil to simulate "router knows the peer but has no
// addresses").
type fakePeerRouter struct {
	mu         sync.Mutex
	findResult peer.AddrInfo
	findErr    error
	calls      int
}

func (r *fakePeerRouter) FindPeer(_ context.Context, _ peer.ID) (peer.AddrInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.findErr != nil {
		return peer.AddrInfo{}, r.findErr
	}
	return r.findResult, nil
}

func (r *fakePeerRouter) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// fakeProviderDiscoveryWithAddrs surfaces every peer in peersFound with the
// given addrs attached. Lets tests control what AddrInfo the manager hands
// the dialer on the initial attempt, which matters for the
// has-new-addresses check in WithFindPeerFallback.
type fakeProviderDiscoveryWithAddrs struct {
	peersFound []peer.ID
	addrs      []ma.Multiaddr
}

func (f *fakeProviderDiscoveryWithAddrs) FindProvidersAsync(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	go func() {
		defer close(out)
		for _, p := range f.peersFound {
			select {
			case <-ctx.Done():
				return
			case out <- peer.AddrInfo{ID: p, Addrs: append([]ma.Multiaddr(nil), f.addrs...)}:
			}
		}
	}()
	return out
}

func collect(ch <-chan peer.AddrInfo) []peer.AddrInfo {
	var out []peer.AddrInfo
	for ai := range ch {
		out = append(out, ai)
	}
	return out
}

func TestFindPeerFallbackRescuesFailedDial(t *testing.T) {
	peers := random.Peers(1)
	p := peers[0]
	freshAddr := ma.StringCast("/ip4/198.51.100.7/tcp/4001")

	dialer := &callCountingDialer{}
	discovery := &fakeProviderDiscovery{peersFound: peers}
	router := &fakePeerRouter{
		findResult: peer.AddrInfo{ID: p, Addrs: []ma.Multiaddr{freshAddr}},
	}

	pqm := mustNotErr(New(dialer, discovery, WithFindPeerFallback(router)))
	defer pqm.Close()

	sessionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	received := collect(pqm.FindProvidersAsync(sessionCtx, random.Cids(1)[0], 0))

	if len(received) != 1 {
		t.Fatalf("expected 1 provider after retry, got %d", len(received))
	}
	if c := dialer.callCount(p); c != 2 {
		t.Errorf("expected 2 Connect calls for %s (initial + retry), got %d", p, c)
	}
	if c := router.callCount(); c != 1 {
		t.Errorf("expected exactly 1 FindPeer call, got %d", c)
	}
}

func TestFindPeerFallbackSkippedWhenNoAddrs(t *testing.T) {
	peers := random.Peers(1)
	p := peers[0]

	dialer := &callCountingDialer{}
	discovery := &fakeProviderDiscovery{peersFound: peers}
	router := &fakePeerRouter{
		findResult: peer.AddrInfo{ID: p, Addrs: nil},
	}

	pqm := mustNotErr(New(dialer, discovery, WithFindPeerFallback(router)))
	defer pqm.Close()

	sessionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	received := collect(pqm.FindProvidersAsync(sessionCtx, random.Cids(1)[0], 0))

	if len(received) != 0 {
		t.Fatalf("expected 0 providers, got %d", len(received))
	}
	if c := dialer.callCount(p); c != 1 {
		t.Errorf("expected exactly 1 Connect call (no retry), got %d", c)
	}
	if c := router.callCount(); c != 1 {
		t.Errorf("expected exactly 1 FindPeer call, got %d", c)
	}
}

func TestFindPeerFallbackSkippedWhenErrors(t *testing.T) {
	peers := random.Peers(1)
	p := peers[0]

	dialer := &callCountingDialer{}
	discovery := &fakeProviderDiscovery{peersFound: peers}
	router := &fakePeerRouter{findErr: errors.New("routing: not found")}

	pqm := mustNotErr(New(dialer, discovery, WithFindPeerFallback(router)))
	defer pqm.Close()

	sessionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	received := collect(pqm.FindProvidersAsync(sessionCtx, random.Cids(1)[0], 0))

	if len(received) != 0 {
		t.Fatalf("expected 0 providers, got %d", len(received))
	}
	if c := dialer.callCount(p); c != 1 {
		t.Errorf("expected exactly 1 Connect call (no retry), got %d", c)
	}
}

// TestFindPeerFallbackSkippedWhenNoNewAddrs verifies the
// hasNewAddrs gate: if FindPeer returns only addresses that were already in
// the routing-record AddrInfo just tried, we don't retry (it would just
// dial the same broken set again).
func TestFindPeerFallbackSkippedWhenNoNewAddrs(t *testing.T) {
	peers := random.Peers(1)
	p := peers[0]
	knownAddr := ma.StringCast("/ip4/198.51.100.7/tcp/4001")

	dialer := &callCountingDialer{}
	// Provider record already carries knownAddr.
	discovery := &fakeProviderDiscoveryWithAddrs{
		peersFound: peers,
		addrs:      []ma.Multiaddr{knownAddr},
	}
	// FindPeer returns the same addr (plus nothing new). Retry must be skipped.
	router := &fakePeerRouter{
		findResult: peer.AddrInfo{ID: p, Addrs: []ma.Multiaddr{knownAddr}},
	}

	pqm := mustNotErr(New(dialer, discovery, WithFindPeerFallback(router)))
	defer pqm.Close()

	sessionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	received := collect(pqm.FindProvidersAsync(sessionCtx, random.Cids(1)[0], 0))

	if len(received) != 0 {
		t.Fatalf("expected 0 providers (retry skipped, dial stays failed), got %d", len(received))
	}
	if c := dialer.callCount(p); c != 1 {
		t.Errorf("expected exactly 1 Connect call (no retry on duplicate addrs), got %d", c)
	}
	if c := router.callCount(); c != 1 {
		t.Errorf("expected exactly 1 FindPeer call, got %d", c)
	}
}

func TestHasNewAddrs(t *testing.T) {
	a := ma.StringCast("/ip4/198.51.100.1/tcp/4001")
	b := ma.StringCast("/ip4/198.51.100.2/tcp/4001")
	c := ma.StringCast("/ip4/198.51.100.3/tcp/4001")

	cases := []struct {
		name      string
		candidate []ma.Multiaddr
		existing  []ma.Multiaddr
		want      bool
	}{
		{name: "empty candidate", candidate: nil, existing: []ma.Multiaddr{a}, want: false},
		{name: "empty existing", candidate: []ma.Multiaddr{a}, existing: nil, want: true},
		{name: "candidate is subset", candidate: []ma.Multiaddr{a}, existing: []ma.Multiaddr{a, b}, want: false},
		{name: "identical sets", candidate: []ma.Multiaddr{a, b}, existing: []ma.Multiaddr{a, b}, want: false},
		{name: "one new addr", candidate: []ma.Multiaddr{a, c}, existing: []ma.Multiaddr{a, b}, want: true},
		{name: "all new addrs", candidate: []ma.Multiaddr{c}, existing: []ma.Multiaddr{a, b}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasNewAddrs(tc.candidate, tc.existing); got != tc.want {
				t.Errorf("hasNewAddrs() = %v, want %v", got, tc.want)
			}
		})
	}
}
