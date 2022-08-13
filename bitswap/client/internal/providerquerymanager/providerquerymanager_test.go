package providerquerymanager

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type fakeProviderNetwork struct {
	peersFound       []peer.ID
	connectError     error
	delay            time.Duration
	connectDelay     time.Duration
	queriesMadeMutex sync.RWMutex
	queriesMade      int
	liveQueries      int
}

func (fpn *fakeProviderNetwork) ConnectTo(context.Context, peer.ID) error {
	time.Sleep(fpn.connectDelay)
	return fpn.connectError
}

func (fpn *fakeProviderNetwork) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.ID {
	fpn.queriesMadeMutex.Lock()
	fpn.queriesMade++
	fpn.liveQueries++
	fpn.queriesMadeMutex.Unlock()
	incomingPeers := make(chan peer.ID)
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
			case incomingPeers <- p:
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

func TestNormalSimultaneousFetch(t *testing.T) {
	peers := testutil.GeneratePeers(10)
	fpn := &fakeProviderNetwork{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	ctx := context.Background()
	providerQueryManager := New(ctx, fpn)
	providerQueryManager.Startup()
	keys := testutil.GenerateCids(2)

	sessionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[0])
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[1])

	var firstPeersReceived []peer.ID
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.ID
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
	peers := testutil.GeneratePeers(10)
	fpn := &fakeProviderNetwork{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	ctx := context.Background()
	providerQueryManager := New(ctx, fpn)
	providerQueryManager.Startup()
	key := testutil.GenerateCids(1)[0]

	sessionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key)

	var firstPeersReceived []peer.ID
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.ID
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
	peers := testutil.GeneratePeers(10)
	fpn := &fakeProviderNetwork{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	ctx := context.Background()
	providerQueryManager := New(ctx, fpn)
	providerQueryManager.Startup()

	key := testutil.GenerateCids(1)[0]

	// first session will cancel before done
	firstSessionCtx, firstCancel := context.WithTimeout(ctx, 3*time.Millisecond)
	defer firstCancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(firstSessionCtx, key)
	secondSessionCtx, secondCancel := context.WithTimeout(ctx, 5*time.Second)
	defer secondCancel()
	secondRequestChan := providerQueryManager.FindProvidersAsync(secondSessionCtx, key)

	var firstPeersReceived []peer.ID
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.ID
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
	peers := testutil.GeneratePeers(10)
	fpn := &fakeProviderNetwork{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	ctx := context.Background()
	managerCtx, managerCancel := context.WithTimeout(ctx, 5*time.Millisecond)
	defer managerCancel()
	providerQueryManager := New(managerCtx, fpn)
	providerQueryManager.Startup()

	key := testutil.GenerateCids(1)[0]

	sessionCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key)

	var firstPeersReceived []peer.ID
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.ID
	for p := range secondRequestChan {
		secondPeersReceived = append(secondPeersReceived, p)
	}

	if len(firstPeersReceived) >= len(peers) ||
		len(secondPeersReceived) >= len(peers) {
		t.Fatal("Did not cancel requests in progress correctly")
	}
}

func TestPeersWithConnectionErrorsNotAddedToPeerList(t *testing.T) {
	peers := testutil.GeneratePeers(10)
	fpn := &fakeProviderNetwork{
		peersFound:   peers,
		connectError: errors.New("not able to connect"),
		delay:        1 * time.Millisecond,
	}
	ctx := context.Background()
	providerQueryManager := New(ctx, fpn)
	providerQueryManager.Startup()

	key := testutil.GenerateCids(1)[0]

	sessionCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key)

	var firstPeersReceived []peer.ID
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.ID
	for p := range secondRequestChan {
		secondPeersReceived = append(secondPeersReceived, p)
	}

	if len(firstPeersReceived) != 0 || len(secondPeersReceived) != 0 {
		t.Fatal("Did not filter out peers with connection issues")
	}

}

func TestRateLimitingRequests(t *testing.T) {
	peers := testutil.GeneratePeers(10)
	fpn := &fakeProviderNetwork{
		peersFound: peers,
		delay:      5 * time.Millisecond,
	}
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	providerQueryManager := New(ctx, fpn)
	providerQueryManager.Startup()

	keys := testutil.GenerateCids(maxInProcessRequests + 1)
	sessionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var requestChannels []<-chan peer.ID
	for i := 0; i < maxInProcessRequests+1; i++ {
		requestChannels = append(requestChannels, providerQueryManager.FindProvidersAsync(sessionCtx, keys[i]))
	}
	time.Sleep(20 * time.Millisecond)
	fpn.queriesMadeMutex.Lock()
	if fpn.liveQueries != maxInProcessRequests {
		t.Logf("Queries made: %d\n", fpn.liveQueries)
		t.Fatal("Did not limit parallel requests to rate limit")
	}
	fpn.queriesMadeMutex.Unlock()
	for i := 0; i < maxInProcessRequests+1; i++ {
		for range requestChannels[i] {
		}
	}

	fpn.queriesMadeMutex.Lock()
	defer fpn.queriesMadeMutex.Unlock()
	if fpn.queriesMade != maxInProcessRequests+1 {
		t.Logf("Queries made: %d\n", fpn.queriesMade)
		t.Fatal("Did not make all seperate requests")
	}
}

func TestFindProviderTimeout(t *testing.T) {
	peers := testutil.GeneratePeers(10)
	fpn := &fakeProviderNetwork{
		peersFound: peers,
		delay:      10 * time.Millisecond,
	}
	ctx := context.Background()
	providerQueryManager := New(ctx, fpn)
	providerQueryManager.Startup()
	providerQueryManager.SetFindProviderTimeout(2 * time.Millisecond)
	keys := testutil.GenerateCids(1)

	sessionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[0])
	var firstPeersReceived []peer.ID
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}
	if len(firstPeersReceived) >= len(peers) {
		t.Fatal("Find provider request should have timed out, did not")
	}
}

func TestFindProviderPreCanceled(t *testing.T) {
	peers := testutil.GeneratePeers(10)
	fpn := &fakeProviderNetwork{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	ctx := context.Background()
	providerQueryManager := New(ctx, fpn)
	providerQueryManager.Startup()
	providerQueryManager.SetFindProviderTimeout(100 * time.Millisecond)
	keys := testutil.GenerateCids(1)

	sessionCtx, cancel := context.WithCancel(ctx)
	cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[0])
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
	peers := testutil.GeneratePeers(2)
	fpn := &fakeProviderNetwork{
		peersFound: peers,
		delay:      1 * time.Millisecond,
	}
	ctx := context.Background()
	providerQueryManager := New(ctx, fpn)
	providerQueryManager.Startup()
	providerQueryManager.SetFindProviderTimeout(100 * time.Millisecond)
	keys := testutil.GenerateCids(1)

	sessionCtx, cancel := context.WithCancel(ctx)
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[0])
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
