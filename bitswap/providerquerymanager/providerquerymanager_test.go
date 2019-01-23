package providerquerymanager

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-peer"
)

type fakeProviderNetwork struct {
	peersFound   []peer.ID
	connectError error
	delay        time.Duration
	connectDelay time.Duration
	queriesMade  int
}

func (fpn *fakeProviderNetwork) ConnectTo(context.Context, peer.ID) error {
	time.Sleep(fpn.connectDelay)
	return fpn.connectError
}

func (fpn *fakeProviderNetwork) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.ID {
	fpn.queriesMade++
	incomingPeers := make(chan peer.ID)
	go func() {
		defer close(incomingPeers)
		for _, p := range fpn.peersFound {
			time.Sleep(fpn.delay)
			select {
			case incomingPeers <- p:
			case <-ctx.Done():
				return
			}
		}
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
	sessionID1 := testutil.GenerateSessionID()
	sessionID2 := testutil.GenerateSessionID()

	sessionCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[0], sessionID1)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, keys[1], sessionID2)

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
	sessionID1 := testutil.GenerateSessionID()
	sessionID2 := testutil.GenerateSessionID()

	sessionCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, sessionID1)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, sessionID2)

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
	sessionID1 := testutil.GenerateSessionID()
	sessionID2 := testutil.GenerateSessionID()

	// first session will cancel before done
	firstSessionCtx, firstCancel := context.WithTimeout(ctx, 3*time.Millisecond)
	defer firstCancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(firstSessionCtx, key, sessionID1)
	secondSessionCtx, secondCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer secondCancel()
	secondRequestChan := providerQueryManager.FindProvidersAsync(secondSessionCtx, key, sessionID2)

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
	sessionID1 := testutil.GenerateSessionID()
	sessionID2 := testutil.GenerateSessionID()

	sessionCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, sessionID1)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, sessionID2)

	var firstPeersReceived []peer.ID
	for p := range firstRequestChan {
		firstPeersReceived = append(firstPeersReceived, p)
	}

	var secondPeersReceived []peer.ID
	for p := range secondRequestChan {
		secondPeersReceived = append(secondPeersReceived, p)
	}

	if len(firstPeersReceived) <= 0 ||
		len(firstPeersReceived) >= len(peers) ||
		len(secondPeersReceived) <= 0 ||
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
	sessionID1 := testutil.GenerateSessionID()
	sessionID2 := testutil.GenerateSessionID()

	sessionCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	firstRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, sessionID1)
	secondRequestChan := providerQueryManager.FindProvidersAsync(sessionCtx, key, sessionID2)

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
		delay:      1 * time.Millisecond,
	}
	ctx := context.Background()
	providerQueryManager := New(ctx, fpn)
	providerQueryManager.Startup()

	keys := testutil.GenerateCids(maxInProcessRequests + 1)
	sessionID := testutil.GenerateSessionID()
	sessionCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	var requestChannels []<-chan peer.ID
	for i := 0; i < maxInProcessRequests+1; i++ {
		requestChannels = append(requestChannels, providerQueryManager.FindProvidersAsync(sessionCtx, keys[i], sessionID))
	}
	time.Sleep(2 * time.Millisecond)
	if fpn.queriesMade != maxInProcessRequests {
		t.Fatal("Did not limit parallel requests to rate limit")
	}
	for i := 0; i < maxInProcessRequests+1; i++ {
		for range requestChannels[i] {
		}
	}

	if fpn.queriesMade != maxInProcessRequests+1 {
		t.Fatal("Did not make all seperate requests")
	}
}
