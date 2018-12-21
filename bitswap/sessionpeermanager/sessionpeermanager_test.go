package sessionpeermanager

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type fakePeerProviderFinder struct {
	peers     []peer.ID
	completed chan struct{}
}

func (fppf *fakePeerProviderFinder) FindProvidersAsync(ctx context.Context, c cid.Cid) <-chan peer.ID {
	peerCh := make(chan peer.ID)
	go func() {

		for _, p := range fppf.peers {
			select {
			case peerCh <- p:
			case <-ctx.Done():
				close(peerCh)
				return
			}
		}
		close(peerCh)

		select {
		case fppf.completed <- struct{}{}:
		case <-ctx.Done():
		}
	}()
	return peerCh
}

type fakePeerTagger struct {
	lk          sync.Mutex
	taggedPeers []peer.ID
	wait        sync.WaitGroup
}

func (fpt *fakePeerTagger) TagPeer(p peer.ID, tag string, n int) {
	fpt.wait.Add(1)

	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	fpt.taggedPeers = append(fpt.taggedPeers, p)
}

func (fpt *fakePeerTagger) UntagPeer(p peer.ID, tag string) {
	defer fpt.wait.Done()

	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	for i := 0; i < len(fpt.taggedPeers); i++ {
		if fpt.taggedPeers[i] == p {
			fpt.taggedPeers[i] = fpt.taggedPeers[len(fpt.taggedPeers)-1]
			fpt.taggedPeers = fpt.taggedPeers[:len(fpt.taggedPeers)-1]
			return
		}
	}
}

func (fpt *fakePeerTagger) count() int {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	return len(fpt.taggedPeers)
}

func getPeers(sessionPeerManager *SessionPeerManager) []peer.ID {
	optimizedPeers := sessionPeerManager.GetOptimizedPeers()
	var peers []peer.ID
	for _, optimizedPeer := range optimizedPeers {
		peers = append(peers, optimizedPeer.Peer)
	}
	return peers
}

func TestFindingMorePeers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	completed := make(chan struct{})

	peers := testutil.GeneratePeers(5)
	fpt := &fakePeerTagger{}
	fppf := &fakePeerProviderFinder{peers, completed}
	c := testutil.GenerateCids(1)[0]
	id := testutil.GenerateSessionID()

	sessionPeerManager := New(ctx, id, fpt, fppf)

	findCtx, findCancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer findCancel()
	sessionPeerManager.FindMorePeers(ctx, c)
	select {
	case <-completed:
	case <-findCtx.Done():
		t.Fatal("Did not finish finding providers")
	}
	time.Sleep(2 * time.Millisecond)

	sessionPeers := getPeers(sessionPeerManager)
	if len(sessionPeers) != len(peers) {
		t.Fatal("incorrect number of peers found")
	}
	for _, p := range sessionPeers {
		if !testutil.ContainsPeer(peers, p) {
			t.Fatal("incorrect peer found through finding providers")
		}
	}
	if len(fpt.taggedPeers) != len(peers) {
		t.Fatal("Peers were not tagged!")
	}
}

func TestRecordingReceivedBlocks(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	fpt := &fakePeerTagger{}
	fppf := &fakePeerProviderFinder{}
	c := testutil.GenerateCids(1)[0]
	id := testutil.GenerateSessionID()

	sessionPeerManager := New(ctx, id, fpt, fppf)
	sessionPeerManager.RecordPeerResponse(p, c)
	time.Sleep(10 * time.Millisecond)
	sessionPeers := getPeers(sessionPeerManager)
	if len(sessionPeers) != 1 {
		t.Fatal("did not add peer on receive")
	}
	if sessionPeers[0] != p {
		t.Fatal("incorrect peer added on receive")
	}
	if len(fpt.taggedPeers) != 1 {
		t.Fatal("Peers was not tagged!")
	}
}

func TestOrderingPeers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	peers := testutil.GeneratePeers(100)
	completed := make(chan struct{})
	fpt := &fakePeerTagger{}
	fppf := &fakePeerProviderFinder{peers, completed}
	c := testutil.GenerateCids(1)
	id := testutil.GenerateSessionID()
	sessionPeerManager := New(ctx, id, fpt, fppf)

	// add all peers to session
	sessionPeerManager.FindMorePeers(ctx, c[0])
	select {
	case <-completed:
	case <-ctx.Done():
		t.Fatal("Did not finish finding providers")
	}
	time.Sleep(2 * time.Millisecond)

	// record broadcast
	sessionPeerManager.RecordPeerRequests(nil, c)

	// record receives
	peer1 := peers[rand.Intn(100)]
	peer2 := peers[rand.Intn(100)]
	peer3 := peers[rand.Intn(100)]
	time.Sleep(1 * time.Millisecond)
	sessionPeerManager.RecordPeerResponse(peer1, c[0])
	time.Sleep(5 * time.Millisecond)
	sessionPeerManager.RecordPeerResponse(peer2, c[0])
	time.Sleep(1 * time.Millisecond)
	sessionPeerManager.RecordPeerResponse(peer3, c[0])

	sessionPeers := sessionPeerManager.GetOptimizedPeers()
	if len(sessionPeers) != maxOptimizedPeers {
		t.Fatal("Should not return more than the max of optimized peers")
	}

	// should prioritize peers which are fastest
	if (sessionPeers[0].Peer != peer1) || (sessionPeers[1].Peer != peer2) || (sessionPeers[2].Peer != peer3) {
		t.Fatal("Did not prioritize peers that received blocks")
	}

	// should give first peer rating of 1
	if sessionPeers[0].OptimizationRating < 1.0 {
		t.Fatal("Did not assign rating to best peer correctly")
	}

	// should give other optimized peers ratings between 0 & 1
	if (sessionPeers[1].OptimizationRating >= 1.0) || (sessionPeers[1].OptimizationRating <= 0.0) ||
		(sessionPeers[2].OptimizationRating >= 1.0) || (sessionPeers[2].OptimizationRating <= 0.0) {
		t.Fatal("Did not assign rating to other optimized peers correctly")
	}

	// should other peers rating of zero
	for i := 3; i < maxOptimizedPeers; i++ {
		if sessionPeers[i].OptimizationRating != 0.0 {
			t.Fatal("Did not assign rating to unoptimized peer correctly")
		}
	}

	c2 := testutil.GenerateCids(1)

	// Request again
	sessionPeerManager.RecordPeerRequests(nil, c2)

	// Receive a second time
	sessionPeerManager.RecordPeerResponse(peer3, c2[0])

	// call again
	nextSessionPeers := sessionPeerManager.GetOptimizedPeers()
	if len(nextSessionPeers) != maxOptimizedPeers {
		t.Fatal("Should not return more than the max of optimized peers")
	}

	// should sort by average latency
	if (nextSessionPeers[0].Peer != peer1) || (nextSessionPeers[1].Peer != peer3) ||
		(nextSessionPeers[2].Peer != peer2) {
		t.Fatal("Did not dedup peers which received multiple blocks")
	}

	// should randomize other peers
	totalSame := 0
	for i := 3; i < maxOptimizedPeers; i++ {
		if sessionPeers[i].Peer == nextSessionPeers[i].Peer {
			totalSame++
		}
	}
	if totalSame >= maxOptimizedPeers-3 {
		t.Fatal("should not return the same random peers each time")
	}
}

func TestUntaggingPeers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	peers := testutil.GeneratePeers(5)
	completed := make(chan struct{})
	fpt := &fakePeerTagger{}
	fppf := &fakePeerProviderFinder{peers, completed}
	c := testutil.GenerateCids(1)[0]
	id := testutil.GenerateSessionID()

	sessionPeerManager := New(ctx, id, fpt, fppf)

	sessionPeerManager.FindMorePeers(ctx, c)
	select {
	case <-completed:
	case <-ctx.Done():
		t.Fatal("Did not finish finding providers")
	}
	time.Sleep(2 * time.Millisecond)

	if fpt.count() != len(peers) {
		t.Fatal("Peers were not tagged!")
	}
	<-ctx.Done()
	fpt.wait.Wait()

	if fpt.count() != 0 {
		t.Fatal("Peers were not untagged!")
	}
}
