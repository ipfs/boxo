package wantmanager

import (
	"context"
	"testing"

	bsbpm "github.com/ipfs/go-bitswap/blockpresencemanager"
	bssim "github.com/ipfs/go-bitswap/sessioninterestmanager"
	"github.com/ipfs/go-bitswap/sessionmanager"
	"github.com/ipfs/go-bitswap/testutil"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type fakePeerHandler struct {
	lastInitialWants []cid.Cid
	lastBcstWants    []cid.Cid
	lastCancels      []cid.Cid
}

func (fph *fakePeerHandler) Connected(p peer.ID, initialWants []cid.Cid) {
	fph.lastInitialWants = initialWants
}
func (fph *fakePeerHandler) Disconnected(p peer.ID) {

}
func (fph *fakePeerHandler) BroadcastWantHaves(ctx context.Context, wantHaves []cid.Cid) {
	fph.lastBcstWants = wantHaves
}
func (fph *fakePeerHandler) SendCancels(ctx context.Context, cancels []cid.Cid) {
	fph.lastCancels = cancels
}

type fakeSessionManager struct {
}

func (*fakeSessionManager) ReceiveFrom(p peer.ID, blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) []sessionmanager.Session {
	return nil
}

func TestInitialBroadcastWantsAddedCorrectly(t *testing.T) {
	ctx := context.Background()
	ph := &fakePeerHandler{}
	sim := bssim.New()
	bpm := bsbpm.New()
	wm := New(context.Background(), ph, sim, bpm)
	sm := &fakeSessionManager{}
	wm.SetSessionManager(sm)

	peers := testutil.GeneratePeers(3)

	// Connect peer 0. Should not receive anything yet.
	wm.Connected(peers[0])
	if len(ph.lastInitialWants) != 0 {
		t.Fatal("expected no initial wants")
	}

	// Broadcast 2 wants
	wantHaves := testutil.GenerateCids(2)
	wm.BroadcastWantHaves(ctx, 1, wantHaves)
	if len(ph.lastBcstWants) != 2 {
		t.Fatal("expected broadcast wants")
	}

	// Connect peer 1. Should receive all wants broadcast so far.
	wm.Connected(peers[1])
	if len(ph.lastInitialWants) != 2 {
		t.Fatal("expected broadcast wants")
	}

	// Broadcast 3 more wants
	wantHaves2 := testutil.GenerateCids(3)
	wm.BroadcastWantHaves(ctx, 2, wantHaves2)
	if len(ph.lastBcstWants) != 3 {
		t.Fatal("expected broadcast wants")
	}

	// Connect peer 2. Should receive all wants broadcast so far.
	wm.Connected(peers[2])
	if len(ph.lastInitialWants) != 5 {
		t.Fatal("expected all wants to be broadcast")
	}
}

func TestReceiveFromRemovesBroadcastWants(t *testing.T) {
	ctx := context.Background()
	ph := &fakePeerHandler{}
	sim := bssim.New()
	bpm := bsbpm.New()
	wm := New(context.Background(), ph, sim, bpm)
	sm := &fakeSessionManager{}
	wm.SetSessionManager(sm)

	peers := testutil.GeneratePeers(3)

	// Broadcast 2 wants
	cids := testutil.GenerateCids(2)
	wm.BroadcastWantHaves(ctx, 1, cids)
	if len(ph.lastBcstWants) != 2 {
		t.Fatal("expected broadcast wants")
	}

	// Connect peer 0. Should receive all wants.
	wm.Connected(peers[0])
	if len(ph.lastInitialWants) != 2 {
		t.Fatal("expected broadcast wants")
	}

	// Receive block for first want
	ks := cids[0:1]
	haves := []cid.Cid{}
	dontHaves := []cid.Cid{}
	wm.ReceiveFrom(ctx, peers[1], ks, haves, dontHaves)

	// Connect peer 2. Should get remaining want (the one that the block has
	// not yet been received for).
	wm.Connected(peers[2])
	if len(ph.lastInitialWants) != 1 {
		t.Fatal("expected remaining wants")
	}
}

func TestRemoveSessionRemovesBroadcastWants(t *testing.T) {
	ctx := context.Background()
	ph := &fakePeerHandler{}
	sim := bssim.New()
	bpm := bsbpm.New()
	wm := New(context.Background(), ph, sim, bpm)
	sm := &fakeSessionManager{}
	wm.SetSessionManager(sm)

	peers := testutil.GeneratePeers(2)

	// Broadcast 2 wants for session 0 and 2 wants for session 1
	ses0 := uint64(0)
	ses1 := uint64(1)
	ses0wants := testutil.GenerateCids(2)
	ses1wants := testutil.GenerateCids(2)
	wm.BroadcastWantHaves(ctx, ses0, ses0wants)
	wm.BroadcastWantHaves(ctx, ses1, ses1wants)

	// Connect peer 0. Should receive all wants.
	wm.Connected(peers[0])
	if len(ph.lastInitialWants) != 4 {
		t.Fatal("expected broadcast wants")
	}

	// Remove session 0
	wm.RemoveSession(ctx, ses0)

	// Connect peer 1. Should receive all wants from session that has not been
	// removed.
	wm.Connected(peers[1])
	if len(ph.lastInitialWants) != 2 {
		t.Fatal("expected broadcast wants")
	}
}

func TestReceiveFrom(t *testing.T) {
	ctx := context.Background()
	ph := &fakePeerHandler{}
	sim := bssim.New()
	bpm := bsbpm.New()
	wm := New(context.Background(), ph, sim, bpm)
	sm := &fakeSessionManager{}
	wm.SetSessionManager(sm)

	p := testutil.GeneratePeers(1)[0]
	ks := testutil.GenerateCids(2)
	haves := testutil.GenerateCids(2)
	dontHaves := testutil.GenerateCids(2)
	wm.ReceiveFrom(ctx, p, ks, haves, dontHaves)

	if !bpm.PeerHasBlock(p, haves[0]) {
		t.Fatal("expected block presence manager to be invoked")
	}
	if !bpm.PeerDoesNotHaveBlock(p, dontHaves[0]) {
		t.Fatal("expected block presence manager to be invoked")
	}
	if len(ph.lastCancels) != len(ks) {
		t.Fatal("expected received blocks to be cancelled")
	}
}

func TestRemoveSession(t *testing.T) {
	ctx := context.Background()
	ph := &fakePeerHandler{}
	sim := bssim.New()
	bpm := bsbpm.New()
	wm := New(context.Background(), ph, sim, bpm)
	sm := &fakeSessionManager{}
	wm.SetSessionManager(sm)

	// Record session interest in 2 keys for session 0 and 2 keys for session 1
	// with 1 overlapping key
	cids := testutil.GenerateCids(3)
	ses0 := uint64(0)
	ses1 := uint64(1)
	ses0ks := cids[:2]
	ses1ks := cids[1:]
	sim.RecordSessionInterest(ses0, ses0ks)
	sim.RecordSessionInterest(ses1, ses1ks)

	// Receive HAVE for all keys
	p := testutil.GeneratePeers(1)[0]
	ks := []cid.Cid{}
	haves := append(ses0ks, ses1ks...)
	dontHaves := []cid.Cid{}
	wm.ReceiveFrom(ctx, p, ks, haves, dontHaves)

	// Remove session 0
	wm.RemoveSession(ctx, ses0)

	// Expect session 0 interest to be removed and session 1 interest to be
	// unchanged
	if len(sim.FilterSessionInterested(ses0, ses0ks)[0]) != 0 {
		t.Fatal("expected session 0 interest to be removed")
	}
	if len(sim.FilterSessionInterested(ses1, ses1ks)[0]) != len(ses1ks) {
		t.Fatal("expected session 1 interest to be unchanged")
	}

	// Should clear block presence for key that was in session 0 and not
	// in session 1
	if bpm.PeerHasBlock(p, ses0ks[0]) {
		t.Fatal("expected block presence manager to be cleared")
	}
	if !bpm.PeerHasBlock(p, ses0ks[1]) {
		t.Fatal("expected block presence manager to be unchanged for overlapping key")
	}

	// Should cancel key that was in session 0 and not session 1
	if len(ph.lastCancels) != 1 || !ph.lastCancels[0].Equals(cids[0]) {
		t.Fatal("expected removed want-have to be cancelled")
	}
}
