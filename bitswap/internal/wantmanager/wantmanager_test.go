package wantmanager

import (
	"context"
	"testing"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	bssim "github.com/ipfs/go-bitswap/internal/sessioninterestmanager"
	"github.com/ipfs/go-bitswap/internal/sessionmanager"
	"github.com/ipfs/go-bitswap/internal/testutil"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type fakePeerHandler struct {
	lastBcstWants []cid.Cid
	lastCancels   []cid.Cid
}

func (fph *fakePeerHandler) Connected(p peer.ID) {
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
