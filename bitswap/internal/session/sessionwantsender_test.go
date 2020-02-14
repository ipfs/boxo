package session

import (
	"context"
	"sync"
	"testing"
	"time"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	bspm "github.com/ipfs/go-bitswap/internal/peermanager"
	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type sentWants struct {
	p          peer.ID
	wantHaves  *cid.Set
	wantBlocks *cid.Set
}

type mockPeerManager struct {
	peerSessions sync.Map
	peerSends    sync.Map
}

func newMockPeerManager() *mockPeerManager {
	return &mockPeerManager{}
}

func (pm *mockPeerManager) RegisterSession(p peer.ID, sess bspm.Session) bool {
	pm.peerSessions.Store(p, sess)
	return true
}

func (pm *mockPeerManager) UnregisterSession(sesid uint64) {
}

func (pm *mockPeerManager) SendWants(ctx context.Context, p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	swi, _ := pm.peerSends.LoadOrStore(p, sentWants{p, cid.NewSet(), cid.NewSet()})
	sw := swi.(sentWants)
	for _, c := range wantBlocks {
		sw.wantBlocks.Add(c)
	}
	for _, c := range wantHaves {
		if !sw.wantBlocks.Has(c) {
			sw.wantHaves.Add(c)
		}
	}
}

func (pm *mockPeerManager) waitNextWants() map[peer.ID]sentWants {
	time.Sleep(5 * time.Millisecond)
	nw := make(map[peer.ID]sentWants)
	pm.peerSends.Range(func(k, v interface{}) bool {
		nw[k.(peer.ID)] = v.(sentWants)
		return true
	})
	return nw
}

func (pm *mockPeerManager) clearWants() {
	pm.peerSends.Range(func(k, v interface{}) bool {
		pm.peerSends.Delete(k)
		return true
	})
}

func TestSendWants(t *testing.T) {
	cids := testutil.GenerateCids(4)
	peers := testutil.GeneratePeers(1)
	peerA := peers[0]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	spm := newSessionWantSender(context.Background(), sid, pm, bpm, onSend, onPeersExhausted)

	go spm.Run()

	// add cid0, cid1
	blkCids0 := cids[0:2]
	spm.Add(blkCids0)
	// peerA: HAVE cid0
	spm.Update(peerA, []cid.Cid{}, []cid.Cid{cids[0]}, []cid.Cid{}, true)

	// Wait for processing to complete
	peerSends := pm.waitNextWants()

	// Should have sent
	// peerA: want-block cid0, cid1
	sw, ok := peerSends[peerA]
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), blkCids0) {
		t.Fatal("Wrong keys")
	}
	if sw.wantHaves.Len() > 0 {
		t.Fatal("Expecting no want-haves")
	}
}

func TestSendsWantBlockToOnePeerOnly(t *testing.T) {
	cids := testutil.GenerateCids(4)
	peers := testutil.GeneratePeers(2)
	peerA := peers[0]
	peerB := peers[1]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	spm := newSessionWantSender(context.Background(), sid, pm, bpm, onSend, onPeersExhausted)

	go spm.Run()

	// add cid0, cid1
	blkCids0 := cids[0:2]
	spm.Add(blkCids0)
	// peerA: HAVE cid0
	spm.Update(peerA, []cid.Cid{}, []cid.Cid{cids[0]}, []cid.Cid{}, true)

	// Wait for processing to complete
	peerSends := pm.waitNextWants()

	// Should have sent
	// peerA: want-block cid0, cid1
	sw, ok := peerSends[peerA]
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), blkCids0) {
		t.Fatal("Wrong keys")
	}

	// Clear wants (makes keeping track of what's been sent easier)
	pm.clearWants()

	// peerB: HAVE cid0
	spm.Update(peerB, []cid.Cid{}, []cid.Cid{cids[0]}, []cid.Cid{}, true)

	// Wait for processing to complete
	peerSends = pm.waitNextWants()

	// Have not received response from peerA, so should not send want-block to
	// peerB. Should have sent
	// peerB: want-have cid0, cid1
	sw, ok = peerSends[peerB]
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	if sw.wantBlocks.Len() > 0 {
		t.Fatal("Expecting no want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(sw.wantHaves.Keys(), blkCids0) {
		t.Fatal("Wrong keys")
	}
}

func TestReceiveBlock(t *testing.T) {
	cids := testutil.GenerateCids(2)
	peers := testutil.GeneratePeers(2)
	peerA := peers[0]
	peerB := peers[1]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	spm := newSessionWantSender(context.Background(), sid, pm, bpm, onSend, onPeersExhausted)

	go spm.Run()

	// add cid0, cid1
	spm.Add(cids)
	// peerA: HAVE cid0
	spm.Update(peerA, []cid.Cid{}, []cid.Cid{cids[0]}, []cid.Cid{}, true)

	// Wait for processing to complete
	peerSends := pm.waitNextWants()

	// Should have sent
	// peerA: want-block cid0, cid1
	sw, ok := peerSends[peerA]
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids) {
		t.Fatal("Wrong keys")
	}

	// Clear wants (makes keeping track of what's been sent easier)
	pm.clearWants()

	// peerA: block cid0, DONT_HAVE cid1
	bpm.ReceiveFrom(peerA, []cid.Cid{}, []cid.Cid{cids[1]})
	spm.Update(peerA, []cid.Cid{cids[0]}, []cid.Cid{}, []cid.Cid{cids[1]}, false)
	// peerB: HAVE cid0, cid1
	bpm.ReceiveFrom(peerB, cids, []cid.Cid{})
	spm.Update(peerB, []cid.Cid{}, cids, []cid.Cid{}, true)

	// Wait for processing to complete
	peerSends = pm.waitNextWants()

	// Should have sent
	// peerB: want-block cid1
	// (should not have sent want-block for cid0 because block0 has already
	// been received)
	sw, ok = peerSends[peerB]
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	wb := sw.wantBlocks.Keys()
	if len(wb) != 1 || !wb[0].Equals(cids[1]) {
		t.Fatal("Wrong keys", wb)
	}
}

func TestPeerUnavailable(t *testing.T) {
	cids := testutil.GenerateCids(2)
	peers := testutil.GeneratePeers(2)
	peerA := peers[0]
	peerB := peers[1]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	spm := newSessionWantSender(context.Background(), sid, pm, bpm, onSend, onPeersExhausted)

	go spm.Run()

	// add cid0, cid1
	spm.Add(cids)
	// peerA: HAVE cid0
	spm.Update(peerA, []cid.Cid{}, []cid.Cid{cids[0]}, []cid.Cid{}, true)

	// Wait for processing to complete
	peerSends := pm.waitNextWants()

	// Should have sent
	// peerA: want-block cid0, cid1
	sw, ok := peerSends[peerA]
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids) {
		t.Fatal("Wrong keys")
	}

	// Clear wants (makes keeping track of what's been sent easier)
	pm.clearWants()

	// peerB: HAVE cid0
	spm.Update(peerB, []cid.Cid{}, []cid.Cid{cids[0]}, []cid.Cid{}, true)

	// Wait for processing to complete
	peerSends = pm.waitNextWants()

	// Should not have sent anything because want-blocks were already sent to
	// peer A
	sw, ok = peerSends[peerB]
	if ok && sw.wantBlocks.Len() > 0 {
		t.Fatal("Expected no wants sent to peer")
	}

	// peerA becomes unavailable
	spm.SignalAvailability(peerA, false)

	// Wait for processing to complete
	peerSends = pm.waitNextWants()

	// Should now have sent want-block cid0, cid1 to peerB
	sw, ok = peerSends[peerB]
	if !ok {
		t.Fatal("Nothing sent to peer")
	}
	if !testutil.MatchKeysIgnoreOrder(sw.wantBlocks.Keys(), cids) {
		t.Fatal("Wrong keys")
	}
}

func TestPeersExhausted(t *testing.T) {
	cids := testutil.GenerateCids(2)
	peers := testutil.GeneratePeers(2)
	peerA := peers[0]
	peerB := peers[1]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}

	var exhausted []cid.Cid
	onPeersExhausted := func(ks []cid.Cid) {
		exhausted = append(exhausted, ks...)
	}
	spm := newSessionWantSender(context.Background(), sid, pm, bpm, onSend, onPeersExhausted)

	go spm.Run()

	// add cid0, cid1
	spm.Add(cids)

	// peerA: DONT_HAVE cid0
	bpm.ReceiveFrom(peerA, []cid.Cid{}, []cid.Cid{cids[0]})
	// Note: this also registers peer A as being available
	spm.Update(peerA, []cid.Cid{}, []cid.Cid{}, []cid.Cid{cids[0]}, true)

	time.Sleep(5 * time.Millisecond)

	// All available peers (peer A) have sent us a DONT_HAVE for cid0,
	// so expect that onPeersExhausted() will be called with cid0
	if !testutil.MatchKeysIgnoreOrder(exhausted, []cid.Cid{cids[0]}) {
		t.Fatal("Wrong keys")
	}

	// Clear exhausted cids
	exhausted = []cid.Cid{}

	// peerB: DONT_HAVE cid0, cid1
	bpm.ReceiveFrom(peerB, []cid.Cid{}, cids)
	spm.Update(peerB, []cid.Cid{}, []cid.Cid{}, cids, true)

	// Wait for processing to complete
	pm.waitNextWants()

	// All available peers (peer A and peer B) have sent us a DONT_HAVE
	// for cid0, but we already called onPeersExhausted with cid0, so it
	// should not be called again
	if len(exhausted) > 0 {
		t.Fatal("Wrong keys")
	}

	// peerA: DONT_HAVE cid1
	bpm.ReceiveFrom(peerA, []cid.Cid{}, []cid.Cid{cids[1]})
	spm.Update(peerA, []cid.Cid{}, []cid.Cid{}, []cid.Cid{cids[1]}, false)

	// Wait for processing to complete
	pm.waitNextWants()

	// All available peers (peer A and peer B) have sent us a DONT_HAVE for
	// cid1, so expect that onPeersExhausted() will be called with cid1
	if !testutil.MatchKeysIgnoreOrder(exhausted, []cid.Cid{cids[1]}) {
		t.Fatal("Wrong keys")
	}
}

func TestConsecutiveDontHaveLimit(t *testing.T) {
	cids := testutil.GenerateCids(peerDontHaveLimit + 10)
	p := testutil.GeneratePeers(1)[0]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	spm := newSessionWantSender(context.Background(), sid, pm, bpm, onSend, onPeersExhausted)

	go spm.Run()

	// Add all cids as wants
	spm.Add(cids)

	// Receive a HAVE from peer (adds it to the session)
	bpm.ReceiveFrom(p, cids[:1], []cid.Cid{})
	spm.Update(p, []cid.Cid{}, cids[:1], []cid.Cid{}, true)

	// Wait for processing to complete
	time.Sleep(5 * time.Millisecond)

	// Peer should be available
	if avail, ok := spm.isAvailable(p); !ok || !avail {
		t.Fatal("Expected peer to be available")
	}

	// Receive DONT_HAVEs from peer that do not exceed limit
	for _, c := range cids[1:peerDontHaveLimit] {
		bpm.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{c})
		spm.Update(p, []cid.Cid{}, []cid.Cid{}, []cid.Cid{c}, false)
	}

	// Wait for processing to complete
	time.Sleep(5 * time.Millisecond)

	// Peer should be available
	if avail, ok := spm.isAvailable(p); !ok || !avail {
		t.Fatal("Expected peer to be available")
	}

	// Receive DONT_HAVEs from peer that exceed limit
	for _, c := range cids[peerDontHaveLimit:] {
		bpm.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{c})
		spm.Update(p, []cid.Cid{}, []cid.Cid{}, []cid.Cid{c}, false)
	}

	// Wait for processing to complete
	time.Sleep(5 * time.Millisecond)

	// Session should remove peer
	if avail, _ := spm.isAvailable(p); avail {
		t.Fatal("Expected peer not to be available")
	}
}

func TestConsecutiveDontHaveLimitInterrupted(t *testing.T) {
	cids := testutil.GenerateCids(peerDontHaveLimit + 10)
	p := testutil.GeneratePeers(1)[0]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	spm := newSessionWantSender(context.Background(), sid, pm, bpm, onSend, onPeersExhausted)

	go spm.Run()

	// Add all cids as wants
	spm.Add(cids)

	// Receive a HAVE from peer (adds it to the session)
	bpm.ReceiveFrom(p, cids[:1], []cid.Cid{})
	spm.Update(p, []cid.Cid{}, cids[:1], []cid.Cid{}, true)

	// Wait for processing to complete
	time.Sleep(5 * time.Millisecond)

	// Peer should be available
	if avail, ok := spm.isAvailable(p); !ok || !avail {
		t.Fatal("Expected peer to be available")
	}

	// Receive DONT_HAVE then HAVE then DONT_HAVE from peer,
	// where consecutive DONT_HAVEs would have exceeded limit
	// (but they are not consecutive)
	for _, c := range cids[1:peerDontHaveLimit] {
		// DONT_HAVEs
		bpm.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{c})
		spm.Update(p, []cid.Cid{}, []cid.Cid{}, []cid.Cid{c}, false)
	}
	for _, c := range cids[peerDontHaveLimit : peerDontHaveLimit+1] {
		// HAVEs
		bpm.ReceiveFrom(p, []cid.Cid{c}, []cid.Cid{})
		spm.Update(p, []cid.Cid{}, []cid.Cid{c}, []cid.Cid{}, false)
	}
	for _, c := range cids[peerDontHaveLimit+1:] {
		// DONT_HAVEs
		bpm.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{c})
		spm.Update(p, []cid.Cid{}, []cid.Cid{}, []cid.Cid{c}, false)
	}

	// Wait for processing to complete
	time.Sleep(5 * time.Millisecond)

	// Peer should be available
	if avail, ok := spm.isAvailable(p); !ok || !avail {
		t.Fatal("Expected peer to be available")
	}
}

func TestConsecutiveDontHaveReinstateAfterRemoval(t *testing.T) {
	cids := testutil.GenerateCids(peerDontHaveLimit + 10)
	p := testutil.GeneratePeers(1)[0]
	sid := uint64(1)
	pm := newMockPeerManager()
	bpm := bsbpm.New()
	onSend := func(peer.ID, []cid.Cid, []cid.Cid) {}
	onPeersExhausted := func([]cid.Cid) {}
	spm := newSessionWantSender(context.Background(), sid, pm, bpm, onSend, onPeersExhausted)

	go spm.Run()

	// Add all cids as wants
	spm.Add(cids)

	// Receive a HAVE from peer (adds it to the session)
	bpm.ReceiveFrom(p, cids[:1], []cid.Cid{})
	spm.Update(p, []cid.Cid{}, cids[:1], []cid.Cid{}, true)

	// Wait for processing to complete
	time.Sleep(5 * time.Millisecond)

	// Peer should be available
	if avail, ok := spm.isAvailable(p); !ok || !avail {
		t.Fatal("Expected peer to be available")
	}

	// Receive DONT_HAVEs from peer that exceed limit
	for _, c := range cids[1 : peerDontHaveLimit+2] {
		bpm.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{c})
		spm.Update(p, []cid.Cid{}, []cid.Cid{}, []cid.Cid{c}, false)
	}

	// Wait for processing to complete
	time.Sleep(5 * time.Millisecond)

	// Session should remove peer
	if avail, _ := spm.isAvailable(p); avail {
		t.Fatal("Expected peer not to be available")
	}

	// Receive a HAVE from peer (adds it back into the session)
	bpm.ReceiveFrom(p, cids[:1], []cid.Cid{})
	spm.Update(p, []cid.Cid{}, cids[:1], []cid.Cid{}, true)

	// Wait for processing to complete
	time.Sleep(5 * time.Millisecond)

	// Peer should be available
	if avail, ok := spm.isAvailable(p); !ok || !avail {
		t.Fatal("Expected peer to be available")
	}

	cids2 := testutil.GenerateCids(peerDontHaveLimit + 10)

	// Receive DONT_HAVEs from peer that don't exceed limit
	for _, c := range cids2[1:peerDontHaveLimit] {
		bpm.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{c})
		spm.Update(p, []cid.Cid{}, []cid.Cid{}, []cid.Cid{c}, false)
	}

	// Wait for processing to complete
	time.Sleep(5 * time.Millisecond)

	// Peer should be available
	if avail, ok := spm.isAvailable(p); !ok || !avail {
		t.Fatal("Expected peer to be available")
	}

	// Receive DONT_HAVEs from peer that exceed limit
	for _, c := range cids2[peerDontHaveLimit:] {
		bpm.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{c})
		spm.Update(p, []cid.Cid{}, []cid.Cid{}, []cid.Cid{c}, false)
	}

	// Wait for processing to complete
	time.Sleep(5 * time.Millisecond)

	// Session should remove peer
	if avail, _ := spm.isAvailable(p); avail {
		t.Fatal("Expected peer not to be available")
	}
}
