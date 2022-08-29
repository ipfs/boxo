package peermanager

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

type gauge struct {
	count int
}

func (g *gauge) Inc() {
	g.count++
}
func (g *gauge) Dec() {
	g.count--
}

type mockPQ struct {
	bcst    []cid.Cid
	wbs     []cid.Cid
	whs     []cid.Cid
	cancels []cid.Cid
}

func (mpq *mockPQ) clear() {
	mpq.bcst = nil
	mpq.wbs = nil
	mpq.whs = nil
	mpq.cancels = nil
}

func (mpq *mockPQ) Startup()  {}
func (mpq *mockPQ) Shutdown() {}

func (mpq *mockPQ) AddBroadcastWantHaves(whs []cid.Cid) {
	mpq.bcst = append(mpq.bcst, whs...)
}
func (mpq *mockPQ) AddWants(wbs []cid.Cid, whs []cid.Cid) {
	mpq.wbs = append(mpq.wbs, wbs...)
	mpq.whs = append(mpq.whs, whs...)
}
func (mpq *mockPQ) AddCancels(cs []cid.Cid) {
	mpq.cancels = append(mpq.cancels, cs...)
}
func (mpq *mockPQ) ResponseReceived(ks []cid.Cid) {
}

func clearSent(pqs map[peer.ID]PeerQueue) {
	for _, pqi := range pqs {
		pqi.(*mockPQ).clear()
	}
}

func TestEmpty(t *testing.T) {
	pwm := newPeerWantManager(&gauge{}, &gauge{})

	if len(pwm.getWantBlocks()) > 0 {
		t.Fatal("Expected GetWantBlocks() to have length 0")
	}
	if len(pwm.getWantHaves()) > 0 {
		t.Fatal("Expected GetWantHaves() to have length 0")
	}
}

func TestPWMBroadcastWantHaves(t *testing.T) {
	pwm := newPeerWantManager(&gauge{}, &gauge{})

	peers := testutil.GeneratePeers(3)
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)
	cids3 := testutil.GenerateCids(2)

	peerQueues := make(map[peer.ID]PeerQueue)
	for _, p := range peers[:2] {
		pq := &mockPQ{}
		peerQueues[p] = pq
		pwm.addPeer(pq, p)
		if len(pq.bcst) > 0 {
			t.Errorf("expected no broadcast wants")
		}
	}

	// Broadcast 2 cids to 2 peers
	pwm.broadcastWantHaves(cids)
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		if len(pq.bcst) != 2 {
			t.Fatal("Expected 2 want-haves")
		}
		if !testutil.MatchKeysIgnoreOrder(pq.bcst, cids) {
			t.Fatal("Expected all cids to be broadcast")
		}
	}

	// Broadcasting same cids should have no effect
	clearSent(peerQueues)
	pwm.broadcastWantHaves(cids)
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		if len(pq.bcst) != 0 {
			t.Fatal("Expected 0 want-haves")
		}
	}

	// Broadcast 2 other cids
	clearSent(peerQueues)
	pwm.broadcastWantHaves(cids2)
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		if len(pq.bcst) != 2 {
			t.Fatal("Expected 2 want-haves")
		}
		if !testutil.MatchKeysIgnoreOrder(pq.bcst, cids2) {
			t.Fatal("Expected all new cids to be broadcast")
		}
	}

	// Broadcast mix of old and new cids
	clearSent(peerQueues)
	pwm.broadcastWantHaves(append(cids, cids3...))
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		if len(pq.bcst) != 2 {
			t.Fatal("Expected 2 want-haves")
		}
		// Only new cids should be broadcast
		if !testutil.MatchKeysIgnoreOrder(pq.bcst, cids3) {
			t.Fatal("Expected all new cids to be broadcast")
		}
	}

	// Sending want-block for a cid should prevent broadcast to that peer
	clearSent(peerQueues)
	cids4 := testutil.GenerateCids(4)
	wantBlocks := []cid.Cid{cids4[0], cids4[2]}
	p0 := peers[0]
	p1 := peers[1]
	pwm.sendWants(p0, wantBlocks, []cid.Cid{})

	pwm.broadcastWantHaves(cids4)
	pq0 := peerQueues[p0].(*mockPQ)
	if len(pq0.bcst) != 2 { // only broadcast 2 / 4 want-haves
		t.Fatal("Expected 2 want-haves")
	}
	if !testutil.MatchKeysIgnoreOrder(pq0.bcst, []cid.Cid{cids4[1], cids4[3]}) {
		t.Fatalf("Expected unsent cids to be broadcast")
	}
	pq1 := peerQueues[p1].(*mockPQ)
	if len(pq1.bcst) != 4 { // broadcast all 4 want-haves
		t.Fatal("Expected 4 want-haves")
	}
	if !testutil.MatchKeysIgnoreOrder(pq1.bcst, cids4) {
		t.Fatal("Expected all cids to be broadcast")
	}

	allCids := cids
	allCids = append(allCids, cids2...)
	allCids = append(allCids, cids3...)
	allCids = append(allCids, cids4...)

	// Add another peer
	peer2 := peers[2]
	pq2 := &mockPQ{}
	peerQueues[peer2] = pq2
	pwm.addPeer(pq2, peer2)
	if !testutil.MatchKeysIgnoreOrder(pq2.bcst, allCids) {
		t.Fatalf("Expected all cids to be broadcast.")
	}

	clearSent(peerQueues)
	pwm.broadcastWantHaves(allCids)
	if len(pq2.bcst) != 0 {
		t.Errorf("did not expect to have CIDs to broadcast")
	}
}

func TestPWMSendWants(t *testing.T) {
	pwm := newPeerWantManager(&gauge{}, &gauge{})

	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)

	peerQueues := make(map[peer.ID]PeerQueue)
	for _, p := range peers[:2] {
		pq := &mockPQ{}
		peerQueues[p] = pq
		pwm.addPeer(pq, p)
	}
	pq0 := peerQueues[p0].(*mockPQ)
	pq1 := peerQueues[p1].(*mockPQ)

	// Send 2 want-blocks and 2 want-haves to p0
	clearSent(peerQueues)
	pwm.sendWants(p0, cids, cids2)
	if !testutil.MatchKeysIgnoreOrder(pq0.wbs, cids) {
		t.Fatal("Expected 2 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(pq0.whs, cids2) {
		t.Fatal("Expected 2 want-haves")
	}

	// Send to p0
	// - 1 old want-block and 2 new want-blocks
	// - 1 old want-have  and 2 new want-haves
	clearSent(peerQueues)
	cids3 := testutil.GenerateCids(2)
	cids4 := testutil.GenerateCids(2)
	pwm.sendWants(p0, append(cids3, cids[0]), append(cids4, cids2[0]))
	if !testutil.MatchKeysIgnoreOrder(pq0.wbs, cids3) {
		t.Fatal("Expected 2 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(pq0.whs, cids4) {
		t.Fatal("Expected 2 want-haves")
	}

	// Send to p0 as want-blocks: 1 new want-block, 1 old want-have
	clearSent(peerQueues)
	cids5 := testutil.GenerateCids(1)
	newWantBlockOldWantHave := append(cids5, cids2[0])
	pwm.sendWants(p0, newWantBlockOldWantHave, []cid.Cid{})
	// If a want was sent as a want-have, it should be ok to now send it as a
	// want-block
	if !testutil.MatchKeysIgnoreOrder(pq0.wbs, newWantBlockOldWantHave) {
		t.Fatal("Expected 2 want-blocks")
	}
	if len(pq0.whs) != 0 {
		t.Fatal("Expected 0 want-haves")
	}

	// Send to p0 as want-haves: 1 new want-have, 1 old want-block
	clearSent(peerQueues)
	cids6 := testutil.GenerateCids(1)
	newWantHaveOldWantBlock := append(cids6, cids[0])
	pwm.sendWants(p0, []cid.Cid{}, newWantHaveOldWantBlock)
	// If a want was previously sent as a want-block, it should not be
	// possible to now send it as a want-have
	if !testutil.MatchKeysIgnoreOrder(pq0.whs, cids6) {
		t.Fatal("Expected 1 want-have")
	}
	if len(pq0.wbs) != 0 {
		t.Fatal("Expected 0 want-blocks")
	}

	// Send 2 want-blocks and 2 want-haves to p1
	pwm.sendWants(p1, cids, cids2)
	if !testutil.MatchKeysIgnoreOrder(pq1.wbs, cids) {
		t.Fatal("Expected 2 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(pq1.whs, cids2) {
		t.Fatal("Expected 2 want-haves")
	}
}

func TestPWMSendCancels(t *testing.T) {
	pwm := newPeerWantManager(&gauge{}, &gauge{})

	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	p1 := peers[1]
	wb1 := testutil.GenerateCids(2)
	wh1 := testutil.GenerateCids(2)
	wb2 := testutil.GenerateCids(2)
	wh2 := testutil.GenerateCids(2)
	allwb := append(wb1, wb2...)
	allwh := append(wh1, wh2...)

	peerQueues := make(map[peer.ID]PeerQueue)
	for _, p := range peers[:2] {
		pq := &mockPQ{}
		peerQueues[p] = pq
		pwm.addPeer(pq, p)
	}
	pq0 := peerQueues[p0].(*mockPQ)
	pq1 := peerQueues[p1].(*mockPQ)

	// Send 2 want-blocks and 2 want-haves to p0
	pwm.sendWants(p0, wb1, wh1)
	// Send 3 want-blocks and 3 want-haves to p1
	// (1 overlapping want-block / want-have with p0)
	pwm.sendWants(p1, append(wb2, wb1[1]), append(wh2, wh1[1]))

	if !testutil.MatchKeysIgnoreOrder(pwm.getWantBlocks(), allwb) {
		t.Fatal("Expected 4 cids to be wanted")
	}
	if !testutil.MatchKeysIgnoreOrder(pwm.getWantHaves(), allwh) {
		t.Fatal("Expected 4 cids to be wanted")
	}

	// Cancel 1 want-block and 1 want-have that were sent to p0
	clearSent(peerQueues)
	pwm.sendCancels([]cid.Cid{wb1[0], wh1[0]})
	// Should cancel the want-block and want-have
	if len(pq1.cancels) != 0 {
		t.Fatal("Expected no cancels sent to p1")
	}
	if !testutil.MatchKeysIgnoreOrder(pq0.cancels, []cid.Cid{wb1[0], wh1[0]}) {
		t.Fatal("Expected 2 cids to be cancelled")
	}
	if !testutil.MatchKeysIgnoreOrder(pwm.getWantBlocks(), append(wb2, wb1[1])) {
		t.Fatal("Expected 3 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(pwm.getWantHaves(), append(wh2, wh1[1])) {
		t.Fatal("Expected 3 want-haves")
	}

	// Cancel everything
	clearSent(peerQueues)
	allCids := append(allwb, allwh...)
	pwm.sendCancels(allCids)
	// Should cancel the remaining want-blocks and want-haves for p0
	if !testutil.MatchKeysIgnoreOrder(pq0.cancels, []cid.Cid{wb1[1], wh1[1]}) {
		t.Fatal("Expected un-cancelled cids to be cancelled")
	}

	// Should cancel the remaining want-blocks and want-haves for p1
	remainingP1 := append(wb2, wh2...)
	remainingP1 = append(remainingP1, wb1[1], wh1[1])
	if len(pq1.cancels) != len(remainingP1) {
		t.Fatal("mismatch", len(pq1.cancels), len(remainingP1))
	}
	if !testutil.MatchKeysIgnoreOrder(pq1.cancels, remainingP1) {
		t.Fatal("Expected un-cancelled cids to be cancelled")
	}
	if len(pwm.getWantBlocks()) != 0 {
		t.Fatal("Expected 0 want-blocks")
	}
	if len(pwm.getWantHaves()) != 0 {
		t.Fatal("Expected 0 want-haves")
	}
}

func TestStats(t *testing.T) {
	g := &gauge{}
	wbg := &gauge{}
	pwm := newPeerWantManager(g, wbg)

	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)

	peerQueues := make(map[peer.ID]PeerQueue)
	pq := &mockPQ{}
	peerQueues[p0] = pq
	pwm.addPeer(pq, p0)

	// Send 2 want-blocks and 2 want-haves to p0
	pwm.sendWants(p0, cids, cids2)

	if g.count != 4 {
		t.Fatal("Expected 4 wants")
	}
	if wbg.count != 2 {
		t.Fatal("Expected 2 want-blocks")
	}

	// Send 1 old want-block and 2 new want-blocks to p0
	cids3 := testutil.GenerateCids(2)
	pwm.sendWants(p0, append(cids3, cids[0]), []cid.Cid{})

	if g.count != 6 {
		t.Fatal("Expected 6 wants")
	}
	if wbg.count != 4 {
		t.Fatal("Expected 4 want-blocks")
	}

	// Broadcast 1 old want-have and 2 new want-haves
	cids4 := testutil.GenerateCids(2)
	pwm.broadcastWantHaves(append(cids4, cids2[0]))
	if g.count != 8 {
		t.Fatal("Expected 8 wants")
	}
	if wbg.count != 4 {
		t.Fatal("Expected 4 want-blocks")
	}

	// Add a second peer
	pwm.addPeer(pq, p1)

	if g.count != 8 {
		t.Fatal("Expected 8 wants")
	}
	if wbg.count != 4 {
		t.Fatal("Expected 4 want-blocks")
	}

	// Cancel 1 want-block that was sent to p0
	// and 1 want-block that was not sent
	cids5 := testutil.GenerateCids(1)
	pwm.sendCancels(append(cids5, cids[0]))

	if g.count != 7 {
		t.Fatal("Expected 7 wants")
	}
	if wbg.count != 3 {
		t.Fatal("Expected 3 want-blocks")
	}

	// Remove first peer
	pwm.removePeer(p0)

	// Should still have 3 broadcast wants
	if g.count != 3 {
		t.Fatal("Expected 3 wants")
	}
	if wbg.count != 0 {
		t.Fatal("Expected all want-blocks to be removed")
	}

	// Remove second peer
	pwm.removePeer(p1)

	// Should still have 3 broadcast wants
	if g.count != 3 {
		t.Fatal("Expected 3 wants")
	}
	if wbg.count != 0 {
		t.Fatal("Expected 0 want-blocks")
	}

	// Cancel one remaining broadcast want-have
	pwm.sendCancels(cids2[:1])
	if g.count != 2 {
		t.Fatal("Expected 2 wants")
	}
	if wbg.count != 0 {
		t.Fatal("Expected 0 want-blocks")
	}
}

func TestStatsOverlappingWantBlockWantHave(t *testing.T) {
	g := &gauge{}
	wbg := &gauge{}
	pwm := newPeerWantManager(g, wbg)

	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)

	pwm.addPeer(&mockPQ{}, p0)
	pwm.addPeer(&mockPQ{}, p1)

	// Send 2 want-blocks and 2 want-haves to p0
	pwm.sendWants(p0, cids, cids2)

	// Send opposite:
	// 2 want-haves and 2 want-blocks to p1
	pwm.sendWants(p1, cids2, cids)

	if g.count != 4 {
		t.Fatal("Expected 4 wants")
	}
	if wbg.count != 4 {
		t.Fatal("Expected 4 want-blocks")
	}

	// Cancel 1 of each group of cids
	pwm.sendCancels([]cid.Cid{cids[0], cids2[0]})

	if g.count != 2 {
		t.Fatal("Expected 2 wants")
	}
	if wbg.count != 2 {
		t.Fatal("Expected 2 want-blocks")
	}
}

func TestStatsRemovePeerOverlappingWantBlockWantHave(t *testing.T) {
	g := &gauge{}
	wbg := &gauge{}
	pwm := newPeerWantManager(g, wbg)

	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)

	pwm.addPeer(&mockPQ{}, p0)
	pwm.addPeer(&mockPQ{}, p1)

	// Send 2 want-blocks and 2 want-haves to p0
	pwm.sendWants(p0, cids, cids2)

	// Send opposite:
	// 2 want-haves and 2 want-blocks to p1
	pwm.sendWants(p1, cids2, cids)

	if g.count != 4 {
		t.Fatal("Expected 4 wants")
	}
	if wbg.count != 4 {
		t.Fatal("Expected 4 want-blocks")
	}

	// Remove p0
	pwm.removePeer(p0)

	if g.count != 4 {
		t.Fatal("Expected 4 wants")
	}
	if wbg.count != 2 {
		t.Fatal("Expected 2 want-blocks")
	}
}
