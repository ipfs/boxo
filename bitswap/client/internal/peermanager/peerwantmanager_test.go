package peermanager

import (
	"sync"
	"testing"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
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
	wllock  sync.Mutex
}

func (mpq *mockPQ) clear() {
	mpq.wllock.Lock()
	defer mpq.wllock.Unlock()
	mpq.bcst = nil
	mpq.wbs = nil
	mpq.whs = nil
	mpq.cancels = nil
}

func (mpq *mockPQ) Startup()  {}
func (mpq *mockPQ) Shutdown() {}

func (mpq *mockPQ) AddBroadcastWantHaves(whs []cid.Cid) {
	mpq.wllock.Lock()
	defer mpq.wllock.Unlock()
	mpq.bcst = append(mpq.bcst, whs...)
}

func (mpq *mockPQ) AddWants(wbs []cid.Cid, whs []cid.Cid) {
	mpq.wllock.Lock()
	defer mpq.wllock.Unlock()
	mpq.wbs = append(mpq.wbs, wbs...)
	mpq.whs = append(mpq.whs, whs...)
}

func (mpq *mockPQ) AddCancels(cs []cid.Cid) {
	mpq.wllock.Lock()
	defer mpq.wllock.Unlock()
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
	require.Empty(t, pwm.getWantBlocks())
	require.Empty(t, pwm.getWantHaves())
}

func TestPWMBroadcastWantHaves(t *testing.T) {
	pwm := newPeerWantManager(&gauge{}, &gauge{})

	peers := random.Peers(3)
	cids := random.Cids(2)
	cids2 := random.Cids(2)
	cids3 := random.Cids(2)

	peerQueues := make(map[peer.ID]PeerQueue)
	for _, p := range peers[:2] {
		pq := &mockPQ{}
		peerQueues[p] = pq
		wg := pwm.addPeer(pq, p)
		wg.Wait()
		require.Empty(t, pq.bcst, "expected no broadcast wants")
	}

	// Broadcast 2 cids to 2 peers
	wg := pwm.broadcastWantHaves(cids)
	wg.Wait()
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		require.Len(t, pq.bcst, 2, "Expected 2 want-haves")
		require.ElementsMatch(t, pq.bcst, cids, "Expected all cids to be broadcast")
	}

	// Broadcasting same cids should have no effect
	clearSent(peerQueues)
	wg = pwm.broadcastWantHaves(cids)
	wg.Wait()
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		require.Len(t, pq.bcst, 0, "Expected 0 want-haves")
	}

	// Broadcast 2 other cids
	clearSent(peerQueues)
	wg = pwm.broadcastWantHaves(cids2)
	wg.Wait()
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		require.Len(t, pq.bcst, 2, "Expected 2 want-haves")
		require.ElementsMatch(t, pq.bcst, cids2, "Expected all new cids to be broadcast")
	}

	// Broadcast mix of old and new cids
	clearSent(peerQueues)
	wg = pwm.broadcastWantHaves(append(cids, cids3...))
	wg.Wait()
	for _, pqi := range peerQueues {
		pq := pqi.(*mockPQ)
		require.Len(t, pq.bcst, 2, "Expected 2 want-haves")
		// Only new cids should be broadcast
		require.ElementsMatch(t, pq.bcst, cids3, "Expected all new cids to be broadcast")
	}

	// Sending want-block for a cid should prevent broadcast to that peer
	clearSent(peerQueues)
	cids4 := random.Cids(4)
	wantBlocks := []cid.Cid{cids4[0], cids4[2]}
	p0 := peers[0]
	p1 := peers[1]
	wg = pwm.sendWants(p0, wantBlocks, []cid.Cid{})
	wg.Wait()

	wg = pwm.broadcastWantHaves(cids4)
	wg.Wait()
	pq0 := peerQueues[p0].(*mockPQ)
	// only broadcast 2 / 4 want-haves
	require.Len(t, pq0.bcst, 2, "Expected 2 want-haves")
	require.ElementsMatch(t, pq0.bcst, []cid.Cid{cids4[1], cids4[3]}, "Expected unsent cids to be broadcast")
	pq1 := peerQueues[p1].(*mockPQ)
	// broadcast all 4 want-haves
	require.Len(t, pq1.bcst, 4, "Expected 4 want-haves")
	require.ElementsMatch(t, pq1.bcst, cids4, "Expected all cids to be broadcast")

	allCids := cids
	allCids = append(allCids, cids2...)
	allCids = append(allCids, cids3...)
	allCids = append(allCids, cids4...)

	// Add another peer
	peer2 := peers[2]
	pq2 := &mockPQ{}
	peerQueues[peer2] = pq2
	wg = pwm.addPeer(pq2, peer2)
	wg.Wait()
	require.ElementsMatch(t, pq2.bcst, allCids, "Expected all cids to be broadcast")

	clearSent(peerQueues)
	wg = pwm.broadcastWantHaves(allCids)
	wg.Wait()
	require.Empty(t, pq2.bcst, "did not expect to have CIDs to broadcast")
}

func TestPWMSendWants(t *testing.T) {
	pwm := newPeerWantManager(&gauge{}, &gauge{})

	peers := random.Peers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := random.Cids(2)
	cids2 := random.Cids(2)

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
	wg := pwm.sendWants(p0, cids, cids2)
	wg.Wait()
	require.ElementsMatch(t, pq0.wbs, cids, "Expected 2 want-blocks")
	require.ElementsMatch(t, pq0.whs, cids2, "Expected 2 want-haves")

	// Send to p0
	// - 1 old want-block and 2 new want-blocks
	// - 1 old want-have  and 2 new want-haves
	clearSent(peerQueues)
	cids3 := random.Cids(2)
	cids4 := random.Cids(2)
	wg = pwm.sendWants(p0, append(cids3, cids[0]), append(cids4, cids2[0]))
	wg.Wait()
	require.ElementsMatch(t, pq0.wbs, cids3, "Expected 2 want-blocks")
	require.ElementsMatch(t, pq0.whs, cids4, "Expected 2 want-haves")

	// Send to p0 as want-blocks: 1 new want-block, 1 old want-have
	clearSent(peerQueues)
	cids5 := random.Cids(1)
	newWantBlockOldWantHave := append(cids5, cids2[0])
	wg = pwm.sendWants(p0, newWantBlockOldWantHave, []cid.Cid{})
	wg.Wait()
	// If a want was sent as a want-have, it should be ok to now send it as a
	// want-block
	require.ElementsMatch(t, pq0.wbs, newWantBlockOldWantHave, "Expected 2 want-blocks")
	require.Empty(t, pq0.whs, "Expected 0 want-haves")

	// Send to p0 as want-haves: 1 new want-have, 1 old want-block
	clearSent(peerQueues)
	cids6 := random.Cids(1)
	newWantHaveOldWantBlock := append(cids6, cids[0])
	wg = pwm.sendWants(p0, []cid.Cid{}, newWantHaveOldWantBlock)
	wg.Wait()
	// If a want was previously sent as a want-block, it should not be
	// possible to now send it as a want-have
	require.ElementsMatch(t, pq0.whs, cids6, "Expected 1 want-have")
	require.Empty(t, pq0.wbs, "Expected 0 want-blocks")

	// Send 2 want-blocks and 2 want-haves to p1
	wg = pwm.sendWants(p1, cids, cids2)
	wg.Wait()
	require.ElementsMatch(t, pq1.wbs, cids, "Expected 2 want-blocks")
	require.ElementsMatch(t, pq1.whs, cids2, "Expected 2 want-haves")
}

func TestPWMSendCancels(t *testing.T) {
	t.Skip()
	pwm := newPeerWantManager(&gauge{}, &gauge{})

	peers := random.Peers(2)
	p0 := peers[0]
	p1 := peers[1]
	wb1 := random.Cids(2)
	wh1 := random.Cids(2)
	wb2 := random.Cids(2)
	wh2 := random.Cids(2)
	allwb := append(wb1, wb2...)
	allwh := append(wh1, wh2...)

	peerQueues := make(map[peer.ID]PeerQueue)
	for _, p := range peers[:2] {
		pq := &mockPQ{}
		peerQueues[p] = pq
		wg := pwm.addPeer(pq, p)
		wg.Wait()
	}
	pq0 := peerQueues[p0].(*mockPQ)
	pq1 := peerQueues[p1].(*mockPQ)

	// Send 2 want-blocks and 2 want-haves to p0
	wg := pwm.sendWants(p0, wb1, wh1)
	wg.Wait()
	// Send 3 want-blocks and 3 want-haves to p1
	// (1 overlapping want-block / want-have with p0)
	wg = pwm.sendWants(p1, append(wb2, wb1[1]), append(wh2, wh1[1]))
	wg.Wait()

	require.ElementsMatch(t, pwm.getWantBlocks(), allwb, "Expected 4 cids to be wanted")
	require.ElementsMatch(t, pwm.getWantHaves(), allwh, "Expected 4 cids to be wanted")

	// Cancel 1 want-block and 1 want-have that were sent to p0
	clearSent(peerQueues)
	wg = pwm.sendCancels([]cid.Cid{wb1[0], wh1[0]})
	wg.Wait()
	// Should cancel the want-block and want-have
	require.Empty(t, pq1.cancels, "Expected no cancels sent to p1")
	require.ElementsMatch(t, pq0.cancels, []cid.Cid{wb1[0], wh1[0]}, "Expected 2 cids to be cancelled")
	require.ElementsMatch(t, pwm.getWantBlocks(), append(wb2, wb1[1]), "Expected 3 want-blocks")
	require.ElementsMatch(t, pwm.getWantHaves(), append(wh2, wh1[1]), "Expected 3 want-haves")

	// Cancel everything
	clearSent(peerQueues)
	allCids := append(allwb, allwh...)
	wg = pwm.sendCancels(allCids)
	wg.Wait()
	// Should cancel the remaining want-blocks and want-haves for p0
	require.ElementsMatch(t, pq0.cancels, []cid.Cid{wb1[1], wh1[1]}, "Expected un-cancelled cids to be cancelled")

	// Should cancel the remaining want-blocks and want-haves for p1
	remainingP1 := append(wb2, wh2...)
	remainingP1 = append(remainingP1, wb1[1], wh1[1])
	require.Equal(t, len(pq1.cancels), len(remainingP1), "mismatch", len(pq1.cancels), len(remainingP1))
	require.ElementsMatch(t, pq1.cancels, remainingP1, "Expected un-cancelled cids to be cancelled")
	require.Empty(t, pwm.getWantBlocks(), "Expected 0 want-blocks")
	require.Empty(t, pwm.getWantHaves(), "Expected 0 want-haves")
}

func TestStats(t *testing.T) {
	g := &gauge{}
	wbg := &gauge{}
	pwm := newPeerWantManager(g, wbg)

	peers := random.Peers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := random.Cids(2)
	cids2 := random.Cids(2)

	peerQueues := make(map[peer.ID]PeerQueue)
	pq := &mockPQ{}
	peerQueues[p0] = pq
	wg := pwm.addPeer(pq, p0)
	wg.Wait()

	// Send 2 want-blocks and 2 want-haves to p0
	wg = pwm.sendWants(p0, cids, cids2)
	wg.Wait()

	require.Equal(t, 4, g.count, "Expected 4 wants")
	require.Equal(t, 2, wbg.count, "Expected 2 want-blocks")

	// Send 1 old want-block and 2 new want-blocks to p0
	cids3 := random.Cids(2)
	wg = pwm.sendWants(p0, append(cids3, cids[0]), []cid.Cid{})
	wg.Wait()

	require.Equal(t, 6, g.count, "Expected 6 wants")
	require.Equal(t, 4, wbg.count, "Expected 4 want-blocks")

	// Broadcast 1 old want-have and 2 new want-haves
	cids4 := random.Cids(2)
	wg = pwm.broadcastWantHaves(append(cids4, cids2[0]))
	wg.Wait()
	require.Equal(t, 8, g.count, "Expected 8 wants")
	require.Equal(t, 4, wbg.count, "Expected 4 want-blocks")

	// Add a second peer
	wg = pwm.addPeer(pq, p1)
	wg.Wait()

	require.Equal(t, 8, g.count, "Expected 8 wants")
	require.Equal(t, 4, wbg.count, "Expected 4 want-blocks")

	// Cancel 1 want-block that was sent to p0
	// and 1 want-block that was not sent
	cids5 := random.Cids(1)
	wg = pwm.sendCancels(append(cids5, cids[0]))
	wg.Wait()

	require.Equal(t, 7, g.count, "Expected 7 wants")
	require.Equal(t, 3, wbg.count, "Expected 3 want-blocks")

	// Remove first peer
	pwm.removePeer(p0)

	// Should still have 3 broadcast wants
	require.Equal(t, 3, g.count, "Expected 3 wants")
	require.Zero(t, wbg.count, "Expected all want-blocks to be removed")

	// Remove second peer
	pwm.removePeer(p1)

	// Should still have 3 broadcast wants
	require.Equal(t, 3, g.count, "Expected 3 wants")
	require.Zero(t, wbg.count, "Expected 0 want-blocks")

	// Cancel one remaining broadcast want-have
	wg = pwm.sendCancels(cids2[:1])
	wg.Wait()
	require.Equal(t, 2, g.count, "Expected 2 wants")
	require.Zero(t, wbg.count, "Expected 0 want-blocks")
}

func TestStatsOverlappingWantBlockWantHave(t *testing.T) {
	g := &gauge{}
	wbg := &gauge{}
	pwm := newPeerWantManager(g, wbg)

	peers := random.Peers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := random.Cids(2)
	cids2 := random.Cids(2)

	wg := pwm.addPeer(&mockPQ{}, p0)
	wg.Wait()
	wg = pwm.addPeer(&mockPQ{}, p1)
	wg.Wait()

	// Send 2 want-blocks and 2 want-haves to p0
	wg = pwm.sendWants(p0, cids, cids2)
	wg.Wait()

	// Send opposite:
	// 2 want-haves and 2 want-blocks to p1
	wg = pwm.sendWants(p1, cids2, cids)
	wg.Wait()

	require.Equal(t, 4, g.count, "Expected 4 wants")
	require.Equal(t, 4, wbg.count, "Expected 4 want-blocks")

	// Cancel 1 of each group of cids
	wg = pwm.sendCancels([]cid.Cid{cids[0], cids2[0]})
	wg.Wait()

	require.Equal(t, 2, g.count, "Expected 2 wants")
	require.Equal(t, 2, wbg.count, "Expected 2 want-blocks")
}

func TestStatsRemovePeerOverlappingWantBlockWantHave(t *testing.T) {
	g := &gauge{}
	wbg := &gauge{}
	pwm := newPeerWantManager(g, wbg)

	peers := random.Peers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := random.Cids(2)
	cids2 := random.Cids(2)

	wg := pwm.addPeer(&mockPQ{}, p0)
	wg.Wait()
	wg = pwm.addPeer(&mockPQ{}, p1)
	wg.Wait()

	// Send 2 want-blocks and 2 want-haves to p0
	wg = pwm.sendWants(p0, cids, cids2)
	wg.Wait()

	// Send opposite:
	// 2 want-haves and 2 want-blocks to p1
	wg = pwm.sendWants(p1, cids2, cids)
	wg.Wait()

	require.Equal(t, 4, g.count, "Expected 4 wants")
	require.Equal(t, 4, wbg.count, "Expected 4 want-blocks")

	// Remove p0
	pwm.removePeer(p0)

	require.Equal(t, 4, g.count, "Expected 4 wants")
	require.Equal(t, 2, wbg.count, "Expected 2 want-blocks")
}
