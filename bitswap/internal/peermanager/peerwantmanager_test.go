package peermanager

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"

	cid "github.com/ipfs/go-cid"
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

func TestEmpty(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	if len(pwm.GetWantBlocks()) > 0 {
		t.Fatal("Expected GetWantBlocks() to have length 0")
	}
	if len(pwm.GetWantHaves()) > 0 {
		t.Fatal("Expected GetWantHaves() to have length 0")
	}
}

func TestPrepareBroadcastWantHaves(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	peers := testutil.GeneratePeers(3)
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)
	cids3 := testutil.GenerateCids(2)

	pwm.AddPeer(peers[0])
	pwm.AddPeer(peers[1])

	// Broadcast 2 cids to 2 peers
	bcst := pwm.PrepareBroadcastWantHaves(cids)
	if len(bcst) != 2 {
		t.Fatal("Expected 2 peers")
	}
	for p := range bcst {
		if !testutil.MatchKeysIgnoreOrder(bcst[p], cids) {
			t.Fatal("Expected all cids to be broadcast")
		}
	}

	// Broadcasting same cids should have no effect
	bcst2 := pwm.PrepareBroadcastWantHaves(cids)
	if len(bcst2) != 0 {
		t.Fatal("Expected 0 peers")
	}

	// Broadcast 2 other cids
	bcst3 := pwm.PrepareBroadcastWantHaves(cids2)
	if len(bcst3) != 2 {
		t.Fatal("Expected 2 peers")
	}
	for p := range bcst3 {
		if !testutil.MatchKeysIgnoreOrder(bcst3[p], cids2) {
			t.Fatal("Expected all new cids to be broadcast")
		}
	}

	// Broadcast mix of old and new cids
	bcst4 := pwm.PrepareBroadcastWantHaves(append(cids, cids3...))
	if len(bcst4) != 2 {
		t.Fatal("Expected 2 peers")
	}
	// Only new cids should be broadcast
	for p := range bcst4 {
		if !testutil.MatchKeysIgnoreOrder(bcst4[p], cids3) {
			t.Fatal("Expected all new cids to be broadcast")
		}
	}

	// Sending want-block for a cid should prevent broadcast to that peer
	cids4 := testutil.GenerateCids(4)
	wantBlocks := []cid.Cid{cids4[0], cids4[2]}
	pwm.PrepareSendWants(peers[0], wantBlocks, []cid.Cid{})

	bcst5 := pwm.PrepareBroadcastWantHaves(cids4)
	if len(bcst4) != 2 {
		t.Fatal("Expected 2 peers")
	}
	// Only cids that were not sent as want-block to peer should be broadcast
	for p := range bcst5 {
		if p == peers[0] {
			if !testutil.MatchKeysIgnoreOrder(bcst5[p], []cid.Cid{cids4[1], cids4[3]}) {
				t.Fatal("Expected unsent cids to be broadcast")
			}
		}
		if p == peers[1] {
			if !testutil.MatchKeysIgnoreOrder(bcst5[p], cids4) {
				t.Fatal("Expected all cids to be broadcast")
			}
		}
	}

	// Add another peer
	pwm.AddPeer(peers[2])
	bcst6 := pwm.PrepareBroadcastWantHaves(cids)
	if len(bcst6) != 1 {
		t.Fatal("Expected 1 peer")
	}
	for p := range bcst6 {
		if !testutil.MatchKeysIgnoreOrder(bcst6[p], cids) {
			t.Fatal("Expected all cids to be broadcast")
		}
	}
}

func TestPrepareSendWants(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)

	pwm.AddPeer(p0)
	pwm.AddPeer(p1)

	// Send 2 want-blocks and 2 want-haves to p0
	wb, wh := pwm.PrepareSendWants(p0, cids, cids2)
	if !testutil.MatchKeysIgnoreOrder(wb, cids) {
		t.Fatal("Expected 2 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(wh, cids2) {
		t.Fatal("Expected 2 want-haves")
	}

	// Send to p0
	// - 1 old want-block and 2 new want-blocks
	// - 1 old want-have  and 2 new want-haves
	cids3 := testutil.GenerateCids(2)
	cids4 := testutil.GenerateCids(2)
	wb2, wh2 := pwm.PrepareSendWants(p0, append(cids3, cids[0]), append(cids4, cids2[0]))
	if !testutil.MatchKeysIgnoreOrder(wb2, cids3) {
		t.Fatal("Expected 2 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(wh2, cids4) {
		t.Fatal("Expected 2 want-haves")
	}

	// Send to p0 as want-blocks: 1 new want-block, 1 old want-have
	cids5 := testutil.GenerateCids(1)
	newWantBlockOldWantHave := append(cids5, cids2[0])
	wb3, wh3 := pwm.PrepareSendWants(p0, newWantBlockOldWantHave, []cid.Cid{})
	// If a want was sent as a want-have, it should be ok to now send it as a
	// want-block
	if !testutil.MatchKeysIgnoreOrder(wb3, newWantBlockOldWantHave) {
		t.Fatal("Expected 2 want-blocks")
	}
	if len(wh3) != 0 {
		t.Fatal("Expected 0 want-haves")
	}

	// Send to p0 as want-haves: 1 new want-have, 1 old want-block
	cids6 := testutil.GenerateCids(1)
	newWantHaveOldWantBlock := append(cids6, cids[0])
	wb4, wh4 := pwm.PrepareSendWants(p0, []cid.Cid{}, newWantHaveOldWantBlock)
	// If a want was previously sent as a want-block, it should not be
	// possible to now send it as a want-have
	if !testutil.MatchKeysIgnoreOrder(wh4, cids6) {
		t.Fatal("Expected 1 want-have")
	}
	if len(wb4) != 0 {
		t.Fatal("Expected 0 want-blocks")
	}

	// Send 2 want-blocks and 2 want-haves to p1
	wb5, wh5 := pwm.PrepareSendWants(p1, cids, cids2)
	if !testutil.MatchKeysIgnoreOrder(wb5, cids) {
		t.Fatal("Expected 2 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(wh5, cids2) {
		t.Fatal("Expected 2 want-haves")
	}
}

func TestPrepareSendCancels(t *testing.T) {
	pwm := newPeerWantManager(&gauge{})

	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	p1 := peers[1]
	wb1 := testutil.GenerateCids(2)
	wh1 := testutil.GenerateCids(2)
	wb2 := testutil.GenerateCids(2)
	wh2 := testutil.GenerateCids(2)
	allwb := append(wb1, wb2...)
	allwh := append(wh1, wh2...)

	pwm.AddPeer(p0)
	pwm.AddPeer(p1)

	// Send 2 want-blocks and 2 want-haves to p0
	pwm.PrepareSendWants(p0, wb1, wh1)
	// Send 3 want-blocks and 3 want-haves to p1
	// (1 overlapping want-block / want-have with p0)
	pwm.PrepareSendWants(p1, append(wb2, wb1[1]), append(wh2, wh1[1]))

	if !testutil.MatchKeysIgnoreOrder(pwm.GetWantBlocks(), allwb) {
		t.Fatal("Expected 4 cids to be wanted")
	}
	if !testutil.MatchKeysIgnoreOrder(pwm.GetWantHaves(), allwh) {
		t.Fatal("Expected 4 cids to be wanted")
	}

	// Cancel 1 want-block and 1 want-have that were sent to p0
	res := pwm.PrepareSendCancels([]cid.Cid{wb1[0], wh1[0]})
	// Should cancel the want-block and want-have
	if len(res) != 1 {
		t.Fatal("Expected 1 peer")
	}
	if !testutil.MatchKeysIgnoreOrder(res[p0], []cid.Cid{wb1[0], wh1[0]}) {
		t.Fatal("Expected 2 cids to be cancelled")
	}
	if !testutil.MatchKeysIgnoreOrder(pwm.GetWantBlocks(), append(wb2, wb1[1])) {
		t.Fatal("Expected 3 want-blocks")
	}
	if !testutil.MatchKeysIgnoreOrder(pwm.GetWantHaves(), append(wh2, wh1[1])) {
		t.Fatal("Expected 3 want-haves")
	}

	// Cancel everything
	allCids := append(allwb, allwh...)
	res2 := pwm.PrepareSendCancels(allCids)
	// Should cancel the remaining want-blocks and want-haves
	if len(res2) != 2 {
		t.Fatal("Expected 2 peers", len(res2))
	}
	if !testutil.MatchKeysIgnoreOrder(res2[p0], []cid.Cid{wb1[1], wh1[1]}) {
		t.Fatal("Expected un-cancelled cids to be cancelled")
	}
	remainingP2 := append(wb2, wh2...)
	remainingP2 = append(remainingP2, wb1[1], wh1[1])
	if !testutil.MatchKeysIgnoreOrder(res2[p1], remainingP2) {
		t.Fatal("Expected un-cancelled cids to be cancelled")
	}
	if len(pwm.GetWantBlocks()) != 0 {
		t.Fatal("Expected 0 want-blocks")
	}
	if len(pwm.GetWantHaves()) != 0 {
		t.Fatal("Expected 0 want-haves")
	}
}

func TestStats(t *testing.T) {
	g := &gauge{}
	pwm := newPeerWantManager(g)

	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	cids := testutil.GenerateCids(2)
	cids2 := testutil.GenerateCids(2)

	pwm.AddPeer(p0)

	// Send 2 want-blocks and 2 want-haves to p0
	pwm.PrepareSendWants(p0, cids, cids2)

	if g.count != 2 {
		t.Fatal("Expected 2 want-blocks")
	}

	// Send 1 old want-block and 2 new want-blocks to p0
	cids3 := testutil.GenerateCids(2)
	pwm.PrepareSendWants(p0, append(cids3, cids[0]), []cid.Cid{})

	if g.count != 4 {
		t.Fatal("Expected 4 want-blocks")
	}

	// Cancel 1 want-block that was sent to p0
	// and 1 want-block that was not sent
	cids4 := testutil.GenerateCids(1)
	pwm.PrepareSendCancels(append(cids4, cids[0]))

	if g.count != 3 {
		t.Fatal("Expected 3 want-blocks", g.count)
	}
}
