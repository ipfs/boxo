package blockpresencemanager

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
	peer "github.com/libp2p/go-libp2p/core/peer"

	cid "github.com/ipfs/go-cid"
)

const (
	expHasFalseMsg         = "Expected PeerHasBlock to return false"
	expHasTrueMsg          = "Expected PeerHasBlock to return true"
	expDoesNotHaveFalseMsg = "Expected PeerDoesNotHaveBlock to return false"
	expDoesNotHaveTrueMsg  = "Expected PeerDoesNotHaveBlock to return true"
)

func TestBlockPresenceManager(t *testing.T) {
	bpm := New()

	p := testutil.GeneratePeers(1)[0]
	cids := testutil.GenerateCids(2)
	c0 := cids[0]
	c1 := cids[1]

	// Nothing stored yet, both PeerHasBlock and PeerDoesNotHaveBlock should
	// return false
	if bpm.PeerHasBlock(p, c0) {
		t.Fatal(expHasFalseMsg)
	}
	if bpm.PeerDoesNotHaveBlock(p, c0) {
		t.Fatal(expDoesNotHaveFalseMsg)
	}

	// HAVE cid0 / DONT_HAVE cid1
	bpm.ReceiveFrom(p, []cid.Cid{c0}, []cid.Cid{c1})

	// Peer has received HAVE for cid0
	if !bpm.PeerHasBlock(p, c0) {
		t.Fatal(expHasTrueMsg)
	}
	if bpm.PeerDoesNotHaveBlock(p, c0) {
		t.Fatal(expDoesNotHaveFalseMsg)
	}

	// Peer has received DONT_HAVE for cid1
	if !bpm.PeerDoesNotHaveBlock(p, c1) {
		t.Fatal(expDoesNotHaveTrueMsg)
	}
	if bpm.PeerHasBlock(p, c1) {
		t.Fatal(expHasFalseMsg)
	}

	// HAVE cid1 / DONT_HAVE cid0
	bpm.ReceiveFrom(p, []cid.Cid{c1}, []cid.Cid{c0})

	// DONT_HAVE cid0 should NOT over-write earlier HAVE cid0
	if bpm.PeerDoesNotHaveBlock(p, c0) {
		t.Fatal(expDoesNotHaveFalseMsg)
	}
	if !bpm.PeerHasBlock(p, c0) {
		t.Fatal(expHasTrueMsg)
	}

	// HAVE cid1 should over-write earlier DONT_HAVE cid1
	if !bpm.PeerHasBlock(p, c1) {
		t.Fatal(expHasTrueMsg)
	}
	if bpm.PeerDoesNotHaveBlock(p, c1) {
		t.Fatal(expDoesNotHaveFalseMsg)
	}

	// Remove cid0
	bpm.RemoveKeys([]cid.Cid{c0})

	// Nothing stored, both PeerHasBlock and PeerDoesNotHaveBlock should
	// return false
	if bpm.PeerHasBlock(p, c0) {
		t.Fatal(expHasFalseMsg)
	}
	if bpm.PeerDoesNotHaveBlock(p, c0) {
		t.Fatal(expDoesNotHaveFalseMsg)
	}

	// Remove cid1
	bpm.RemoveKeys([]cid.Cid{c1})

	// Nothing stored, both PeerHasBlock and PeerDoesNotHaveBlock should
	// return false
	if bpm.PeerHasBlock(p, c1) {
		t.Fatal(expHasFalseMsg)
	}
	if bpm.PeerDoesNotHaveBlock(p, c1) {
		t.Fatal(expDoesNotHaveFalseMsg)
	}
}

func TestAddRemoveMulti(t *testing.T) {
	bpm := New()

	peers := testutil.GeneratePeers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := testutil.GenerateCids(3)
	c0 := cids[0]
	c1 := cids[1]
	c2 := cids[2]

	// p0: HAVE cid0, cid1 / DONT_HAVE cid1, cid2
	// p1: HAVE cid1, cid2 / DONT_HAVE cid0
	bpm.ReceiveFrom(p0, []cid.Cid{c0, c1}, []cid.Cid{c1, c2})
	bpm.ReceiveFrom(p1, []cid.Cid{c1, c2}, []cid.Cid{c0})

	// Peer 0 should end up with
	// - HAVE cid0
	// - HAVE cid1
	// - DONT_HAVE cid2
	if !bpm.PeerHasBlock(p0, c0) {
		t.Fatal(expHasTrueMsg)
	}
	if !bpm.PeerHasBlock(p0, c1) {
		t.Fatal(expHasTrueMsg)
	}
	if !bpm.PeerDoesNotHaveBlock(p0, c2) {
		t.Fatal(expDoesNotHaveTrueMsg)
	}

	// Peer 1 should end up with
	// - HAVE cid1
	// - HAVE cid2
	// - DONT_HAVE cid0
	if !bpm.PeerHasBlock(p1, c1) {
		t.Fatal(expHasTrueMsg)
	}
	if !bpm.PeerHasBlock(p1, c2) {
		t.Fatal(expHasTrueMsg)
	}
	if !bpm.PeerDoesNotHaveBlock(p1, c0) {
		t.Fatal(expDoesNotHaveTrueMsg)
	}

	// Remove cid1 and cid2. Should end up with
	// Peer 0: HAVE cid0
	// Peer 1: DONT_HAVE cid0
	bpm.RemoveKeys([]cid.Cid{c1, c2})
	if !bpm.PeerHasBlock(p0, c0) {
		t.Fatal(expHasTrueMsg)
	}
	if !bpm.PeerDoesNotHaveBlock(p1, c0) {
		t.Fatal(expDoesNotHaveTrueMsg)
	}

	// The other keys should have been cleared, so both HasBlock() and
	// DoesNotHaveBlock() should return false
	if bpm.PeerHasBlock(p0, c1) {
		t.Fatal(expHasFalseMsg)
	}
	if bpm.PeerDoesNotHaveBlock(p0, c1) {
		t.Fatal(expDoesNotHaveFalseMsg)
	}
	if bpm.PeerHasBlock(p0, c2) {
		t.Fatal(expHasFalseMsg)
	}
	if bpm.PeerDoesNotHaveBlock(p0, c2) {
		t.Fatal(expDoesNotHaveFalseMsg)
	}
	if bpm.PeerHasBlock(p1, c1) {
		t.Fatal(expHasFalseMsg)
	}
	if bpm.PeerDoesNotHaveBlock(p1, c1) {
		t.Fatal(expDoesNotHaveFalseMsg)
	}
	if bpm.PeerHasBlock(p1, c2) {
		t.Fatal(expHasFalseMsg)
	}
	if bpm.PeerDoesNotHaveBlock(p1, c2) {
		t.Fatal(expDoesNotHaveFalseMsg)
	}
}

func TestAllPeersDoNotHaveBlock(t *testing.T) {
	bpm := New()

	peers := testutil.GeneratePeers(3)
	p0 := peers[0]
	p1 := peers[1]
	p2 := peers[2]

	cids := testutil.GenerateCids(3)
	c0 := cids[0]
	c1 := cids[1]
	c2 := cids[2]

	//      c0  c1  c2
	//  p0   ?  N   N
	//  p1   N  Y   ?
	//  p2   Y  Y   N
	bpm.ReceiveFrom(p0, []cid.Cid{}, []cid.Cid{c1, c2})
	bpm.ReceiveFrom(p1, []cid.Cid{c1}, []cid.Cid{c0})
	bpm.ReceiveFrom(p2, []cid.Cid{c0, c1}, []cid.Cid{c2})

	type testcase struct {
		peers []peer.ID
		ks    []cid.Cid
		exp   []cid.Cid
	}

	testcases := []testcase{
		{[]peer.ID{p0}, []cid.Cid{c0}, []cid.Cid{}},
		{[]peer.ID{p1}, []cid.Cid{c0}, []cid.Cid{c0}},
		{[]peer.ID{p2}, []cid.Cid{c0}, []cid.Cid{}},

		{[]peer.ID{p0}, []cid.Cid{c1}, []cid.Cid{c1}},
		{[]peer.ID{p1}, []cid.Cid{c1}, []cid.Cid{}},
		{[]peer.ID{p2}, []cid.Cid{c1}, []cid.Cid{}},

		{[]peer.ID{p0}, []cid.Cid{c2}, []cid.Cid{c2}},
		{[]peer.ID{p1}, []cid.Cid{c2}, []cid.Cid{}},
		{[]peer.ID{p2}, []cid.Cid{c2}, []cid.Cid{c2}},

		// p0 recieved DONT_HAVE for c1 & c2 (but not for c0)
		{[]peer.ID{p0}, []cid.Cid{c0, c1, c2}, []cid.Cid{c1, c2}},
		{[]peer.ID{p0, p1}, []cid.Cid{c0, c1, c2}, []cid.Cid{}},
		// Both p0 and p2 received DONT_HAVE for c2
		{[]peer.ID{p0, p2}, []cid.Cid{c0, c1, c2}, []cid.Cid{c2}},
		{[]peer.ID{p0, p1, p2}, []cid.Cid{c0, c1, c2}, []cid.Cid{}},
	}

	for i, tc := range testcases {
		if !testutil.MatchKeysIgnoreOrder(
			bpm.AllPeersDoNotHaveBlock(tc.peers, tc.ks),
			tc.exp,
		) {
			t.Fatalf("test case %d failed: expected matching keys", i)
		}
	}
}
