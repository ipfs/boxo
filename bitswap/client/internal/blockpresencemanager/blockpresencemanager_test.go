package blockpresencemanager

import (
	"testing"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

const (
	expHasFalseMsg         = "Expected PeerHasBlock to return false"
	expHasTrueMsg          = "Expected PeerHasBlock to return true"
	expDoesNotHaveFalseMsg = "Expected PeerDoesNotHaveBlock to return false"
	expDoesNotHaveTrueMsg  = "Expected PeerDoesNotHaveBlock to return true"
)

func TestBlockPresenceManager(t *testing.T) {
	bpm := New()

	p := random.Peers(1)[0]
	cids := random.Cids(2)
	c0 := cids[0]
	c1 := cids[1]

	// Nothing stored yet, both PeerHasBlock and PeerDoesNotHaveBlock should
	// return false
	require.False(t, bpm.PeerHasBlock(p, c0), expHasFalseMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p, c0), expDoesNotHaveFalseMsg)

	// HAVE cid0 / DONT_HAVE cid1
	bpm.ReceiveFrom(p, []cid.Cid{c0}, []cid.Cid{c1})

	// Peer has received HAVE for cid0
	require.True(t, bpm.PeerHasBlock(p, c0), expHasTrueMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p, c0), expDoesNotHaveFalseMsg)

	// Peer has received DONT_HAVE for cid1
	require.True(t, bpm.PeerDoesNotHaveBlock(p, c1), expDoesNotHaveTrueMsg)
	require.False(t, bpm.PeerHasBlock(p, c1), expHasFalseMsg)

	// HAVE cid1 / DONT_HAVE cid0
	bpm.ReceiveFrom(p, []cid.Cid{c1}, []cid.Cid{c0})

	// DONT_HAVE cid0 should NOT over-write earlier HAVE cid0
	require.False(t, bpm.PeerDoesNotHaveBlock(p, c0), expDoesNotHaveFalseMsg)
	require.True(t, bpm.PeerHasBlock(p, c0), expHasTrueMsg)

	// HAVE cid1 should over-write earlier DONT_HAVE cid1
	require.True(t, bpm.PeerHasBlock(p, c1), expHasTrueMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p, c1), expDoesNotHaveFalseMsg)

	// Remove cid0
	bpm.RemoveKeys([]cid.Cid{c0})

	// Nothing stored, both PeerHasBlock and PeerDoesNotHaveBlock should
	// return false
	require.False(t, bpm.PeerHasBlock(p, c0), expHasFalseMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p, c0), expDoesNotHaveFalseMsg)

	// Remove cid1
	bpm.RemoveKeys([]cid.Cid{c1})

	// Nothing stored, both PeerHasBlock and PeerDoesNotHaveBlock should
	// return false
	require.False(t, bpm.PeerHasBlock(p, c1), expHasFalseMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p, c1), expDoesNotHaveFalseMsg)

	bpm.ReceiveFrom(p, []cid.Cid{c0}, []cid.Cid{c1})
	require.True(t, bpm.PeerHasBlock(p, c0), expHasTrueMsg)
	require.True(t, bpm.PeerDoesNotHaveBlock(p, c1), expDoesNotHaveTrueMsg)

	bpm.RemovePeer(p)
	require.False(t, bpm.PeerHasBlock(p, c0), expHasFalseMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p, c0), expDoesNotHaveFalseMsg)
	require.False(t, bpm.PeerHasBlock(p, c1), expHasFalseMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p, c1), expDoesNotHaveFalseMsg)
}

func TestAddRemoveMulti(t *testing.T) {
	bpm := New()

	peers := random.Peers(2)
	p0 := peers[0]
	p1 := peers[1]
	cids := random.Cids(3)
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
	require.True(t, bpm.PeerHasBlock(p0, c0), expHasTrueMsg)
	require.True(t, bpm.PeerHasBlock(p0, c1), expHasTrueMsg)
	require.True(t, bpm.PeerDoesNotHaveBlock(p0, c2), expDoesNotHaveTrueMsg)

	// Peer 1 should end up with
	// - HAVE cid1
	// - HAVE cid2
	// - DONT_HAVE cid0
	require.True(t, bpm.PeerHasBlock(p1, c1), expHasTrueMsg)
	require.True(t, bpm.PeerHasBlock(p1, c2), expHasTrueMsg)
	require.True(t, bpm.PeerDoesNotHaveBlock(p1, c0), expDoesNotHaveTrueMsg)

	// Remove cid1 and cid2. Should end up with
	// Peer 0: HAVE cid0
	// Peer 1: DONT_HAVE cid0
	bpm.RemoveKeys([]cid.Cid{c1, c2})
	require.True(t, bpm.PeerHasBlock(p0, c0), expHasTrueMsg)
	require.True(t, bpm.PeerDoesNotHaveBlock(p1, c0), expDoesNotHaveTrueMsg)

	// The other keys should have been cleared, so both HasBlock() and
	// DoesNotHaveBlock() should return false
	require.False(t, bpm.PeerHasBlock(p0, c1), expHasFalseMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p0, c1), expDoesNotHaveFalseMsg)
	require.False(t, bpm.PeerHasBlock(p0, c2), expHasFalseMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p0, c2), expDoesNotHaveFalseMsg)
	require.False(t, bpm.PeerHasBlock(p1, c1), expHasFalseMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p1, c1), expDoesNotHaveFalseMsg)
	require.False(t, bpm.PeerHasBlock(p1, c2), expHasFalseMsg)
	require.False(t, bpm.PeerDoesNotHaveBlock(p1, c2), expDoesNotHaveFalseMsg)
}

func TestAllPeersDoNotHaveBlock(t *testing.T) {
	bpm := New()

	peers := random.Peers(3)
	p0 := peers[0]
	p1 := peers[1]
	p2 := peers[2]

	cids := random.Cids(3)
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

		// p0 received DONT_HAVE for c1 & c2 (but not for c0)
		{[]peer.ID{p0}, []cid.Cid{c0, c1, c2}, []cid.Cid{c1, c2}},
		{[]peer.ID{p0, p1}, []cid.Cid{c0, c1, c2}, []cid.Cid{}},
		// Both p0 and p2 received DONT_HAVE for c2
		{[]peer.ID{p0, p2}, []cid.Cid{c0, c1, c2}, []cid.Cid{c2}},
		{[]peer.ID{p0, p1, p2}, []cid.Cid{c0, c1, c2}, []cid.Cid{}},
	}

	for i, tc := range testcases {
		require.ElementsMatchf(t, bpm.AllPeersDoNotHaveBlock(tc.peers, tc.ks), tc.exp,
			"test case %d failed: expected matching keys", i)
	}
}
