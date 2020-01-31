package session

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
)

func TestSendWantBlocksTracker(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	cids := testutil.GenerateCids(2)
	swbt := newSentWantBlocksTracker()

	if swbt.haveSentWantBlockTo(peers[0], cids[0]) {
		t.Fatal("expected not to have sent anything yet")
	}

	swbt.addSentWantBlocksTo(peers[0], cids)
	if !swbt.haveSentWantBlockTo(peers[0], cids[0]) {
		t.Fatal("expected to have sent cid to peer")
	}
	if !swbt.haveSentWantBlockTo(peers[0], cids[1]) {
		t.Fatal("expected to have sent cid to peer")
	}
	if swbt.haveSentWantBlockTo(peers[1], cids[0]) {
		t.Fatal("expected not to have sent cid to peer")
	}
}
