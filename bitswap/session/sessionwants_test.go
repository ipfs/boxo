package session

import (
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"
	cid "github.com/ipfs/go-cid"
)

func TestSessionWants(t *testing.T) {
	sw := sessionWants{
		toFetch:   newCidQueue(),
		liveWants: make(map[cid.Cid]time.Time),
		pastWants: cid.NewSet(),
	}
	cids := testutil.GenerateCids(10)
	others := testutil.GenerateCids(1)

	// Expect these functions to return nothing on a new sessionWants
	lws := sw.PrepareBroadcast()
	if len(lws) > 0 {
		t.Fatal("expected no broadcast wants")
	}
	lws = sw.LiveWants()
	if len(lws) > 0 {
		t.Fatal("expected no live wants")
	}
	if sw.HasLiveWants() {
		t.Fatal("expected not to have live wants")
	}
	rw := sw.RandomLiveWant()
	if rw.Defined() {
		t.Fatal("expected no random want")
	}
	if sw.IsWanted(cids[0]) {
		t.Fatal("expected cid to not be wanted")
	}
	if len(sw.FilterInteresting(cids)) > 0 {
		t.Fatal("expected no interesting wants")
	}

	// Add 10 new wants with a limit of 5
	// The first 5 cids should go into the toFetch queue
	// The other 5 cids should go into the live want queue
	//  toFetch   Live    Past
	//   98765    43210
	nextw := sw.GetNextWants(5, cids)
	if len(nextw) != 5 {
		t.Fatal("expected 5 next wants")
	}
	lws = sw.PrepareBroadcast()
	if len(lws) != 5 {
		t.Fatal("expected 5 broadcast wants")
	}
	lws = sw.LiveWants()
	if len(lws) != 5 {
		t.Fatal("expected 5 live wants")
	}
	if !sw.HasLiveWants() {
		t.Fatal("expected to have live wants")
	}
	rw = sw.RandomLiveWant()
	if !rw.Defined() {
		t.Fatal("expected random want")
	}
	if !sw.IsWanted(cids[0]) {
		t.Fatal("expected cid to be wanted")
	}
	if !sw.IsWanted(cids[9]) {
		t.Fatal("expected cid to be wanted")
	}
	if len(sw.FilterInteresting([]cid.Cid{cids[0], cids[9], others[0]})) != 2 {
		t.Fatal("expected 2 interesting wants")
	}

	// Two wanted blocks and one other block are received.
	// The wanted blocks should be moved from the live wants queue
	// to the past wants set (the other block CID should be ignored)
	//  toFetch   Live    Past
	//   98765    432__   10
	recvdCids := []cid.Cid{cids[0], cids[1], others[0]}
	uniq := 0
	dup := 0
	sw.ForEachUniqDup(recvdCids, func() { uniq++ }, func() { dup++ })
	if uniq != 2 || dup != 0 {
		t.Fatal("expected 2 uniqs / 0 dups", uniq, dup)
	}
	sw.BlocksReceived(recvdCids)
	lws = sw.LiveWants()
	if len(lws) != 3 {
		t.Fatal("expected 3 live wants")
	}
	if sw.IsWanted(cids[0]) {
		t.Fatal("expected cid to no longer be wanted")
	}
	if !sw.IsWanted(cids[9]) {
		t.Fatal("expected cid to be wanted")
	}
	if len(sw.FilterInteresting([]cid.Cid{cids[0], cids[9], others[0]})) != 2 {
		t.Fatal("expected 2 interesting wants")
	}

	// Ask for next wants with a limit of 5
	// Should move 2 wants from toFetch queue to live wants
	//  toFetch   Live    Past
	//   987__    65432   10
	nextw = sw.GetNextWants(5, nil)
	if len(nextw) != 2 {
		t.Fatal("expected 2 next wants")
	}
	lws = sw.LiveWants()
	if len(lws) != 5 {
		t.Fatal("expected 5 live wants")
	}
	if !sw.IsWanted(cids[5]) {
		t.Fatal("expected cid to be wanted")
	}

	// One wanted block and one dup block are received.
	// The wanted block should be moved from the live wants queue
	// to the past wants set
	//  toFetch   Live    Past
	//   987      654_2   310
	recvdCids = []cid.Cid{cids[0], cids[3]}
	uniq = 0
	dup = 0
	sw.ForEachUniqDup(recvdCids, func() { uniq++ }, func() { dup++ })
	if uniq != 1 || dup != 1 {
		t.Fatal("expected 1 uniq / 1 dup", uniq, dup)
	}
	sw.BlocksReceived(recvdCids)
	lws = sw.LiveWants()
	if len(lws) != 4 {
		t.Fatal("expected 4 live wants")
	}

	// One block in the toFetch queue should be cancelled
	//  toFetch   Live    Past
	//   9_7      654_2   310
	sw.CancelPending([]cid.Cid{cids[8]})
	lws = sw.LiveWants()
	if len(lws) != 4 {
		t.Fatal("expected 4 live wants")
	}
	if sw.IsWanted(cids[8]) {
		t.Fatal("expected cid to no longer be wanted")
	}
	if len(sw.FilterInteresting([]cid.Cid{cids[0], cids[8]})) != 1 {
		t.Fatal("expected 1 interesting wants")
	}
}
