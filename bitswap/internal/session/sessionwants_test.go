package session

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"
)

func TestEmptySessionWants(t *testing.T) {
	sw := newSessionWants(broadcastLiveWantsLimit)

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
}

func TestSessionWants(t *testing.T) {
	sw := newSessionWants(5)
	cids := testutil.GenerateCids(10)
	others := testutil.GenerateCids(1)

	// Add 10 new wants
	//  toFetch    Live
	// 9876543210
	sw.BlocksRequested(cids)

	// Get next wants with a limit of 5
	// The first 5 cids should go move into the live queue
	//  toFetch   Live
	//   98765    43210
	nextw := sw.GetNextWants()
	if len(nextw) != 5 {
		t.Fatal("expected 5 next wants")
	}
	lws := sw.PrepareBroadcast()
	if len(lws) != 5 {
		t.Fatal("expected 5 broadcast wants", len(lws))
	}
	lws = sw.LiveWants()
	if len(lws) != 5 {
		t.Fatal("expected 5 live wants")
	}
	if !sw.HasLiveWants() {
		t.Fatal("expected to have live wants")
	}
	rw := sw.RandomLiveWant()
	if !rw.Defined() {
		t.Fatal("expected random want")
	}

	// Two wanted blocks and one other block are received.
	// The wanted blocks should be removed from the live wants queue
	// (the other block CID should be ignored)
	//  toFetch   Live
	//   98765    432__
	recvdCids := []cid.Cid{cids[0], cids[1], others[0]}
	sw.BlocksReceived(recvdCids)
	lws = sw.LiveWants()
	if len(lws) != 3 {
		t.Fatal("expected 3 live wants")
	}

	// Ask for next wants with a limit of 5
	// Should move 2 wants from toFetch queue to live wants
	//  toFetch   Live
	//   987__    65432
	nextw = sw.GetNextWants()
	if len(nextw) != 2 {
		t.Fatal("expected 2 next wants")
	}
	lws = sw.LiveWants()
	if len(lws) != 5 {
		t.Fatal("expected 5 live wants")
	}

	// One wanted block and one dup block are received.
	// The wanted block should be removed from the live
	// wants queue.
	//  toFetch   Live
	//   987      654_2
	recvdCids = []cid.Cid{cids[0], cids[3]}
	sw.BlocksReceived(recvdCids)
	lws = sw.LiveWants()
	if len(lws) != 4 {
		t.Fatal("expected 4 live wants")
	}

	// One block in the toFetch queue should be cancelled
	//  toFetch   Live
	//   9_7      654_2
	sw.CancelPending([]cid.Cid{cids[8]})
	lws = sw.LiveWants()
	if len(lws) != 4 {
		t.Fatal("expected 4 live wants")
	}
}

func TestPrepareBroadcast(t *testing.T) {
	sw := newSessionWants(3)
	cids := testutil.GenerateCids(10)

	// Add 6 new wants
	//  toFetch    Live
	//  543210
	sw.BlocksRequested(cids[:6])

	// Get next wants with a limit of 3
	// The first 3 cids should go move into the live queue
	//  toFetch   Live
	//  543       210
	sw.GetNextWants()

	// Broadcast should contain wants in order
	for i := 0; i < 10; i++ {
		ws := sw.PrepareBroadcast()
		if len(ws) != 3 {
			t.Fatal("should broadcast all live wants")
		}
		for idx, c := range ws {
			if !c.Equals(cids[idx]) {
				t.Fatal("broadcast should always return wants in order")
			}
		}
	}

	// One block received
	// Remove a cid from the live queue
	sw.BlocksReceived(cids[:1])
	//  toFetch    Live
	//  543        21_

	// Add 4 new wants
	//  toFetch    Live
	//  9876543    21
	sw.BlocksRequested(cids[6:])

	// 2 Wants sent
	//  toFetch    Live
	//  98765      4321
	sw.WantsSent(cids[3:5])

	// Broadcast should contain wants in order
	cids = cids[1:]
	for i := 0; i < 10; i++ {
		ws := sw.PrepareBroadcast()
		if len(ws) != 3 {
			t.Fatal("should broadcast live wants up to limit", len(ws), len(cids))
		}
		for idx, c := range ws {
			if !c.Equals(cids[idx]) {
				t.Fatal("broadcast should always return wants in order")
			}
		}
	}
}

// Test that even after GC broadcast returns correct wants
func TestPrepareBroadcastAfterGC(t *testing.T) {
	sw := newSessionWants(5)
	cids := testutil.GenerateCids(liveWantsOrderGCLimit * 2)

	sw.BlocksRequested(cids)

	// Trigger a sessionWants internal GC of the live wants
	sw.BlocksReceived(cids[:liveWantsOrderGCLimit+1])
	cids = cids[:liveWantsOrderGCLimit+1]

	// Broadcast should contain wants in order
	ws := sw.PrepareBroadcast()
	for i, c := range ws {
		if !c.Equals(cids[i]) {
			t.Fatal("broadcast should always return wants in order")
		}
	}
}
