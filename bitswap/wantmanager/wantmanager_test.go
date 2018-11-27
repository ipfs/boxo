package wantmanager

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/ipfs/go-bitswap/testutil"

	bsmsg "github.com/ipfs/go-bitswap/message"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-peer"
)

type fakeWantSender struct {
	lk          sync.RWMutex
	lastWantSet wantSet
}

func (fws *fakeWantSender) SendMessage(entries []*bsmsg.Entry, targets []peer.ID, from uint64) {
	fws.lk.Lock()
	fws.lastWantSet = wantSet{entries, targets, from}
	fws.lk.Unlock()
}

func (fws *fakeWantSender) getLastWantSet() wantSet {
	fws.lk.Lock()
	defer fws.lk.Unlock()
	return fws.lastWantSet
}

func setupTestFixturesAndInitialWantList() (
	context.Context, *fakeWantSender, *WantManager, []cid.Cid, []cid.Cid, []peer.ID, uint64, uint64) {
	ctx := context.Background()

	// setup fixtures
	wantSender := &fakeWantSender{}
	wantManager := New(ctx)
	keys := testutil.GenerateCids(10)
	otherKeys := testutil.GenerateCids(5)
	peers := testutil.GeneratePeers(10)
	session := testutil.GenerateSessionID()
	otherSession := testutil.GenerateSessionID()

	// startup wantManager
	wantManager.SetDelegate(wantSender)
	wantManager.Startup()

	// add initial wants
	wantManager.WantBlocks(
		ctx,
		keys,
		peers,
		session)

	return ctx, wantSender, wantManager, keys, otherKeys, peers, session, otherSession
}

func TestInitialWantsAddedCorrectly(t *testing.T) {

	_, wantSender, wantManager, keys, _, peers, session, _ :=
		setupTestFixturesAndInitialWantList()

	bcwl := wantManager.CurrentBroadcastWants()
	wl := wantManager.CurrentWants()

	if len(bcwl) > 0 {
		t.Fatal("should not create broadcast wants when peers are specified")
	}

	if len(wl) != len(keys) {
		t.Fatal("did not add correct number of wants to want lsit")
	}

	generatedWantSet := wantSender.getLastWantSet()

	if len(generatedWantSet.entries) != len(keys) {
		t.Fatal("incorrect wants sent")
	}

	for _, entry := range generatedWantSet.entries {
		if entry.Cancel {
			t.Fatal("did not send only non-cancel messages")
		}
	}

	if generatedWantSet.from != session {
		t.Fatal("incorrect session used in sending")
	}

	if !reflect.DeepEqual(generatedWantSet.targets, peers) {
		t.Fatal("did not setup peers correctly")
	}

	wantManager.Shutdown()
}

func TestCancellingWants(t *testing.T) {
	ctx, wantSender, wantManager, keys, _, peers, session, _ :=
		setupTestFixturesAndInitialWantList()

	wantManager.CancelWants(ctx, keys, peers, session)

	wl := wantManager.CurrentWants()

	if len(wl) != 0 {
		t.Fatal("did not remove blocks from want list")
	}

	generatedWantSet := wantSender.getLastWantSet()

	if len(generatedWantSet.entries) != len(keys) {
		t.Fatal("incorrect wants sent")
	}

	for _, entry := range generatedWantSet.entries {
		if !entry.Cancel {
			t.Fatal("did not send only cancel messages")
		}
	}

	if generatedWantSet.from != session {
		t.Fatal("incorrect session used in sending")
	}

	if !reflect.DeepEqual(generatedWantSet.targets, peers) {
		t.Fatal("did not setup peers correctly")
	}

	wantManager.Shutdown()

}

func TestCancellingWantsFromAnotherSessionHasNoEffect(t *testing.T) {
	ctx, _, wantManager, keys, _, peers, _, otherSession :=
		setupTestFixturesAndInitialWantList()

	// cancelling wants from another session has no effect
	wantManager.CancelWants(ctx, keys, peers, otherSession)

	wl := wantManager.CurrentWants()

	if len(wl) != len(keys) {
		t.Fatal("should not cancel wants unless they match session that made them")
	}

	wantManager.Shutdown()
}

func TestAddingWantsWithNoPeersAddsToBroadcastAndRegularWantList(t *testing.T) {
	ctx, _, wantManager, keys, otherKeys, _, session, _ :=
		setupTestFixturesAndInitialWantList()

	wantManager.WantBlocks(ctx, otherKeys, nil, session)

	bcwl := wantManager.CurrentBroadcastWants()
	wl := wantManager.CurrentWants()

	if len(bcwl) != len(otherKeys) {
		t.Fatal("want requests with no peers should get added to broadcast list")
	}

	if len(wl) != len(otherKeys)+len(keys) {
		t.Fatal("want requests with no peers should get added to regular want list")
	}

	wantManager.Shutdown()
}

func TestAddingRequestFromSecondSessionPreventsCancel(t *testing.T) {
	ctx, wantSender, wantManager, keys, _, peers, session, otherSession :=
		setupTestFixturesAndInitialWantList()

	// add a second session requesting the first key
	firstKeys := append([]cid.Cid(nil), keys[0])
	wantManager.WantBlocks(ctx, firstKeys, peers, otherSession)

	wl := wantManager.CurrentWants()

	if len(wl) != len(keys) {
		t.Fatal("wants from other sessions should not get added seperately")
	}

	generatedWantSet := wantSender.getLastWantSet()
	if len(generatedWantSet.entries) != len(firstKeys) &&
		generatedWantSet.from != otherSession &&
		generatedWantSet.entries[0].Cid != firstKeys[0] &&
		generatedWantSet.entries[0].Cancel != false {
		t.Fatal("should send additional message requesting want for new session")
	}

	// cancel block from first session
	wantManager.CancelWants(ctx, firstKeys, peers, session)

	wl = wantManager.CurrentWants()

	// want should still be on want list
	if len(wl) != len(keys) {
		t.Fatal("wants should not be removed until all sessions cancel wants")
	}

	// cancel other block from first session
	secondKeys := append([]cid.Cid(nil), keys[1])
	wantManager.CancelWants(ctx, secondKeys, peers, session)

	wl = wantManager.CurrentWants()

	// want should not be on want list, cause it was only tracked by one session
	if len(wl) != len(keys)-1 {
		t.Fatal("wants should be removed if all sessions have cancelled")
	}

	wantManager.Shutdown()
}
