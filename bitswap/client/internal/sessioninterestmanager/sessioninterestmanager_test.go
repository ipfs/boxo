package sessioninterestmanager

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"
)

func TestEmpty(t *testing.T) {
	sim := New()

	ses := uint64(1)
	cids := testutil.GenerateCids(2)
	res := sim.FilterSessionInterested(ses, cids)
	if len(res) != 1 || len(res[0]) > 0 {
		t.Fatal("Expected no interest")
	}
	if len(sim.InterestedSessions(cids, []cid.Cid{}, []cid.Cid{})) > 0 {
		t.Fatal("Expected no interest")
	}
}

func TestBasic(t *testing.T) {
	sim := New()

	ses1 := uint64(1)
	ses2 := uint64(2)
	cids1 := testutil.GenerateCids(2)
	cids2 := append(testutil.GenerateCids(1), cids1[1])
	sim.RecordSessionInterest(ses1, cids1)

	res := sim.FilterSessionInterested(ses1, cids1)
	if len(res) != 1 || len(res[0]) != 2 {
		t.Fatal("Expected 2 keys")
	}
	if len(sim.InterestedSessions(cids1, []cid.Cid{}, []cid.Cid{})) != 1 {
		t.Fatal("Expected 1 session")
	}

	sim.RecordSessionInterest(ses2, cids2)
	res = sim.FilterSessionInterested(ses2, cids1[:1])
	if len(res) != 1 || len(res[0]) != 0 {
		t.Fatal("Expected no interest")
	}
	res = sim.FilterSessionInterested(ses2, cids2)
	if len(res) != 1 || len(res[0]) != 2 {
		t.Fatal("Expected 2 keys")
	}

	if len(sim.InterestedSessions(cids1[:1], []cid.Cid{}, []cid.Cid{})) != 1 {
		t.Fatal("Expected 1 session")
	}
	if len(sim.InterestedSessions(cids1[1:], []cid.Cid{}, []cid.Cid{})) != 2 {
		t.Fatal("Expected 2 sessions")
	}
}

func TestInterestedSessions(t *testing.T) {
	sim := New()

	ses := uint64(1)
	cids := testutil.GenerateCids(3)
	sim.RecordSessionInterest(ses, cids[0:2])

	if len(sim.InterestedSessions(cids, []cid.Cid{}, []cid.Cid{})) != 1 {
		t.Fatal("Expected 1 session")
	}
	if len(sim.InterestedSessions(cids[0:1], []cid.Cid{}, []cid.Cid{})) != 1 {
		t.Fatal("Expected 1 session")
	}
	if len(sim.InterestedSessions([]cid.Cid{}, cids, []cid.Cid{})) != 1 {
		t.Fatal("Expected 1 session")
	}
	if len(sim.InterestedSessions([]cid.Cid{}, cids[0:1], []cid.Cid{})) != 1 {
		t.Fatal("Expected 1 session")
	}
	if len(sim.InterestedSessions([]cid.Cid{}, []cid.Cid{}, cids)) != 1 {
		t.Fatal("Expected 1 session")
	}
	if len(sim.InterestedSessions([]cid.Cid{}, []cid.Cid{}, cids[0:1])) != 1 {
		t.Fatal("Expected 1 session")
	}
}

func TestRemoveSession(t *testing.T) {
	sim := New()

	ses1 := uint64(1)
	ses2 := uint64(2)
	cids1 := testutil.GenerateCids(2)
	cids2 := append(testutil.GenerateCids(1), cids1[1])
	sim.RecordSessionInterest(ses1, cids1)
	sim.RecordSessionInterest(ses2, cids2)
	sim.RemoveSession(ses1)

	res := sim.FilterSessionInterested(ses1, cids1)
	if len(res) != 1 || len(res[0]) != 0 {
		t.Fatal("Expected no interest")
	}

	res = sim.FilterSessionInterested(ses2, cids1, cids2)
	if len(res) != 2 {
		t.Fatal("unexpected results size")
	}
	if len(res[0]) != 1 {
		t.Fatal("Expected 1 key")
	}
	if len(res[1]) != 2 {
		t.Fatal("Expected 2 keys")
	}
}

func TestRemoveSessionInterested(t *testing.T) {
	sim := New()

	ses1 := uint64(1)
	ses2 := uint64(2)
	cids1 := testutil.GenerateCids(2)
	cids2 := append(testutil.GenerateCids(1), cids1[1])
	sim.RecordSessionInterest(ses1, cids1)
	sim.RecordSessionInterest(ses2, cids2)

	res := sim.RemoveSessionInterested(ses1, []cid.Cid{cids1[0]})
	if len(res) != 1 {
		t.Fatal("Expected no interested sessions left")
	}

	interested := sim.FilterSessionInterested(ses1, cids1)
	if len(interested) != 1 || len(interested[0]) != 1 {
		t.Fatal("Expected ses1 still interested in one cid")
	}

	res = sim.RemoveSessionInterested(ses1, cids1)
	if len(res) != 0 {
		t.Fatal("Expected ses2 to be interested in one cid")
	}

	interested = sim.FilterSessionInterested(ses1, cids1)
	if len(interested) != 1 || len(interested[0]) != 0 {
		t.Fatal("Expected ses1 to have no remaining interest")
	}

	interested = sim.FilterSessionInterested(ses2, cids1)
	if len(interested) != 1 || len(interested[0]) != 1 {
		t.Fatal("Expected ses2 to still be interested in one key")
	}
}

func TestSplitWantedUnwanted(t *testing.T) {
	blks := testutil.GenerateBlocksOfSize(3, 1024)
	sim := New()
	ses1 := uint64(1)
	ses2 := uint64(2)

	var cids []cid.Cid
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	// ses1: <none>
	// ses2: <none>
	wanted, unwanted := sim.SplitWantedUnwanted(blks)
	if len(wanted) > 0 {
		t.Fatal("Expected no blocks")
	}
	if len(unwanted) != 3 {
		t.Fatal("Expected 3 blocks")
	}

	// ses1: 0 1
	// ses2: <none>
	sim.RecordSessionInterest(ses1, cids[0:2])
	wanted, unwanted = sim.SplitWantedUnwanted(blks)
	if len(wanted) != 2 {
		t.Fatal("Expected 2 blocks")
	}
	if len(unwanted) != 1 {
		t.Fatal("Expected 1 block")
	}

	// ses1: 1
	// ses2: 1 2
	sim.RecordSessionInterest(ses2, cids[1:])
	sim.RemoveSessionWants(ses1, cids[:1])

	wanted, unwanted = sim.SplitWantedUnwanted(blks)
	if len(wanted) != 2 {
		t.Fatal("Expected 2 blocks")
	}
	if len(unwanted) != 1 {
		t.Fatal("Expected no blocks")
	}

	// ses1: <none>
	// ses2: 1 2
	sim.RemoveSessionWants(ses1, cids[1:2])

	wanted, unwanted = sim.SplitWantedUnwanted(blks)
	if len(wanted) != 2 {
		t.Fatal("Expected 2 blocks")
	}
	if len(unwanted) != 1 {
		t.Fatal("Expected no blocks")
	}

	// ses1: <none>
	// ses2: 2
	sim.RemoveSessionWants(ses2, cids[1:2])

	wanted, unwanted = sim.SplitWantedUnwanted(blks)
	if len(wanted) != 1 {
		t.Fatal("Expected 2 blocks")
	}
	if len(unwanted) != 2 {
		t.Fatal("Expected 2 blocks")
	}
}
