package sessionwantlist

import (
	"os"
	"testing"

	"github.com/ipfs/go-bitswap/testutil"

	cid "github.com/ipfs/go-cid"
)

var c0 cid.Cid
var c1 cid.Cid
var c2 cid.Cid

const s0 = uint64(0)
const s1 = uint64(1)

func setup() {
	cids := testutil.GenerateCids(3)
	c0 = cids[0]
	c1 = cids[1]
	c2 = cids[2]
}

func TestMain(m *testing.M) {
	setup()
	os.Exit(m.Run())
}

func TestEmpty(t *testing.T) {
	swl := NewSessionWantlist()

	if len(swl.Keys()) != 0 {
		t.Fatal("Expected Keys() to be empty")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 0 {
		t.Fatal("Expected SessionsFor() to be empty")
	}
}

func TestSimpleAdd(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0
	swl.Add([]cid.Cid{c0}, s0)
	if len(swl.Keys()) != 1 {
		t.Fatal("Expected Keys() to have length 1")
	}
	if !swl.Keys()[0].Equals(c0) {
		t.Fatal("Expected Keys() to be [cid0]")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 1 {
		t.Fatal("Expected SessionsFor() to have length 1")
	}
	if swl.SessionsFor([]cid.Cid{c0})[0] != s0 {
		t.Fatal("Expected SessionsFor() to be [s0]")
	}

	// s0: c0, c1
	swl.Add([]cid.Cid{c1}, s0)
	if len(swl.Keys()) != 2 {
		t.Fatal("Expected Keys() to have length 2")
	}
	if !testutil.MatchKeysIgnoreOrder(swl.Keys(), []cid.Cid{c0, c1}) {
		t.Fatal("Expected Keys() to contain [cid0, cid1]")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 1 {
		t.Fatal("Expected SessionsFor() to have length 1")
	}
	if swl.SessionsFor([]cid.Cid{c0})[0] != s0 {
		t.Fatal("Expected SessionsFor() to be [s0]")
	}

	// s0: c0, c1
	// s1: c0
	swl.Add([]cid.Cid{c0}, s1)
	if len(swl.Keys()) != 2 {
		t.Fatal("Expected Keys() to have length 2")
	}
	if !testutil.MatchKeysIgnoreOrder(swl.Keys(), []cid.Cid{c0, c1}) {
		t.Fatal("Expected Keys() to contain [cid0, cid1]")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 2 {
		t.Fatal("Expected SessionsFor() to have length 2")
	}
}

func TestMultiKeyAdd(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0, c1
	swl.Add([]cid.Cid{c0, c1}, s0)
	if len(swl.Keys()) != 2 {
		t.Fatal("Expected Keys() to have length 2")
	}
	if !testutil.MatchKeysIgnoreOrder(swl.Keys(), []cid.Cid{c0, c1}) {
		t.Fatal("Expected Keys() to contain [cid0, cid1]")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 1 {
		t.Fatal("Expected SessionsFor() to have length 1")
	}
	if swl.SessionsFor([]cid.Cid{c0})[0] != s0 {
		t.Fatal("Expected SessionsFor() to be [s0]")
	}
}

func TestSessionHas(t *testing.T) {
	swl := NewSessionWantlist()

	if swl.Has([]cid.Cid{c0, c1}).Len() > 0 {
		t.Fatal("Expected Has([c0, c1]) to be []")
	}
	if swl.SessionHas(s0, []cid.Cid{c0, c1}).Len() > 0 {
		t.Fatal("Expected SessionHas(s0, [c0, c1]) to be []")
	}

	// s0: c0
	swl.Add([]cid.Cid{c0}, s0)
	if !matchSet(swl.Has([]cid.Cid{c0, c1}), []cid.Cid{c0}) {
		t.Fatal("Expected Has([c0, c1]) to be [c0]")
	}
	if !matchSet(swl.SessionHas(s0, []cid.Cid{c0, c1}), []cid.Cid{c0}) {
		t.Fatal("Expected SessionHas(s0, [c0, c1]) to be [c0]")
	}
	if swl.SessionHas(s1, []cid.Cid{c0, c1}).Len() > 0 {
		t.Fatal("Expected SessionHas(s1, [c0, c1]) to be []")
	}

	// s0: c0, c1
	swl.Add([]cid.Cid{c1}, s0)
	if !matchSet(swl.Has([]cid.Cid{c0, c1}), []cid.Cid{c0, c1}) {
		t.Fatal("Expected Has([c0, c1]) to be [c0, c1]")
	}
	if !matchSet(swl.SessionHas(s0, []cid.Cid{c0, c1}), []cid.Cid{c0, c1}) {
		t.Fatal("Expected SessionHas(s0, [c0, c1]) to be [c0, c1]")
	}

	// s0: c0, c1
	// s1: c0
	swl.Add([]cid.Cid{c0}, s1)
	if len(swl.Keys()) != 2 {
		t.Fatal("Expected Keys() to have length 2")
	}
	if !matchSet(swl.Has([]cid.Cid{c0, c1}), []cid.Cid{c0, c1}) {
		t.Fatal("Expected Has([c0, c1]) to be [c0, c1]")
	}
	if !matchSet(swl.SessionHas(s0, []cid.Cid{c0, c1}), []cid.Cid{c0, c1}) {
		t.Fatal("Expected SessionHas(s0, [c0, c1]) to be [c0, c1]")
	}
	if !matchSet(swl.SessionHas(s1, []cid.Cid{c0, c1}), []cid.Cid{c0}) {
		t.Fatal("Expected SessionHas(s1, [c0, c1]) to be [c0]")
	}
}

func TestSimpleRemoveKeys(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0, c1
	// s1: c0
	swl.Add([]cid.Cid{c0, c1}, s0)
	swl.Add([]cid.Cid{c0}, s1)

	// s0: c1
	swl.RemoveKeys([]cid.Cid{c0})
	if len(swl.Keys()) != 1 {
		t.Fatal("Expected Keys() to have length 1")
	}
	if !swl.Keys()[0].Equals(c1) {
		t.Fatal("Expected Keys() to be [cid1]")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 0 {
		t.Fatal("Expected SessionsFor(c0) to be empty")
	}
	if len(swl.SessionsFor([]cid.Cid{c1})) != 1 {
		t.Fatal("Expected SessionsFor(c1) to have length 1")
	}
	if swl.SessionsFor([]cid.Cid{c1})[0] != s0 {
		t.Fatal("Expected SessionsFor(c1) to be [s0]")
	}
}

func TestMultiRemoveKeys(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0, c1
	// s1: c0
	swl.Add([]cid.Cid{c0, c1}, s0)
	swl.Add([]cid.Cid{c0}, s1)

	// <empty>
	swl.RemoveKeys([]cid.Cid{c0, c1})
	if len(swl.Keys()) != 0 {
		t.Fatal("Expected Keys() to be empty")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 0 {
		t.Fatal("Expected SessionsFor() to be empty")
	}
}

func TestRemoveSession(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0, c1
	// s1: c0
	swl.Add([]cid.Cid{c0, c1}, s0)
	swl.Add([]cid.Cid{c0}, s1)

	// s1: c0
	swl.RemoveSession(s0)
	if len(swl.Keys()) != 1 {
		t.Fatal("Expected Keys() to have length 1")
	}
	if !swl.Keys()[0].Equals(c0) {
		t.Fatal("Expected Keys() to be [cid0]")
	}
	if len(swl.SessionsFor([]cid.Cid{c1})) != 0 {
		t.Fatal("Expected SessionsFor(c1) to be empty")
	}
	if len(swl.SessionsFor([]cid.Cid{c0})) != 1 {
		t.Fatal("Expected SessionsFor(c0) to have length 1")
	}
	if swl.SessionsFor([]cid.Cid{c0})[0] != s1 {
		t.Fatal("Expected SessionsFor(c0) to be [s1]")
	}
}

func TestRemoveSessionKeys(t *testing.T) {
	swl := NewSessionWantlist()

	// s0: c0, c1, c2
	// s1: c0
	swl.Add([]cid.Cid{c0, c1, c2}, s0)
	swl.Add([]cid.Cid{c0}, s1)

	// s0: c2
	// s1: c0
	swl.RemoveSessionKeys(s0, []cid.Cid{c0, c1})
	if !matchSet(swl.SessionHas(s0, []cid.Cid{c0, c1, c2}), []cid.Cid{c2}) {
		t.Fatal("Expected SessionHas(s0, [c0, c1, c2]) to be [c2]")
	}
	if !matchSet(swl.SessionHas(s1, []cid.Cid{c0, c1, c2}), []cid.Cid{c0}) {
		t.Fatal("Expected SessionHas(s1, [c0, c1, c2]) to be [c0]")
	}
}

func matchSet(ks1 *cid.Set, ks2 []cid.Cid) bool {
	if ks1.Len() != len(ks2) {
		return false
	}

	for _, k := range ks2 {
		if !ks1.Has(k) {
			return false
		}
	}
	return true
}
