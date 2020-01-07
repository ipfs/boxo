package dshelp

import (
	"testing"

	cid "github.com/ipfs/go-cid"
)

func TestKey(t *testing.T) {
	c, _ := cid.Decode("QmP63DkAFEnDYNjDYBpyNDfttu1fvUw99x1brscPzpqmmq")
	dsKey := CidToDsKey(c)
	c2, err := DsKeyToCid(dsKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(c.Hash()) != string(c2.Hash()) {
		t.Fatal("should have parsed the same key")
	}
	if c.Equals(c2) || c2.Type() != cid.Raw || c2.Version() != 1 {
		t.Fatal("should have been converted to CIDv1-raw")
	}
}
