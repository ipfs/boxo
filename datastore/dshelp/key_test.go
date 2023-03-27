package dshelp

import (
	"testing"

	cid "github.com/ipfs/go-cid"
)

func TestKey(t *testing.T) {
	c, _ := cid.Decode("QmP63DkAFEnDYNjDYBpyNDfttu1fvUw99x1brscPzpqmmq")
	dsKey := MultihashToDsKey(c.Hash())
	mh, err := DsKeyToMultihash(dsKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(c.Hash()) != string(mh) {
		t.Fatal("should have parsed the same multihash")
	}

	c2, err := DsKeyToCidV1(dsKey, cid.Raw)
	if err != nil || c.Equals(c2) || c2.Type() != cid.Raw || c2.Version() != 1 {
		t.Fatal("should have been converted to CIDv1-raw")
	}
}
