package pb

import (
	"bytes"
	"testing"

	u "github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	"google.golang.org/protobuf/proto"
)

func TestCID(t *testing.T) {
	expected := [...]byte{
		10, 34, 18, 32, 195, 171,
		143, 241, 55, 32, 232, 173,
		144, 71, 221, 57, 70, 107,
		60, 137, 116, 229, 146, 194,
		250, 56, 61, 74, 57, 96,
		113, 76, 174, 240, 196, 242,
	}

	c := cid.NewCidV0(u.Hash([]byte("foobar")))
	msg := Message_BlockPresence{Cid: c.Bytes()}
	actual, err := proto.Marshal(&msg)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, expected[:]) {
		t.Fatal("failed to correctly encode custom CID type")
	}
}
