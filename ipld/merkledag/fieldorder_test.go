package merkledag_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/ipfs/boxo/ipld/merkledag"
	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// Test fixtures from IPIP-550 (https://github.com/ipfs/specs/pull/550):
// the same UnixFS directory and HAMT shard, each serialized with the
// canonical links-first order and with the opt-in data-first order.
const (
	dirLinksFirstHex = "12330a24015512205891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03120968656c6c6f2e74787418060a020801"
	dirDataFirstHex  = "0a02080112330a24015512205891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03120968656c6c6f2e7478741806"

	hamtLinksFirstHex = "12350a24015512205891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03120b444668656c6c6f2e74787418060a250805121c800000000000000000000000000000000000000000000000000000002822308002"
	hamtDataFirstHex  = "0a250805121c80000000000000000000000000000000000000000000000000000000282230800212350a24015512205891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03120b444668656c6c6f2e7478741806"

	dirLinksFirstCid  = "bafybeigdcg7pksx2zk5336vrfsktjodlr4rbfz37qr3koc5xboxe5ekv24"
	dirDataFirstCid   = "bafybeigqvyloizmfcdy6scaxnyltftzptaruqa3hnnplfzsbf4sqteiwlm"
	hamtLinksFirstCid = "bafybeicjwkfslu7gwyywffvqgse5kiibojtktxcdqhgv7ldj5fjdacuceq"
	hamtDataFirstCid  = "bafybeicwgy2rlqmqqu3yy2tqvm2wbgdvy3snu4sbbv4wqpvpnoplpzxz74"
)

func saveFieldOrder(t *testing.T) {
	old := merkledag.DefaultPBNodeFieldOrder
	t.Cleanup(func() { merkledag.DefaultPBNodeFieldOrder = old })
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// reencode decodes a raw dag-pb block and re-encodes it under the given
// field order, returning the bytes and the CIDv1 of the result.
func reencode(t *testing.T, raw []byte, order merkledag.PBNodeFieldOrder) ([]byte, cid.Cid) {
	t.Helper()
	node, err := merkledag.DecodeProtobuf(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := node.SetCidBuilder(cid.Prefix{
		Version:  1,
		Codec:    cid.DagProtobuf,
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}); err != nil {
		t.Fatal(err)
	}
	merkledag.DefaultPBNodeFieldOrder = order
	enc, err := node.EncodeProtobuf(true)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return enc, node.Cid()
}

func TestPBNodeFieldOrder(t *testing.T) {
	cases := []struct {
		name                        string
		linksFirstHex, dataFirstHex string
		linksFirstCid, dataFirstCid string
	}{
		{"directory", dirLinksFirstHex, dirDataFirstHex, dirLinksFirstCid, dirDataFirstCid},
		{"hamt shard", hamtLinksFirstHex, hamtDataFirstHex, hamtLinksFirstCid, hamtDataFirstCid},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			saveFieldOrder(t)
			linksFirst := mustDecodeHex(t, tc.linksFirstHex)
			dataFirst := mustDecodeHex(t, tc.dataFirstHex)

			// Decoding must accept both orders and yield the same logical node.
			a, err := merkledag.DecodeProtobuf(linksFirst)
			if err != nil {
				t.Fatalf("decode links-first: %v", err)
			}
			b, err := merkledag.DecodeProtobuf(dataFirst)
			if err != nil {
				t.Fatalf("decode data-first: %v", err)
			}
			if !bytes.Equal(a.Data(), b.Data()) {
				t.Error("Data differs between orders")
			}
			if len(a.Links()) != len(b.Links()) {
				t.Fatal("link count differs between orders")
			}
			for i, la := range a.Links() {
				lb := b.Links()[i]
				if la.Name != lb.Name || la.Size != lb.Size || !la.Cid.Equals(lb.Cid) {
					t.Errorf("link %d differs between orders", i)
				}
			}

			// Re-encoding either input under each order must reproduce the
			// fixture bytes and CIDs exactly.
			for _, input := range [][]byte{linksFirst, dataFirst} {
				enc, c := reencode(t, input, merkledag.PBNodeLinksFirst)
				if !bytes.Equal(enc, linksFirst) {
					t.Errorf("links-first re-encode: got %x, want %s", enc, tc.linksFirstHex)
				}
				if c.String() != tc.linksFirstCid {
					t.Errorf("links-first CID: got %s, want %s", c, tc.linksFirstCid)
				}

				enc, c = reencode(t, input, merkledag.PBNodeDataFirst)
				if !bytes.Equal(enc, dataFirst) {
					t.Errorf("data-first re-encode: got %x, want %s", enc, tc.dataFirstHex)
				}
				if c.String() != tc.dataFirstCid {
					t.Errorf("data-first CID: got %s, want %s", c, tc.dataFirstCid)
				}
			}
		})
	}
}
