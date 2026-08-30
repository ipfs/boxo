package merkledag_test

import (
	"bytes"
	"encoding/hex"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/ipfs/boxo/ipld/merkledag"
	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
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

func TestPBNodeFieldOrderUnknown(t *testing.T) {
	saveFieldOrder(t)
	merkledag.DefaultPBNodeFieldOrder = merkledag.PBNodeFieldOrder(7)

	node := merkledag.NodeWithData([]byte("x"))
	_, err := node.EncodeProtobuf(true)
	require.ErrorContains(t, err, "unknown PBNodeFieldOrder 7")
}

// encodeLinksFirst is a test-only dag-pb encoder built directly on
// protowire. Unlike the reference encoder it keeps links in the given order,
// so it can produce blocks with unsorted links, which is what a node decoded
// from another implementation may look like.
func encodeLinksFirst(data []byte, links []*format.Link) []byte {
	var enc []byte
	for _, l := range links {
		var pl []byte
		pl = protowire.AppendTag(pl, 1, protowire.BytesType)
		pl = protowire.AppendBytes(pl, l.Cid.Bytes())
		pl = protowire.AppendTag(pl, 2, protowire.BytesType)
		pl = protowire.AppendString(pl, l.Name)
		pl = protowire.AppendTag(pl, 3, protowire.VarintType)
		pl = protowire.AppendVarint(pl, l.Size)
		enc = protowire.AppendTag(enc, 2, protowire.BytesType)
		enc = protowire.AppendBytes(enc, pl)
	}
	if data != nil {
		enc = protowire.AppendTag(enc, 1, protowire.BytesType)
		enc = protowire.AppendBytes(enc, data)
	}
	return enc
}

func randomBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.IntN(256))
	}
	return b
}

func randomName(r *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-_."
	var sb strings.Builder
	sb.Grow(n)
	for range n {
		sb.WriteByte(alphabet[r.IntN(len(alphabet))])
	}
	return sb.String()
}

func randomCid(t *testing.T, r *rand.Rand) cid.Cid {
	payload := randomBytes(r, r.IntN(64)+1)
	switch r.IntN(3) {
	case 0:
		h, err := mh.Sum(payload, mh.SHA2_256, -1)
		require.NoError(t, err)
		return cid.NewCidV0(h)
	case 1:
		h, err := mh.Sum(payload, mh.SHA2_256, -1)
		require.NoError(t, err)
		return cid.NewCidV1(cid.Raw, h)
	default:
		h, err := mh.Sum(payload, mh.IDENTITY, -1)
		require.NoError(t, err)
		return cid.NewCidV1(cid.DagProtobuf, h)
	}
}

// pickLen returns a length that crosses the 1, 2 and 3 byte varint
// boundaries with useful frequency; the largest class is rare because it
// dominates block size.
func pickLen(r *rand.Rand) int {
	switch r.IntN(20) {
	case 0:
		return 0
	case 1, 2, 3:
		return 128 + r.IntN(2000)
	case 4:
		return 16384 + r.IntN(600)
	default:
		return 1 + r.IntN(127)
	}
}

type randomNode struct {
	data  []byte
	links []*format.Link
}

func newRandomNode(t *testing.T, r *rand.Rand) randomNode {
	var n randomNode
	switch r.IntN(4) {
	case 0:
		n.data = nil
	case 1:
		n.data = []byte{}
	default:
		n.data = randomBytes(r, pickLen(r))
	}
	var count int
	switch r.IntN(6) {
	case 0:
		count = 0
	case 1:
		count = 128 + r.IntN(200)
	default:
		count = 1 + r.IntN(16)
	}
	n.links = make([]*format.Link, 0, count)
	for range count {
		nameLen := pickLen(r)
		if count > 16 && nameLen > 2000 {
			nameLen = 2000
		}
		n.links = append(n.links, &format.Link{
			Name: randomName(r, nameLen),
			Size: r.Uint64N(1 << 63),
			Cid:  randomCid(t, r),
		})
	}
	return n
}

func requireSameLinks(t *testing.T, want, got []*format.Link) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].Name, got[i].Name, "link %d name", i)
		require.Equal(t, want[i].Size, got[i].Size, "link %d size", i)
		require.True(t, want[i].Cid.Equals(got[i].Cid), "link %d cid", i)
	}
}

// TestPBNodeFieldOrderRandomNodes decodes randomly shaped links-first blocks
// (including ones with unsorted links) and checks that re-encoding them under
// each order yields a block that decodes to the same node with links sorted
// by name, that the Data field leads the data-first form, and that the two
// forms differ only by where the Data field sits.
func TestPBNodeFieldOrderRandomNodes(t *testing.T) {
	saveFieldOrder(t)
	r := rand.New(rand.NewPCG(550, 2026))

	for i := range 300 {
		n := newRandomNode(t, r)
		r.Shuffle(len(n.links), func(a, b int) { n.links[a], n.links[b] = n.links[b], n.links[a] })
		sorted := slices.Clone(n.links)
		slices.SortStableFunc(sorted, func(a, b *format.Link) int { return strings.Compare(a.Name, b.Name) })

		raw := encodeLinksFirst(n.data, n.links)
		node, err := merkledag.DecodeProtobuf(raw)
		require.NoError(t, err, "node %d", i)

		var encoded [2][]byte
		for _, order := range []merkledag.PBNodeFieldOrder{merkledag.PBNodeLinksFirst, merkledag.PBNodeDataFirst} {
			merkledag.DefaultPBNodeFieldOrder = order
			enc, err := node.EncodeProtobuf(true)
			require.NoError(t, err, "node %d order %d", i, order)
			encoded[order] = enc

			back, err := merkledag.DecodeProtobuf(enc)
			require.NoError(t, err, "node %d order %d", i, order)
			if len(n.data) == 0 {
				require.Empty(t, back.Data(), "node %d order %d", i, order)
			} else {
				require.Equal(t, n.data, back.Data(), "node %d order %d", i, order)
			}
			requireSameLinks(t, sorted, back.Links())
		}

		linksFirst, dataFirst := encoded[merkledag.PBNodeLinksFirst], encoded[merkledag.PBNodeDataFirst]
		require.Len(t, dataFirst, len(linksFirst), "node %d", i)
		if n.data == nil {
			require.Equal(t, linksFirst, dataFirst, "node %d: no Data field, orders must agree", i)
			continue
		}
		num, typ, tagLen := protowire.ConsumeTag(dataFirst)
		require.Equal(t, protowire.Number(1), num, "node %d: first field", i)
		require.Equal(t, protowire.BytesType, typ, "node %d: first field type", i)
		field, fieldLen := protowire.ConsumeBytes(dataFirst[tagLen:])
		require.Equal(t, n.data, field, "node %d: leading Data field", i)
		span := tagLen + fieldLen
		require.Equal(t, linksFirst, slices.Concat(dataFirst[span:], dataFirst[:span]), "node %d: forms differ beyond Data placement", i)
	}
}
