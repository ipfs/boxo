package merkledag

import (
	"encoding/binary"

	format "github.com/ipfs/go-ipld-format"
)

// PBNodeFieldOrder selects the order of the top-level PBNode fields in the
// serialized dag-pb form. Both orders decode to the same logical node, but
// produce different bytes and therefore different CIDs.
type PBNodeFieldOrder int

const (
	// PBNodeLinksFirst writes the repeated Links field (field number 2)
	// before the Data field (field number 1). This is the canonical DAG-PB
	// order, produced by all UnixFS profiles through unixfs-v1-2025.
	PBNodeLinksFirst PBNodeFieldOrder = iota

	// PBNodeDataFirst writes the Data field (field number 1) before the
	// repeated Links field (field number 2), so streaming readers can
	// process Data (e.g. HAMT parameters) before reading links. Proposed
	// by IPIP-550 (https://github.com/ipfs/specs/pull/550) for the
	// unixfs-v1-2026 profile.
	PBNodeDataFirst
)

// DefaultPBNodeFieldOrder is the field order used when encoding a ProtoNode.
// The default, PBNodeLinksFirst, keeps the bytes and CIDs boxo has always
// produced; PBNodeDataFirst is opt-in and changes the CID of every encoded
// node that has both fields.
//
// Thread safety: this variable is read on every encode and is not safe for
// concurrent modification. Set it once during program initialization, before
// starting any imports, e.g. via io.UnixFSProfile.ApplyGlobals.
var DefaultPBNodeFieldOrder = PBNodeLinksFirst

// appendEncodeDataFirst encodes a PBNode with the Data field before the
// repeated Links field. go-codec-dagpb only writes the canonical links-first
// order, hence this local encoder. Field presence mirrors the go-codec-dagpb
// path in marshalImmutable: Data is written when non-nil (even if empty),
// links with an undefined CID are dropped, and every written link carries
// Hash, Name, and Tsize in that order.
//
// TODO: this could be upstreamed to github.com/ipld/go-codec-dagpb as an
// encode option if IPIP-550 is ratified.
func appendEncodeDataFirst(enc []byte, data []byte, links []*format.Link) []byte {
	const (
		tagPBNodeData  = 0x0a // field 1, wire type 2 (bytes)
		tagPBNodeLinks = 0x12 // field 2, wire type 2 (embedded message)
		tagPBLinkHash  = 0x0a // field 1, wire type 2 (bytes)
		tagPBLinkName  = 0x12 // field 2, wire type 2 (string)
		tagPBLinkTsize = 0x18 // field 3, wire type 0 (varint)
	)

	if data != nil {
		enc = append(enc, tagPBNodeData)
		enc = binary.AppendUvarint(enc, uint64(len(data)))
		enc = append(enc, data...)
	}
	for _, link := range links {
		if !link.Cid.Defined() {
			continue
		}
		hash := link.Cid.Bytes()
		// overflow, >MaxInt64 is almost certainly an error
		tsize := uint64(max(int64(link.Size), 0))
		linkLen := 1 + uvarintLen(uint64(len(hash))) + len(hash) +
			1 + uvarintLen(uint64(len(link.Name))) + len(link.Name) +
			1 + uvarintLen(tsize)
		enc = append(enc, tagPBNodeLinks)
		enc = binary.AppendUvarint(enc, uint64(linkLen))
		enc = append(enc, tagPBLinkHash)
		enc = binary.AppendUvarint(enc, uint64(len(hash)))
		enc = append(enc, hash...)
		enc = append(enc, tagPBLinkName)
		enc = binary.AppendUvarint(enc, uint64(len(link.Name)))
		enc = append(enc, link.Name...)
		enc = append(enc, tagPBLinkTsize)
		enc = binary.AppendUvarint(enc, tsize)
	}
	return enc
}

// uvarintLen returns the number of bytes binary.AppendUvarint writes for v.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
