// Package blocks contains the lowest level of IPLD data structures.
// A block is raw data accompanied by a CID. The CID contains the multihash
// corresponding to the block.
package blocks

import (
	"errors"
	"fmt"

	cid "github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	mh "github.com/multiformats/go-multihash"
)

// ErrWrongHash is returned when the Cid of a block is not the expected
// according to the contents. It is currently used only when debugging.
var ErrWrongHash = errors.New("data did not match given hash")

// A Block is a singular block of data in ipfs. This is some bytes addressed by a hash.
type Block struct {
	CID  cid.Cid
	Data []byte
}

// NewBlock creates a Block object from opaque data. It will hash the data.
func NewBlock(data []byte) Block {
	// TODO: fix assumptions
	return Block{Data: data, CID: cid.NewCidV0(u.Hash(data))}
}

// NewBlockWithCid creates a new block when the hash of the data
// is already known, this is used to save time in situations where
// we are able to be confident that the data is correct.
func NewBlockWithCid(data []byte, c cid.Cid) (Block, error) {
	if u.Debug {
		chkc, err := c.Prefix().Sum(data)
		if err != nil {
			return Block{}, err
		}

		if !chkc.Equals(c) {
			return Block{}, ErrWrongHash
		}
	}
	return Block{Data: data, CID: c}, nil
}

// Multihash returns the hash contained in the block CID.
func (b Block) Multihash() mh.Multihash {
	return b.CID.Hash()
}

// RawData returns the block raw contents as a byte slice.
func (b Block) RawData() []byte {
	return b.Data
}

// Cid returns the content identifier of the block.
func (b Block) Cid() cid.Cid {
	return b.CID
}

// String provides a human-readable representation of the block CID.
func (b Block) String() string {
	return fmt.Sprintf("[Block %s]", b.Cid())
}

// Loggable returns a go-log loggable item.
func (b Block) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"block": b.Cid().String(),
	}
}
