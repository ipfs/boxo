package carbon

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/util"
	carbs "github.com/willscott/carbs"
)

// carbonFD is a carbon implementation based on having two file handles opened, one appending to the file, and the other
// seeking to read items as needed. This implementation is preferable for a write-heavy workload.
type carbonFD struct {
	path        string
	writeHandle *poswriter
	carbs.Carbs
	idx *insertionIndex
}

var _ (Carbon) = (*carbonFD)(nil)

func (c *carbonFD) DeleteBlock(cid.Cid) error {
	return errUnsupported
}

// Put puts a given block to the underlying datastore
func (c *carbonFD) Put(b blocks.Block) error {
	return c.PutMany([]blocks.Block{b})
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (c *carbonFD) PutMany(b []blocks.Block) error {
	for _, bl := range b {
		n := c.writeHandle.at
		if err := util.LdWrite(c.writeHandle, bl.Cid().Bytes(), bl.RawData()); err != nil {
			return err
		}
		c.idx.items.InsertNoReplace(mkRecordFromCid(bl.Cid(), n))
	}
	return nil
}

// Finish serializes the carbon index so that it can be later used as a carbs read-only blockstore
func (c *carbonFD) Finish() error {
	fi, err := c.idx.Flatten()
	if err != nil {
		return err
	}
	return carbs.Save(fi, c.path)
}

// Checkpoint serializes the carbon index so that the partially written blockstore can be resumed.
func (c *carbonFD) Checkpoint() error {
	return carbs.Save(c.idx, c.path)
}
