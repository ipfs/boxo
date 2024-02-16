package blockservice

import (
	"context"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/provider"
	blocks "github.com/ipfs/go-block-format"
)

var _ blockstore.Blockstore = providingBlockstore{}

type providingBlockstore struct {
	blockstore.Blockstore
	provider provider.Provider
}

func (pbs providingBlockstore) Put(ctx context.Context, b blocks.Block) error {
	if err := pbs.Blockstore.Put(ctx, b); err != nil {
		return err
	}

	return pbs.provider.Provide(b.Cid())
}

func (pbs providingBlockstore) PutMany(ctx context.Context, b []blocks.Block) error {
	if err := pbs.Blockstore.PutMany(ctx, b); err != nil {
		return err // what are the semantics here, did some blocks were put ? assume PutMany is atomic
	}

	for _, b := range b {
		if err := pbs.provider.Provide(b.Cid()); err != nil {
			return err // this can only error if the whole provider is done for
		}
	}
	return nil
}
