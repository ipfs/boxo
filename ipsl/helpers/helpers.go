// helpers package have some utility functions that do recursive traversals on ipsl traversal objects.
// This aims to be easy to use. You are not meant to use this in all cases, calling .Traverse on Traversal
// objects gives more control. And can allow you to write faster code.
package helpers

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-libipfs/ipsl"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
)

var _ ByteBlockGetter = BlockGetterToByteBlockGetter{}

// BlockGetterToByteBlockGetter implements [ByteBlockGetter] given an [blockservice.BlockGetter].
// This prevent the optimizations [ByteBlockGetter] is trying to do, so you should ideally not use this.
type BlockGetterToByteBlockGetter struct {
	BlockGetter blockservice.BlockGetter
}

func (bgtbbg BlockGetterToByteBlockGetter) GetBlock(ctx context.Context, c cid.Cid) ([]byte, error) {
	b, err := bgtbbg.BlockGetter.GetBlock(ctx, c)
	if err != nil {
		return nil, err
	}

	return b.RawData(), nil
}

// ByteBlockGetter is like [blockservice.BlockGetter] but it does not wrap in an extra block object.
// This is to avoid making too much garbage, passing blocks interface objects arround is moves stuff on the heap.
type ByteBlockGetter interface {
	GetBlock(ctx context.Context, c cid.Cid) ([]byte, error)

	// TODO: Add GetBlocks when that is needed.
}

var ErrDepthLimitReached = errors.New("safety depth limit reached")

// SyncDFS perform a synchronous recursive depth-first-search.
// It will return [ErrDepthLimitReached] when the safetyDepthLimit is reached.
// It will wrap errors returned by the call back, so use [errors.Is] to test them.
func SyncDFS(ctx context.Context, c cid.Cid, t ipsl.Traversal, getter ByteBlockGetter, safetyDepthLimit uint, callBack func(cid.Cid, []byte) error) error {
	if safetyDepthLimit == 0 {
		return ErrDepthLimitReached
	}
	safetyDepthLimit--

	block, err := getter.GetBlock(ctx, c)
	if err != nil {
		return fmt.Errorf("GetBlock: %w", err)
	}

	err = callBack(c, block)
	if err != nil {
		return fmt.Errorf("callBack: %w", err)
	}

	pairs, err := t.Traverse(c, block)
	if err != nil {
		return fmt.Errorf("Traversal.Traverse: %w", err)
	}

	for _, p := range pairs {
		err := SyncDFS(ctx, p.Cid, p.Traversal, getter, safetyDepthLimit, callBack)
		if err != nil {
			return err
		}
	}

	return nil
}
