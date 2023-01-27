// helpers package have some utility functions that do recursive traversals on ipsl traversal objects.
// This aims to be easy to use. You are not meant to use this in all cases, calling .Traverse on Traversal
// objects gives more control. And can allow you to write faster code.
package helpers

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/ipsl"
)

var ErrDepthLimitReached = errors.New("safety depth limit reached")

// SyncDFS perform a synchronous recursive depth-first-search.
// It will return [ErrDepthLimitReached] when the safetyDepthLimit is reached.
// It will wrap errors returned by the call back, so use [errors.Is] to test them.
func SyncDFS(ctx context.Context, c cid.Cid, t ipsl.Traversal, getter blockservice.BlockGetter, safetyDepthLimit uint, callBack func(blocks.Block) error) error {
	if safetyDepthLimit == 0 {
		return ErrDepthLimitReached
	}
	safetyDepthLimit--

	block, err := getter.GetBlock(ctx, c)
	if err != nil {
		return fmt.Errorf("GetBlock: %w", err)
	}

	err = callBack(block)
	if err != nil {
		return fmt.Errorf("callBack: %w", err)
	}

	pairs, err := t.Traverse(block)
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
