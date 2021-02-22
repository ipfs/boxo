package fetcher

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-bitswap"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// TODO: need to support sessions

type Fetcher struct {
	exchange *bitswap.Bitswap
}

type FetchResult struct {
	Node          ipld.Node
	Path          ipld.Path
	LastBlockPath ipld.Path
	LastBlockLink ipld.Link
}

func NewFetcher(exchange *bitswap.Bitswap) Fetcher {
	return Fetcher{exchange: exchange}
}

func (f Fetcher) FetchNode(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()

	err := cidlink.Link{Cid: c}.Load(ctx, ipld.LinkContext{}, nb, f.loader(ctx))
	if err != nil {
		return nil, err
	}

	return nb.Build(), nil
}

func (f Fetcher) FetchMatching(ctx context.Context, root cid.Cid, match selector.Selector) (chan FetchResult, chan error) {
	results := make(chan FetchResult)
	errors := make(chan error)

	go func() {
		defer close(results)

		// retrieve first node
		node, err := f.FetchNode(ctx, root)
		if err != nil {
			errors <- err
			return
		}

		err = traversal.Progress{
			Cfg: &traversal.Config{
				LinkLoader: f.loader(ctx),
				LinkTargetNodePrototypeChooser: func(_ ipld.Link, _ ipld.LinkContext) (ipld.NodePrototype, error) {
					return basicnode.Prototype__Any{}, nil
				},
			},
		}.WalkMatching(node, match, func(prog traversal.Progress, n ipld.Node) error {
			results <- FetchResult{
				Node:          n,
				Path:          prog.Path,
				LastBlockPath: prog.LastBlock.Path,
				LastBlockLink: prog.LastBlock.Link,
			}
			return nil
		})
		if err != nil {
			errors <- err
			return
		}
	}()

	return results, errors
}

func (f Fetcher) FetchAll(ctx context.Context, root cid.Cid) (chan FetchResult, chan error) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype__Any{})
	allSelector, err := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreUnion(
		ssb.Matcher(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	)).Selector()
	if err != nil {
		errors := make(chan error, 1)
		errors <- err
		return nil, errors
	}
	return f.FetchMatching(ctx, root, allSelector)
}

func (f Fetcher) loader(ctx context.Context) ipld.Loader {
	return func(lnk ipld.Link, _ ipld.LinkContext) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}

		blk, err := f.exchange.GetBlock(ctx, cidLink.Cid)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(blk.RawData()), nil
	}
}
