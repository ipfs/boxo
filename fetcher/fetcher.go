package fetcher

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipld/go-ipld-prime/traversal/selector/builder"

	"github.com/ipld/go-ipld-prime/traversal"

	"github.com/ipld/go-ipld-prime/traversal/selector"

	"github.com/ipfs/go-bitswap"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

// TODO: need to support sessions

type Fetcher struct {
	Exchange *bitswap.Bitswap
}

func NewFetcher(exchange *bitswap.Bitswap) *Fetcher {
	return &Fetcher{Exchange: exchange}
}

func (f *Fetcher) FetchNode(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()

	err := cidlink.Link{Cid: c}.Load(ctx, ipld.LinkContext{}, nb, f.loader(ctx))
	if err != nil {
		return nil, err
	}

	return nb.Build(), nil
}

type NodeResult struct {
	Node ipld.Node
	Err  error
}

func (f *Fetcher) FetchMatching(ctx context.Context, root cid.Cid, match selector.Selector) (chan NodeResult, error) {
	node, err := f.FetchNode(ctx, root)
	if err != nil {
		return nil, err
	}

	results := make(chan NodeResult)

	go func() {
		defer close(results)
		err = traversal.Progress{
			Cfg: &traversal.Config{
				LinkLoader: f.loader(ctx),
				LinkTargetNodePrototypeChooser: func(_ ipld.Link, _ ipld.LinkContext) (ipld.NodePrototype, error) {
					return basicnode.Prototype__Any{}, nil
				},
			},
		}.WalkMatching(node, match, func(prog traversal.Progress, n ipld.Node) error {
			results <- NodeResult{Node: n}
			return nil
		})
		if err != nil {
			results <- NodeResult{Err: err}
		}
	}()

	return results, nil
}

func (f *Fetcher) FetchAll(ctx context.Context, root cid.Cid) (chan NodeResult, error) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype__Any{})
	allSelector, err := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreUnion(
		ssb.Matcher(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	)).Selector()
	if err != nil {
		return nil, err
	}
	return f.FetchMatching(ctx, root, allSelector)
}

// TODO: take optional Cid channel for links traversed
func (f *Fetcher) loader(ctx context.Context) ipld.Loader {
	return func(lnk ipld.Link, _ ipld.LinkContext) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}

		blk, err := f.Exchange.GetBlock(ctx, cidLink.Cid)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(blk.RawData()), nil
	}
}
