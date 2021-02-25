package fetcher

import (
	"bytes"
	"context"
	"fmt"
	"io"

	dagpb "github.com/ipld/go-ipld-prime-proto"

	"github.com/ipfs/go-blockservice"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

type FetcherConfig struct {
	blockService blockservice.BlockService
}

type Fetcher struct {
	blockGetter blockservice.BlockGetter
}

type FetchResult struct {
	Node          ipld.Node
	Path          ipld.Path
	LastBlockPath ipld.Path
	LastBlockLink ipld.Link
}

// NewFetcherConfig creates a FetchConfig from which session may be created and nodes retrieved.
func NewFetcherConfig(blockService blockservice.BlockService) FetcherConfig {
	return FetcherConfig{blockService: blockService}
}

// NewSession creates a session from which nodes may be retrieved.
// The session ends when the provided context is canceled.
func (fc FetcherConfig) NewSession(ctx context.Context) *Fetcher {
	return &Fetcher{
		blockGetter: blockservice.NewSession(ctx, fc.blockService),
	}
}

// Block fetches a schemaless node graph corresponding to single block by link.
func (f *Fetcher) Block(ctx context.Context, link ipld.Link) (ipld.Node, error) {
	return f.BlockOfType(ctx, link, basicnode.Prototype.Any)
}

// BlockOfType fetches a node graph of the provided type corresponding to single block by link.
func (f *Fetcher) BlockOfType(ctx context.Context, link ipld.Link, ptype ipld.NodePrototype) (ipld.Node, error) {
	nb := ptype.NewBuilder()

	err := link.Load(ctx, ipld.LinkContext{}, nb, f.loader(ctx))
	if err != nil {
		return nil, err
	}

	return nb.Build(), nil
}

// NodeMatching traverses a node graph starting with the provided node using the given selector and possibly crossing
// block boundaries. Each matched node is sent to the FetchResult channel.
func (f *Fetcher) NodeMatching(ctx context.Context, node ipld.Node, match selector.Selector) (chan FetchResult, chan error) {
	results := make(chan FetchResult)
	errors := make(chan error)

	go func() {
		defer close(results)

		err := f.fetch(ctx, node, match, results)
		if err != nil {
			errors <- err
			return
		}
	}()

	return results, errors
}

// BlockMatching traverses a schemaless node graph starting with the given link using the given selector and possibly crossing
// block boundaries. Each matched node is sent to the FetchResult channel.
func (f *Fetcher) BlockMatching(ctx context.Context, root ipld.Link, match selector.Selector) (chan FetchResult, chan error) {
	return f.BlockMatchingOfType(ctx, root, match, basicnode.Prototype.Any)
}

// BlockMatchingOfType traverses a node graph starting with the given link using the given selector and possibly
// crossing block boundaries. The nodes will be typed using the provided prototype. Each matched node is sent to
// the FetchResult channel.
func (f *Fetcher) BlockMatchingOfType(ctx context.Context, root ipld.Link, match selector.Selector, ptype ipld.NodePrototype) (chan FetchResult, chan error) {
	results := make(chan FetchResult)
	errors := make(chan error)

	go func() {
		defer close(results)

		// retrieve first node
		node, err := f.BlockOfType(ctx, root, ptype)
		if err != nil {
			errors <- err
			return
		}

		err = f.fetch(ctx, node, match, results)
		if err != nil {
			errors <- err
			return
		}
	}()

	return results, errors
}

// BlockAll traverses all nodes in the graph linked by root. The nodes will be untyped and send over the results
// channel.
func (f *Fetcher) BlockAll(ctx context.Context, root ipld.Link) (chan FetchResult, chan error) {
	return f.BlockAllOfType(ctx, root, basicnode.Prototype.Any)
}

// BlockAllOfType traverses all nodes in the graph linked by root. The nodes will typed according to ptype
// and send over the results channel.
func (f *Fetcher) BlockAllOfType(ctx context.Context, root ipld.Link, ptype ipld.NodePrototype) (chan FetchResult, chan error) {
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
	return f.BlockMatchingOfType(ctx, root, allSelector, ptype)
}

func (f *Fetcher) fetch(ctx context.Context, node ipld.Node, match selector.Selector, results chan FetchResult) error {
	return traversal.Progress{
		Cfg: &traversal.Config{
			LinkLoader: f.loader(ctx),
			LinkTargetNodePrototypeChooser: dagpb.AddDagPBSupportToChooser(func(_ ipld.Link, _ ipld.LinkContext) (ipld.NodePrototype, error) {
				return basicnode.Prototype__Any{}, nil
			}),
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
}

func (f *Fetcher) loader(ctx context.Context) ipld.Loader {
	return func(lnk ipld.Link, _ ipld.LinkContext) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}

		blk, err := f.blockGetter.GetBlock(ctx, cidLink.Cid)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(blk.RawData()), nil
	}
}
