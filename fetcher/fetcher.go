package fetcher

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-blockservice"
	"github.com/ipld/go-ipld-prime"
	dagpb "github.com/ipld/go-ipld-prime-proto"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

type FetcherConfig struct {
	blockService blockservice.BlockService
}

type Fetcher interface {
	// NodeMatching traverses a node graph starting with the provided node using the given selector and possibly crossing
	// block boundaries. Each matched node is passed as FetchResult to the callback.
	// The sequence of events is: NodeMatching begins, the callback is called zero or more times with a FetchResult, the
	// callback is called zero or one time with an error, then NodeMatching returns.
	NodeMatching(context.Context, ipld.Node, selector.Selector, FetchCallback)

	// BlockOfType fetches a node graph of the provided type corresponding to single block by link.
	BlockOfType(context.Context, ipld.Link, ipld.NodePrototype) (ipld.Node, error)

	// BlockMatchingOfType traverses a node graph starting with the given link using the given selector and possibly
	// crossing block boundaries. The nodes will be typed using the provided prototype. Each matched node is passed as
	// a FetchResult to the callback.
	// The sequence of events is: BlockMatchingOfType begins, the callback is called zero or more times with a
	// FetchResult, the callback is called zero or one time with an error, then BlockMatchingOfType returns.
	BlockMatchingOfType(context.Context, ipld.Link, selector.Selector, ipld.NodePrototype, FetchCallback)
}

type fetcherSession struct {
	blockGetter blockservice.BlockGetter
}

type FetchResult struct {
	Node          ipld.Node
	Path          ipld.Path
	LastBlockPath ipld.Path
	LastBlockLink ipld.Link
}

type FetchCallback func(result FetchResult, err error)

// NewFetcherConfig creates a FetchConfig from which session may be created and nodes retrieved.
func NewFetcherConfig(blockService blockservice.BlockService) FetcherConfig {
	return FetcherConfig{blockService: blockService}
}

// NewSession creates a session from which nodes may be retrieved.
// The session ends when the provided context is canceled.
func (fc FetcherConfig) NewSession(ctx context.Context) Fetcher {
	return &fetcherSession{
		blockGetter: blockservice.NewSession(ctx, fc.blockService),
	}
}

// BlockOfType fetches a node graph of the provided type corresponding to single block by link.
func (f *fetcherSession) BlockOfType(ctx context.Context, link ipld.Link, ptype ipld.NodePrototype) (ipld.Node, error) {
	nb := ptype.NewBuilder()

	err := link.Load(ctx, ipld.LinkContext{}, nb, f.loader(ctx))
	if err != nil {
		return nil, err
	}

	return nb.Build(), nil
}

func (f *fetcherSession) NodeMatching(ctx context.Context, node ipld.Node, match selector.Selector, cb FetchCallback) {
	f.fetch(ctx, node, match, cb)
}

func (f *fetcherSession) BlockMatchingOfType(ctx context.Context, root ipld.Link, match selector.Selector,
	ptype ipld.NodePrototype, cb FetchCallback) {

	// retrieve first node
	node, err := f.BlockOfType(ctx, root, ptype)
	if err != nil {
		cb(FetchResult{}, err)
		return
	}

	f.fetch(ctx, node, match, cb)
}

// Block fetches a schemaless node graph corresponding to single block by link.
func Block(ctx context.Context, f Fetcher, link ipld.Link) (ipld.Node, error) {
	prototype, err := prototypeFromLink(link)
	if err != nil {
		return nil, err
	}
	return f.BlockOfType(ctx, link, prototype)
}

// BlockMatching traverses a schemaless node graph starting with the given link using the given selector and possibly crossing
// block boundaries. Each matched node is sent to the FetchResult channel.
func BlockMatching(ctx context.Context, f Fetcher, root ipld.Link, match selector.Selector, cb FetchCallback) {
	prototype, err := prototypeFromLink(root)
	if err != nil {
		cb(FetchResult{}, err)
		return
	}
	f.BlockMatchingOfType(ctx, root, match, prototype, cb)
}

// BlockAll traverses all nodes in the graph linked by root. The nodes will be untyped and send over the results
// channel.
func BlockAll(ctx context.Context, f Fetcher, root ipld.Link, cb FetchCallback) {
	prototype, err := prototypeFromLink(root)
	if err != nil {
		cb(FetchResult{}, err)
		return
	}
	BlockAllOfType(ctx, f, root, prototype, cb)
}

// BlockAllOfType traverses all nodes in the graph linked by root. The nodes will typed according to ptype
// and send over the results channel.
func BlockAllOfType(ctx context.Context, f Fetcher, root ipld.Link, ptype ipld.NodePrototype, cb FetchCallback) {
	ssb := builder.NewSelectorSpecBuilder(ptype)
	allSelector, err := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreUnion(
		ssb.Matcher(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	)).Selector()
	if err != nil {
		cb(FetchResult{}, err)
		return
	}
	f.BlockMatchingOfType(ctx, root, allSelector, ptype, cb)
}

func (f *fetcherSession) fetch(ctx context.Context, node ipld.Node, match selector.Selector, cb FetchCallback) {
	err := traversal.Progress{
		Cfg: &traversal.Config{
			LinkLoader: f.loader(ctx),
			LinkTargetNodePrototypeChooser: dagpb.AddDagPBSupportToChooser(func(_ ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
				if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
					return tlnkNd.LinkTargetNodePrototype(), nil
				}
				return basicnode.Prototype.Any, nil
			}),
		},
	}.WalkMatching(node, match, func(prog traversal.Progress, n ipld.Node) error {
		cb(FetchResult{
			Node:          n,
			Path:          prog.Path,
			LastBlockPath: prog.LastBlock.Path,
			LastBlockLink: prog.LastBlock.Link,
		}, nil)
		return nil
	})
	if err != nil {
		cb(FetchResult{}, err)
	}
}

func (f *fetcherSession) loader(ctx context.Context) ipld.Loader {
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

func prototypeFromLink(lnk ipld.Link) (ipld.NodePrototype, error) {
	return dagpb.AddDagPBSupportToChooser(func(_ ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		return basicnode.Prototype__Any{}, nil
	})(lnk, ipld.LinkContext{})
}
