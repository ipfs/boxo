package fetcher

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-blockservice"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// FetcherConfig defines a configuration object from which Fetcher instances are constructed
type FetcherConfig struct {
	blockService     blockservice.BlockService
	NodeReifier      ipld.NodeReifier
	PrototypeChooser traversal.LinkTargetNodePrototypeChooser
}

// Fetcher is an interface for reading from a dag. Reads may be local or remote, and may employ data exchange
// protocols like graphsync and bitswap
type Fetcher interface {
	// NodeMatching traverses a node graph starting with the provided node using the given selector and possibly crossing
	// block boundaries. Each matched node is passed as FetchResult to the callback. Errors returned from callback will
	// halt the traversal. The sequence of events is: NodeMatching begins, the callback is called zero or more times
	// with a FetchResult, then NodeMatching returns.
	NodeMatching(context.Context, ipld.Node, ipld.Node, FetchCallback) error

	// BlockOfType fetches a node graph of the provided type corresponding to single block by link.
	BlockOfType(context.Context, ipld.Link, ipld.NodePrototype) (ipld.Node, error)

	// BlockMatchingOfType traverses a node graph starting with the given link using the given selector and possibly
	// crossing block boundaries. The nodes will be typed using the provided prototype. Each matched node is passed as
	// a FetchResult to the callback. Errors returned from callback will halt the traversal.
	// The sequence of events is: BlockMatchingOfType begins, the callback is called zero or more times with a
	// FetchResult, then BlockMatchingOfType returns.
	BlockMatchingOfType(context.Context, ipld.Link, ipld.Node, ipld.NodePrototype, FetchCallback) error

	// Uses the given link to pick a prototype to build the linked node.
	PrototypeFromLink(link ipld.Link) (ipld.NodePrototype, error)
}

type fetcherSession struct {
	linkSystem   ipld.LinkSystem
	protoChooser traversal.LinkTargetNodePrototypeChooser
}

// FetchResult is a single node read as part of a dag operation called on a fetcher
type FetchResult struct {
	Node          ipld.Node
	Path          ipld.Path
	LastBlockPath ipld.Path
	LastBlockLink ipld.Link
}

// FetchCallback is called for each node traversed during a fetch
type FetchCallback func(result FetchResult) error

// NewFetcherConfig creates a FetchConfig from which session may be created and nodes retrieved.
func NewFetcherConfig(blockService blockservice.BlockService) FetcherConfig {
	return FetcherConfig{
		blockService:     blockService,
		PrototypeChooser: DefaultPrototypeChooser,
	}
}

// NewSession creates a session from which nodes may be retrieved.
// The session ends when the provided context is canceled.
func (fc FetcherConfig) NewSession(ctx context.Context) Fetcher {
	ls := cidlink.DefaultLinkSystem()
	// while we may be loading blocks remotely, they are already hash verified by the time they load
	// into ipld-prime
	ls.TrustedStorage = true
	ls.StorageReadOpener = blockOpener(ctx, blockservice.NewSession(ctx, fc.blockService))
	ls.NodeReifier = fc.NodeReifier

	protoChooser := fc.PrototypeChooser
	return &fetcherSession{linkSystem: ls, protoChooser: protoChooser}
}

// BlockOfType fetches a node graph of the provided type corresponding to single block by link.
func (f *fetcherSession) BlockOfType(ctx context.Context, link ipld.Link, ptype ipld.NodePrototype) (ipld.Node, error) {
	return f.linkSystem.Load(ipld.LinkContext{}, link, ptype)
}

func (f *fetcherSession) nodeMatching(ctx context.Context, initialProgress traversal.Progress, node ipld.Node, match ipld.Node, cb FetchCallback) error {
	matchSelector, err := selector.ParseSelector(match)
	if err != nil {
		return err
	}
	return initialProgress.WalkMatching(node, matchSelector, func(prog traversal.Progress, n ipld.Node) error {
		return cb(FetchResult{
			Node:          n,
			Path:          prog.Path,
			LastBlockPath: prog.LastBlock.Path,
			LastBlockLink: prog.LastBlock.Link,
		})
	})
}

func (f *fetcherSession) blankProgress(ctx context.Context) traversal.Progress {
	return traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem:                     f.linkSystem,
			LinkTargetNodePrototypeChooser: f.protoChooser,
		},
	}
}

func (f *fetcherSession) NodeMatching(ctx context.Context, node ipld.Node, match ipld.Node, cb FetchCallback) error {
	return f.nodeMatching(ctx, f.blankProgress(ctx), node, match, cb)
}

func (f *fetcherSession) BlockMatchingOfType(ctx context.Context, root ipld.Link, match ipld.Node,
	ptype ipld.NodePrototype, cb FetchCallback) error {

	// retrieve first node
	node, err := f.BlockOfType(ctx, root, ptype)
	if err != nil {
		return err
	}

	progress := f.blankProgress(ctx)
	progress.LastBlock.Link = root
	return f.nodeMatching(ctx, progress, node, match, cb)
}

func (f *fetcherSession) PrototypeFromLink(lnk ipld.Link) (ipld.NodePrototype, error) {
	return f.protoChooser(lnk, ipld.LinkContext{})
}

// Block fetches a schemaless node graph corresponding to single block by link.
func Block(ctx context.Context, f Fetcher, link ipld.Link) (ipld.Node, error) {
	prototype, err := f.PrototypeFromLink(link)
	if err != nil {
		return nil, err
	}
	return f.BlockOfType(ctx, link, prototype)
}

// BlockMatching traverses a schemaless node graph starting with the given link using the given selector and possibly crossing
// block boundaries. Each matched node is sent to the FetchResult channel.
func BlockMatching(ctx context.Context, f Fetcher, root ipld.Link, match ipld.Node, cb FetchCallback) error {
	prototype, err := f.PrototypeFromLink(root)
	if err != nil {
		return err
	}
	return f.BlockMatchingOfType(ctx, root, match, prototype, cb)
}

// BlockAll traverses all nodes in the graph linked by root. The nodes will be untyped and send over the results
// channel.
func BlockAll(ctx context.Context, f Fetcher, root ipld.Link, cb FetchCallback) error {
	prototype, err := f.PrototypeFromLink(root)
	if err != nil {
		return err
	}
	return BlockAllOfType(ctx, f, root, prototype, cb)
}

// BlockAllOfType traverses all nodes in the graph linked by root. The nodes will typed according to ptype
// and send over the results channel.
func BlockAllOfType(ctx context.Context, f Fetcher, root ipld.Link, ptype ipld.NodePrototype, cb FetchCallback) error {
	ssb := builder.NewSelectorSpecBuilder(ptype)
	allSelector := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreUnion(
		ssb.Matcher(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	)).Node()
	return f.BlockMatchingOfType(ctx, root, allSelector, ptype, cb)
}

func blockOpener(ctx context.Context, bs *blockservice.Session) ipld.BlockReadOpener {
	return func(_ ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}

		blk, err := bs.GetBlock(ctx, cidLink.Cid)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(blk.RawData()), nil
	}
}

// Chooser that supports DagPB nodes and choosing the prototype from the link.
var DefaultPrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
	if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
		return tlnkNd.LinkTargetNodePrototype(), nil
	}
	return basicnode.Prototype.Any, nil
})
