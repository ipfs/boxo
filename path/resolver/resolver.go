// Package resolver implements utilities for resolving paths within ipfs.
package resolver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/boxo/fetcher"
	fetcherhelpers "github.com/ipfs/boxo/fetcher/helpers"
	path "github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/path/internal"
	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

var log = logging.Logger("pathresolv")

// ErrNoComponents is used when Paths after a protocol
// do not contain at least one component
var ErrNoComponents = errors.New(
	"path must contain at least one component")

// ErrNoLink is returned when a link is not found in a path
type ErrNoLink struct {
	Name string
	Node cid.Cid
}

// Error implements the Error interface for ErrNoLink with a useful
// human readable message.
func (e ErrNoLink) Error() string {
	return fmt.Sprintf("no link named %q under %s", e.Name, e.Node.String())
}

// Resolver provides path resolution to IPFS.
type Resolver interface {
	// ResolveToLastNode walks the given path and returns the cid of the
	// last block referenced by the path, and the path segments to
	// traverse from the final block boundary to the final node within the
	// block.
	ResolveToLastNode(ctx context.Context, fpath path.Path) (cid.Cid, []string, error)
	// ResolvePath fetches the node for given path. It returns the last
	// item returned by ResolvePathComponents and the last link traversed
	// which can be used to recover the block.
	ResolvePath(ctx context.Context, fpath path.Path) (ipld.Node, ipld.Link, error)
	// ResolvePathComponents fetches the nodes for each segment of the given path.
	// It uses the first path component as a hash (key) of the first node, then
	// resolves all other components walking the links via a selector traversal
	ResolvePathComponents(ctx context.Context, fpath path.Path) ([]ipld.Node, error)
}

// basicResolver implements the Resolver interface.
// It references a FetcherFactory, which is uses to resolve nodes.
// TODO: now that this is more modular, try to unify this code with the
//
//	the resolvers in namesys.
type basicResolver struct {
	FetcherFactory fetcher.Factory
}

// NewBasicResolver constructs a new basic resolver.
func NewBasicResolver(fetcherFactory fetcher.Factory) Resolver {
	return &basicResolver{
		FetcherFactory: fetcherFactory,
	}
}

// ResolveToLastNode walks the given path and returns the cid of the last
// block referenced by the path, and the path segments to traverse from the
// final block boundary to the final node within the block.
func (r *basicResolver) ResolveToLastNode(ctx context.Context, fpath path.Path) (cid.Cid, []string, error) {
	ctx, span := internal.StartSpan(ctx, "basicResolver.ResolveToLastNode", trace.WithAttributes(attribute.Stringer("Path", fpath)))
	defer span.End()

	c, p, err := path.SplitAbsPath(fpath)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	if len(p) == 0 {
		return c, nil, nil
	}

	// create a selector to traverse and match all path segments
	pathSelector := pathAllSelector(p[:len(p)-1])

	// create a new cancellable session
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// resolve node before last path segment
	nodes, lastCid, depth, err := r.resolveNodes(ctx, c, pathSelector)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	if len(nodes) < 1 {
		return cid.Cid{}, nil, fmt.Errorf("path %v did not resolve to a node", fpath)
	} else if len(nodes) < len(p) {
		return cid.Undef, nil, ErrNoLink{Name: p[len(nodes)-1], Node: lastCid}
	}

	parent := nodes[len(nodes)-1]
	lastSegment := p[len(p)-1]

	// find final path segment within node
	nd, err := parent.LookupBySegment(ipld.ParsePathSegment(lastSegment))
	switch err.(type) {
	case nil:
	case schema.ErrNoSuchField:
		return cid.Undef, nil, ErrNoLink{Name: lastSegment, Node: lastCid}
	default:
		return cid.Cid{}, nil, err
	}

	// if last node is not a link, just return it's cid, add path to remainder and return
	if nd.Kind() != ipld.Kind_Link {
		// return the cid and the remainder of the path
		return lastCid, p[len(p)-depth-1:], nil
	}

	lnk, err := nd.AsLink()
	if err != nil {
		return cid.Cid{}, nil, err
	}

	clnk, ok := lnk.(cidlink.Link)
	if !ok {
		return cid.Cid{}, nil, fmt.Errorf("path %v resolves to a link that is not a cid link: %v", fpath, lnk)
	}

	return clnk.Cid, []string{}, nil
}

// ResolvePath fetches the node for given path. It returns the last item
// returned by ResolvePathComponents and the last link traversed which can be used to recover the block.
//
// Note: if/when the context is cancelled or expires then if a multi-block ADL node is returned then it may not be
// possible to load certain values.
func (r *basicResolver) ResolvePath(ctx context.Context, fpath path.Path) (ipld.Node, ipld.Link, error) {
	ctx, span := internal.StartSpan(ctx, "basicResolver.ResolvePath", trace.WithAttributes(attribute.Stringer("Path", fpath)))
	defer span.End()

	// validate path
	if err := fpath.IsValid(); err != nil {
		return nil, nil, err
	}

	c, p, err := path.SplitAbsPath(fpath)
	if err != nil {
		return nil, nil, err
	}

	// create a selector to traverse all path segments but only match the last
	pathSelector := pathLeafSelector(p)

	nodes, c, _, err := r.resolveNodes(ctx, c, pathSelector)
	if err != nil {
		return nil, nil, err
	}
	if len(nodes) < 1 {
		return nil, nil, fmt.Errorf("path %v did not resolve to a node", fpath)
	}
	return nodes[len(nodes)-1], cidlink.Link{Cid: c}, nil
}

// ResolveSingle simply resolves one hop of a path through a graph with no
// extra context (does not opaquely resolve through sharded nodes)
// Deprecated: fetch node as ipld-prime or convert it and then use a selector to traverse through it.
func ResolveSingle(ctx context.Context, ds format.NodeGetter, nd format.Node, names []string) (*format.Link, []string, error) {
	_, span := internal.StartSpan(ctx, "ResolveSingle", trace.WithAttributes(attribute.Stringer("CID", nd.Cid())))
	defer span.End()
	return nd.ResolveLink(names)
}

// ResolvePathComponents fetches the nodes for each segment of the given path.
// It uses the first path component as a hash (key) of the first node, then
// resolves all other components walking the links via a selector traversal
//
// Note: if/when the context is cancelled or expires then if a multi-block ADL node is returned then it may not be
// possible to load certain values.
func (r *basicResolver) ResolvePathComponents(ctx context.Context, fpath path.Path) (nodes []ipld.Node, err error) {
	ctx, span := internal.StartSpan(ctx, "basicResolver.ResolvePathComponents", trace.WithAttributes(attribute.Stringer("Path", fpath)))
	defer span.End()

	defer log.Debugw("resolvePathComponents", "fpath", fpath, "error", err)

	// validate path
	if err := fpath.IsValid(); err != nil {
		return nil, err
	}

	c, p, err := path.SplitAbsPath(fpath)
	if err != nil {
		return nil, err
	}

	// create a selector to traverse and match all path segments
	pathSelector := pathAllSelector(p)

	nodes, _, _, err = r.resolveNodes(ctx, c, pathSelector)
	return nodes, err
}

// ResolveLinks iteratively resolves names by walking the link hierarchy.
// Every node is fetched from the Fetcher, resolving the next name.
// Returns the list of nodes forming the path, starting with ndd. This list is
// guaranteed never to be empty.
//
// ResolveLinks(nd, []string{"foo", "bar", "baz"})
// would retrieve "baz" in ("bar" in ("foo" in nd.Links).Links).Links
//
// Note: if/when the context is cancelled or expires then if a multi-block ADL node is returned then it may not be
// possible to load certain values.
func (r *basicResolver) ResolveLinks(ctx context.Context, ndd ipld.Node, names []string) (nodes []ipld.Node, err error) {
	ctx, span := internal.StartSpan(ctx, "basicResolver.ResolveLinks")
	defer span.End()

	defer log.Debugw("resolvePathComponents", "names", names, "error", err)
	// create a selector to traverse and match all path segments
	pathSelector := pathAllSelector(names)

	session := r.FetcherFactory.NewSession(ctx)

	// traverse selector
	nodes = []ipld.Node{ndd}
	err = session.NodeMatching(ctx, ndd, pathSelector, func(res fetcher.FetchResult) error {
		nodes = append(nodes, res.Node)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return nodes, err
}

// Finds nodes matching the selector starting with a cid. Returns the matched nodes, the cid of the block containing
// the last node, and the depth of the last node within its block (root is depth 0).
func (r *basicResolver) resolveNodes(ctx context.Context, c cid.Cid, sel ipld.Node) ([]ipld.Node, cid.Cid, int, error) {
	ctx, span := internal.StartSpan(ctx, "basicResolver.resolveNodes", trace.WithAttributes(attribute.Stringer("CID", c)))
	defer span.End()
	session := r.FetcherFactory.NewSession(ctx)

	// traverse selector
	lastLink := cid.Undef
	depth := 0
	nodes := []ipld.Node{}
	err := fetcherhelpers.BlockMatching(ctx, session, cidlink.Link{Cid: c}, sel, func(res fetcher.FetchResult) error {
		if res.LastBlockLink == nil {
			res.LastBlockLink = cidlink.Link{Cid: c}
		}
		cidLnk, ok := res.LastBlockLink.(cidlink.Link)
		if !ok {
			return fmt.Errorf("link is not a cidlink: %v", cidLnk)
		}

		// if we hit a block boundary
		if !lastLink.Equals(cidLnk.Cid) {
			depth = 0
			lastLink = cidLnk.Cid
		} else {
			depth++
		}

		nodes = append(nodes, res.Node)
		return nil
	})
	if err != nil {
		return nil, cid.Undef, 0, err
	}

	return nodes, lastLink, depth, nil
}

func pathLeafSelector(path []string) ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return pathSelector(path, ssb, func(p string, s builder.SelectorSpec) builder.SelectorSpec {
		return ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) { efsb.Insert(p, s) })
	})
}

func pathAllSelector(path []string) ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return pathSelector(path, ssb, func(p string, s builder.SelectorSpec) builder.SelectorSpec {
		return ssb.ExploreUnion(
			ssb.Matcher(),
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) { efsb.Insert(p, s) }),
		)
	})
}

func pathSelector(path []string, ssb builder.SelectorSpecBuilder, reduce func(string, builder.SelectorSpec) builder.SelectorSpec) ipld.Node {
	spec := ssb.Matcher()
	for i := len(path) - 1; i >= 0; i-- {
		spec = reduce(path[i], spec)
	}
	return spec.Node()
}
