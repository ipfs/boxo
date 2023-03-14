package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	gopath "path"
	"strings"

	"go.uber.org/multierr"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-namesys"
	"github.com/ipfs/go-namesys/resolve"
	ipfspath "github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	ufile "github.com/ipfs/go-unixfs/file"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/ipfs/go-unixfsnode"
	nsopts "github.com/ipfs/interface-go-ipfs-core/options/namesys"
	ifacepath "github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/ipld/go-car"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	mc "github.com/multiformats/go-multicodec"
)

type BlocksGateway struct {
	blockStore   blockstore.Blockstore
	blockService blockservice.BlockService
	dagService   format.DAGService
	resolver     resolver.Resolver

	// Optional routing system to handle /ipns addresses.
	namesys namesys.NameSystem
	routing routing.ValueStore
}

var _ IPFSBackend = (*BlocksGateway)(nil)

type gwOptions struct {
	ns namesys.NameSystem
	vs routing.ValueStore
}

// WithNameSystem sets the name system to use for the gateway. If not set it will use a default DNSLink resolver
// along with any configured ValueStore
func WithNameSystem(ns namesys.NameSystem) BlockGatewayOption {
	return func(opts *gwOptions) error {
		opts.ns = ns
		return nil
	}
}

// WithValueStore sets the ValueStore to use for the gateway
func WithValueStore(vs routing.ValueStore) BlockGatewayOption {
	return func(opts *gwOptions) error {
		opts.vs = vs
		return nil
	}
}

type BlockGatewayOption func(gwOptions *gwOptions) error

func NewBlocksGateway(blockService blockservice.BlockService, opts ...BlockGatewayOption) (*BlocksGateway, error) {
	var compiledOptions gwOptions
	for _, o := range opts {
		if err := o(&compiledOptions); err != nil {
			return nil, err
		}
	}

	// Setup the DAG services, which use the CAR block store.
	dagService := merkledag.NewDAGService(blockService)

	// Setup the UnixFS resolver.
	fetcherConfig := bsfetcher.NewFetcherConfig(blockService)
	fetcherConfig.PrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})
	fetcher := fetcherConfig.WithReifier(unixfsnode.Reify)
	r := resolver.NewBasicResolver(fetcher)

	// Setup a name system so that we are able to resolve /ipns links.
	var (
		ns namesys.NameSystem
		vs routing.ValueStore
	)

	vs = compiledOptions.vs
	if vs == nil {
		vs = routinghelpers.Null{}
	}

	ns = compiledOptions.ns
	if ns == nil {
		dns, err := NewDNSResolver(nil, nil)
		if err != nil {
			return nil, err
		}

		ns, err = namesys.NewNameSystem(vs, namesys.WithDNSResolver(dns))
		if err != nil {
			return nil, err
		}
	}

	return &BlocksGateway{
		blockStore:   blockService.Blockstore(),
		blockService: blockService,
		dagService:   dagService,
		resolver:     r,
		routing:      vs,
		namesys:      ns,
	}, nil
}

func (api *BlocksGateway) Get(ctx context.Context, path ImmutablePath) (ContentPathMetadata, files.Node, error) {
	md, nd, err := api.getNode(ctx, path)
	if err != nil {
		return md, nil, err
	}

	rootCodec := nd.Cid().Prefix().GetCodec()
	// This covers both Raw blocks and terminal IPLD codecs like dag-cbor and dag-json
	// Note: while only cbor, json, dag-cbor, and dag-json are currently supported by gateways this could change
	if rootCodec != uint64(mc.DagPb) {
		return md, files.NewBytesFile(nd.RawData()), nil
	}

	// This code path covers full graph, single file/directory, and range requests
	f, err := ufile.NewUnixfsFile(ctx, api.dagService, nd)
	// Note: there is an assumption here that non-UnixFS dag-pb should not be returned which is currently valid
	if err != nil {
		return md, nil, err
	}

	if _, ok := f.(files.Directory); ok {
		dir, err := uio.NewDirectoryFromNode(api.dagService, nd)
		if err != nil {
			return md, nil, err
		}
		return md, &wrappedDirectory{node: f, dir: dir.EnumLinksAsync(ctx)}, nil
	}

	return md, f, nil
}

func (api *BlocksGateway) GetRange(ctx context.Context, path ImmutablePath, ranges ...GetRange) (ContentPathMetadata, files.File, error) {
	md, nd, err := api.getNode(ctx, path)
	if err != nil {
		return md, nil, err
	}

	// This code path covers full graph, single file/directory, and range requests
	n, err := ufile.NewUnixfsFile(ctx, api.dagService, nd)
	if err != nil {
		return md, nil, err
	}
	f, ok := n.(files.File)
	if !ok {
		return ContentPathMetadata{}, nil, NewErrorResponse(fmt.Errorf("can only do range requests on files, but did not get a file"), http.StatusBadRequest)
	}

	return md, f, nil
}

func (api *BlocksGateway) GetAll(ctx context.Context, path ImmutablePath) (ContentPathMetadata, files.Node, error) {
	md, nd, err := api.getNode(ctx, path)
	if err != nil {
		return md, nil, err
	}

	// This code path covers full graph, single file/directory, and range requests
	n, err := ufile.NewUnixfsFile(ctx, api.dagService, nd)
	if err != nil {
		return md, nil, err
	}
	return md, n, nil
}

func (api *BlocksGateway) GetBlock(ctx context.Context, path ImmutablePath) (ContentPathMetadata, files.File, error) {
	md, nd, err := api.getNode(ctx, path)
	if err != nil {
		return md, nil, err
	}

	return md, files.NewBytesFile(nd.RawData()), nil
}

func (api *BlocksGateway) Head(ctx context.Context, path ImmutablePath) (ContentPathMetadata, files.Node, error) {
	md, nd, err := api.getNode(ctx, path)
	if err != nil {
		return md, nil, err
	}

	rootCodec := nd.Cid().Prefix().GetCodec()
	if rootCodec != uint64(mc.DagPb) {
		return md, files.NewBytesFile(nd.RawData()), nil
	}

	// TODO: We're not handling non-UnixFS dag-pb. There's a bit of a discrepancy between what we want from a HEAD request and a Resolve request here and we're using this for both
	fileNode, err := ufile.NewUnixfsFile(ctx, api.dagService, nd)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	return md, fileNode, nil
}

func (api *BlocksGateway) GetCAR(ctx context.Context, path ImmutablePath) (ContentPathMetadata, io.ReadCloser, <-chan error, error) {
	// Same go-car settings as dag.export command
	store := dagStore{api: api, ctx: ctx}

	// TODO: When switching to exposing path blocks we'll want to add these as well
	roots, lastSeg, err := api.getPathRoots(ctx, path)
	if err != nil {
		return ContentPathMetadata{}, nil, nil, err
	}

	md := ContentPathMetadata{
		PathSegmentRoots: roots,
		LastSegment:      lastSeg,
	}

	rootCid := lastSeg.Cid()

	// TODO: support selectors passed as request param: https://github.com/ipfs/kubo/issues/8769
	// TODO: this is very slow if blocks are remote due to linear traversal. Do we need deterministic traversals here?
	dag := car.Dag{Root: rootCid, Selector: selectorparse.CommonSelector_ExploreAllRecursively}
	c := car.NewSelectiveCar(ctx, store, []car.Dag{dag}, car.TraverseLinksOnlyOnce())
	r, w := io.Pipe()

	errCh := make(chan error, 1)
	go func() {
		carWriteErr := c.Write(w)
		pipeCloseErr := w.Close()
		errCh <- multierr.Combine(carWriteErr, pipeCloseErr)
		close(errCh)
	}()

	return md, r, errCh, nil
}

func (api *BlocksGateway) getNode(ctx context.Context, path ImmutablePath) (ContentPathMetadata, format.Node, error) {
	roots, lastSeg, err := api.getPathRoots(ctx, path)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	md := ContentPathMetadata{
		PathSegmentRoots: roots,
		LastSegment:      lastSeg,
	}

	lastRoot := lastSeg.Cid()

	nd, err := api.dagService.Get(ctx, lastRoot)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	return md, nd, err
}

func (api *BlocksGateway) getPathRoots(ctx context.Context, contentPath ImmutablePath) ([]cid.Cid, ifacepath.Resolved, error) {
	/*
		These are logical roots where each CID represent one path segment
		and resolves to either a directory or the root block of a file.
		The main purpose of this header is allow HTTP caches to do smarter decisions
		around cache invalidation (eg. keep specific subdirectory/file if it did not change)
		A good example is Wikipedia, which is HAMT-sharded, but we only care about
		logical roots that represent each segment of the human-readable content
		path:
		Given contentPath = /ipns/en.wikipedia-on-ipfs.org/wiki/Block_of_Wikipedia_in_Turkey
		rootCidList is a generated by doing `ipfs resolve -r` on each sub path:
			/ipns/en.wikipedia-on-ipfs.org → bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze
			/ipns/en.wikipedia-on-ipfs.org/wiki/ → bafybeihn2f7lhumh4grizksi2fl233cyszqadkn424ptjajfenykpsaiw4
			/ipns/en.wikipedia-on-ipfs.org/wiki/Block_of_Wikipedia_in_Turkey → bafkreibn6euazfvoghepcm4efzqx5l3hieof2frhp254hio5y7n3hv5rma
		The result is an ordered array of values:
			X-Ipfs-Roots: bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze,bafybeihn2f7lhumh4grizksi2fl233cyszqadkn424ptjajfenykpsaiw4,bafkreibn6euazfvoghepcm4efzqx5l3hieof2frhp254hio5y7n3hv5rma
		Note that while the top one will change every time any article is changed,
		the last root (responsible for specific article) may not change at all.
	*/
	var sp strings.Builder
	var pathRoots []cid.Cid
	contentPathStr := contentPath.String()
	pathSegments := strings.Split(contentPathStr[6:], "/")
	sp.WriteString(contentPathStr[:5]) // /ipfs or /ipns
	var lastPath ifacepath.Resolved
	for _, root := range pathSegments {
		if root == "" {
			continue
		}
		sp.WriteString("/")
		sp.WriteString(root)
		resolvedSubPath, err := api.resolvePath(ctx, ifacepath.New(sp.String()))
		if err != nil {
			// TODO: should we be more explicit here and is this part of the Gateway API contract?
			// The issue here was that we returned datamodel.ErrWrongKind instead of this resolver error
			if isErrNotFound(err) {
				return nil, nil, resolver.ErrNoLink{Name: root, Node: lastPath.Cid()}
			}
			return nil, nil, err
		}
		lastPath = resolvedSubPath
		pathRoots = append(pathRoots, lastPath.Cid())
	}

	pathRoots = pathRoots[:len(pathRoots)-1]
	return pathRoots, lastPath, nil
}

// FIXME(@Jorropo): https://github.com/ipld/go-car/issues/315
type dagStore struct {
	api *BlocksGateway
	ctx context.Context
}

func (ds dagStore) Get(_ context.Context, c cid.Cid) (blocks.Block, error) {
	return ds.api.blockService.GetBlock(ds.ctx, c)
}

func (api *BlocksGateway) ResolveMutable(ctx context.Context, p ifacepath.Path) (ImmutablePath, error) {
	err := p.IsValid()
	if err != nil {
		return ImmutablePath{}, err
	}

	ipath := ipfspath.Path(p.String())
	switch ipath.Segments()[0] {
	case "ipns":
		ipath, err = resolve.ResolveIPNS(ctx, api.namesys, ipath)
		if err != nil {
			return ImmutablePath{}, err
		}
		imPath, err := NewImmutablePath(ifacepath.New(ipath.String()))
		if err != nil {
			return ImmutablePath{}, err
		}
		return imPath, nil
	case "ipfs":
		imPath, err := NewImmutablePath(ifacepath.New(ipath.String()))
		if err != nil {
			return ImmutablePath{}, err
		}
		return imPath, nil
	default:
		return ImmutablePath{}, NewErrorResponse(fmt.Errorf("unsupported path namespace: %s", p.Namespace()), http.StatusNotImplemented)
	}
}

func (api *BlocksGateway) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	if api.routing == nil {
		return nil, NewErrorResponse(errors.New("IPNS Record responses are not supported by this gateway"), http.StatusNotImplemented)
	}

	// Fails fast if the CID is not an encoded Libp2p Key, avoids wasteful
	// round trips to the remote routing provider.
	if mc.Code(c.Type()) != mc.Libp2pKey {
		return nil, NewErrorResponse(errors.New("cid codec must be libp2p-key"), http.StatusBadRequest)
	}

	// The value store expects the key itself to be encoded as a multihash.
	id, err := peer.FromCid(c)
	if err != nil {
		return nil, err
	}

	return api.routing.GetValue(ctx, "/ipns/"+string(id))
}

func (api *BlocksGateway) GetDNSLinkRecord(ctx context.Context, hostname string) (ifacepath.Path, error) {
	if api.namesys != nil {
		p, err := api.namesys.Resolve(ctx, "/ipns/"+hostname, nsopts.Depth(1))
		if err == namesys.ErrResolveRecursion {
			err = nil
		}
		return ifacepath.New(p.String()), err
	}

	return nil, NewErrorResponse(errors.New("not implemented"), http.StatusNotImplemented)
}

func (api *BlocksGateway) IsCached(ctx context.Context, p ifacepath.Path) bool {
	rp, err := api.resolvePath(ctx, p)
	if err != nil {
		return false
	}

	has, _ := api.blockStore.Has(ctx, rp.Cid())
	return has
}

func (api *BlocksGateway) ResolvePath(ctx context.Context, path ImmutablePath) (ContentPathMetadata, error) {
	roots, lastSeg, err := api.getPathRoots(ctx, path)
	if err != nil {
		return ContentPathMetadata{}, err
	}
	md := ContentPathMetadata{
		PathSegmentRoots: roots,
		LastSegment:      lastSeg,
	}
	return md, nil
}

func (api *BlocksGateway) resolvePath(ctx context.Context, p ifacepath.Path) (ifacepath.Resolved, error) {
	if _, ok := p.(ifacepath.Resolved); ok {
		return p.(ifacepath.Resolved), nil
	}

	err := p.IsValid()
	if err != nil {
		return nil, err
	}

	ipath := ipfspath.Path(p.String())
	if ipath.Segments()[0] == "ipns" {
		ipath, err = resolve.ResolveIPNS(ctx, api.namesys, ipath)
		if err != nil {
			return nil, err
		}
	}

	if ipath.Segments()[0] != "ipfs" {
		return nil, fmt.Errorf("unsupported path namespace: %s", p.Namespace())
	}

	node, rest, err := api.resolver.ResolveToLastNode(ctx, ipath)
	if err != nil {
		return nil, err
	}

	root, err := cid.Parse(ipath.Segments()[1])
	if err != nil {
		return nil, err
	}

	return ifacepath.NewResolvedPath(ipath, node, root, gopath.Join(rest...)), nil
}
