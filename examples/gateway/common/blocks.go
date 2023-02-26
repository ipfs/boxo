package common

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	gopath "path"
	"strings"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/go-libipfs/gateway"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-namesys"
	"github.com/ipfs/go-namesys/resolve"
	ipfspath "github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	"github.com/ipfs/go-unixfs"
	ufile "github.com/ipfs/go-unixfs/file"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/ipfs/go-unixfsnode"
	iface "github.com/ipfs/interface-go-ipfs-core"
	nsopts "github.com/ipfs/interface-go-ipfs-core/options/namesys"
	ifacepath "github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/ipld/go-car"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	mc "github.com/multiformats/go-multicodec"
)

func NewBlocksHandler(gw *BlocksGateway, port int) http.Handler {
	headers := map[string][]string{}
	gateway.AddAccessControlHeaders(headers)

	conf := gateway.Config{
		Headers: headers,
	}

	mux := http.NewServeMux()
	gwHandler := gateway.NewHandler(conf, gw)
	mux.Handle("/ipfs/", gwHandler)
	mux.Handle("/ipns/", gwHandler)
	return mux
}

type BlocksGateway struct {
	blockStore   blockstore.Blockstore
	blockService blockservice.BlockService
	dagService   format.DAGService
	resolver     resolver.Resolver

	// Optional routing system to handle /ipns addresses.
	namesys namesys.NameSystem
	routing routing.ValueStore
}

var _ gateway.API = (*BlocksGateway)(nil)

func NewBlocksGateway(blockService blockservice.BlockService, routing routing.ValueStore) (*BlocksGateway, error) {
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
	resolver := resolver.NewBasicResolver(fetcher)

	// Setup a name system so that we are able to resolve /ipns links.
	var (
		ns  namesys.NameSystem
		err error
	)
	if routing != nil {
		ns, err = namesys.NewNameSystem(routing)
		if err != nil {
			return nil, err
		}
	}

	return &BlocksGateway{
		blockStore:   blockService.Blockstore(),
		blockService: blockService,
		dagService:   dagService,
		resolver:     resolver,
		routing:      routing,
		namesys:      ns,
	}, nil
}

func (api *BlocksGateway) Get(ctx context.Context, path gateway.ImmutablePath, opt ...gateway.GetOpt) (gateway.ContentPathMetadata, files.Node, error) {
	var opts gateway.GetOptions
	for _, o := range opt {
		if err := o(&opts); err != nil {
			return gateway.ContentPathMetadata{}, nil, err
		}
	}

	roots, lastSeg, err := api.getPathRoots(ctx, path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}

	md := gateway.ContentPathMetadata{
		PathSegmentRoots: roots,
		LastSegment:      lastSeg,
	}

	lastRoot := lastSeg.Cid()

	nd, err := api.dagService.Get(ctx, lastRoot)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}

	if opts.RawBlock {
		return md, files.NewBytesFile(nd.RawData()), nil
	}

	// This code path covers full graph, single file/directory, and range requests
	rootCodec := nd.Cid().Prefix().GetCodec()
	if rootCodec == uint64(mc.Raw) {
		return md, files.NewBytesFile(nd.RawData()), nil
	}
	if rootCodec != uint64(mc.DagPb) {
		return md, nil, fmt.Errorf("data is not UnixFS")
	}
	f, err := ufile.NewUnixfsFile(ctx, api.dagService, nd)
	if err != nil {
		return md, nil, err
	}
	return md, f, nil
}

func (api *BlocksGateway) Head(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, files.Node, error) {
	roots, lastSeg, err := api.getPathRoots(ctx, path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}

	md := gateway.ContentPathMetadata{
		PathSegmentRoots: roots,
		LastSegment:      lastSeg,
	}

	lastRoot := lastSeg.Cid()

	nd, err := api.dagService.Get(ctx, lastRoot)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}

	rootCodec := nd.Cid().Prefix().GetCodec()
	if rootCodec != uint64(mc.DagPb) {
		return md, files.NewBytesFile(nd.RawData()), nil
	}

	// TODO: We're not handling non-UnixFS dag-pb. There's a bit of a discrepancy between what we want from a HEAD request and a Resolve request here and we're using this for both
	fileNode, err := ufile.NewUnixfsFile(ctx, api.dagService, nd)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err
	}

	return md, fileNode, nil
}

func (api *BlocksGateway) GetCAR(ctx context.Context, path gateway.ImmutablePath) (gateway.ContentPathMetadata, io.ReadCloser, error, <-chan error) {
	// Same go-car settings as dag.export command
	store := dagStore{api: api, ctx: ctx}

	// TODO: When switching to exposing path blocks we'll want to add these as well
	roots, lastSeg, err := api.getPathRoots(ctx, path)
	if err != nil {
		return gateway.ContentPathMetadata{}, nil, err, nil
	}

	md := gateway.ContentPathMetadata{
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
		errCh <- c.Write(w)
		close(errCh)
	}()

	return md, r, nil, errCh
}

func (api *BlocksGateway) getPathRoots(ctx context.Context, contentPath gateway.ImmutablePath) ([]cid.Cid, ifacepath.Resolved, error) {
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
		resolvedSubPath, err := api.ResolvePath(ctx, ifacepath.New(sp.String()))
		if err != nil {
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
	return ds.api.GetBlock(ds.ctx, c)
}

func (api *BlocksGateway) ResolveMutable(ctx context.Context, p ifacepath.Path) (gateway.ImmutablePath, error) {
	err := p.IsValid()
	if err != nil {
		return gateway.ImmutablePath{}, err
	}

	ipath := ipfspath.Path(p.String())
	switch ipath.Segments()[0] {
	case "ipns":
		ipath, err = resolve.ResolveIPNS(ctx, api.namesys, ipath)
		if err != nil {
			return gateway.ImmutablePath{}, err
		}
		imPath, err := gateway.NewImmutablePath(ifacepath.New(ipath.String()))
		if err != nil {
			return gateway.ImmutablePath{}, err
		}
		return imPath, nil
	case "ipfs":
		imPath, err := gateway.NewImmutablePath(ifacepath.New(ipath.String()))
		if err != nil {
			return gateway.ImmutablePath{}, err
		}
		return imPath, nil
	default:
		return gateway.ImmutablePath{}, fmt.Errorf("unsupported path namespace: %s", p.Namespace())
	}
}

func (api *BlocksGateway) GetUnixFsNode(ctx context.Context, p ifacepath.Resolved) (files.Node, error) {
	nd, err := api.resolveNode(ctx, p)
	if err != nil {
		return nil, err
	}

	return ufile.NewUnixfsFile(ctx, api.dagService, nd)
}

func (api *BlocksGateway) LsUnixFsDir(ctx context.Context, p ifacepath.Resolved) (<-chan iface.DirEntry, error) {
	node, err := api.resolveNode(ctx, p)
	if err != nil {
		return nil, err
	}

	dir, err := uio.NewDirectoryFromNode(api.dagService, node)
	if err != nil {
		return nil, err
	}

	out := make(chan iface.DirEntry, uio.DefaultShardWidth)

	go func() {
		defer close(out)
		for l := range dir.EnumLinksAsync(ctx) {
			select {
			case out <- api.processLink(ctx, l):
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

func (api *BlocksGateway) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return api.blockService.GetBlock(ctx, c)
}

func (api *BlocksGateway) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	if api.routing == nil {
		return nil, routing.ErrNotSupported
	}

	// Fails fast if the CID is not an encoded Libp2p Key, avoids wasteful
	// round trips to the remote routing provider.
	if mc.Code(c.Type()) != mc.Libp2pKey {
		return nil, errors.New("provided cid is not an encoded libp2p key")
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

	return nil, errors.New("not implemented")
}

func (api *BlocksGateway) IsCached(ctx context.Context, p ifacepath.Path) bool {
	rp, err := api.ResolvePath(ctx, p)
	if err != nil {
		return false
	}

	has, _ := api.blockStore.Has(ctx, rp.Cid())
	return has
}

func (api *BlocksGateway) ResolvePath(ctx context.Context, p ifacepath.Path) (ifacepath.Resolved, error) {
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

func (api *BlocksGateway) resolveNode(ctx context.Context, p ifacepath.Path) (format.Node, error) {
	rp, err := api.ResolvePath(ctx, p)
	if err != nil {
		return nil, err
	}

	node, err := api.dagService.Get(ctx, rp.Cid())
	if err != nil {
		return nil, fmt.Errorf("get node: %w", err)
	}
	return node, nil
}

func (api *BlocksGateway) processLink(ctx context.Context, result unixfs.LinkResult) iface.DirEntry {
	if result.Err != nil {
		return iface.DirEntry{Err: result.Err}
	}

	link := iface.DirEntry{
		Name: result.Link.Name,
		Cid:  result.Link.Cid,
	}

	switch link.Cid.Type() {
	case cid.Raw:
		link.Type = iface.TFile
		link.Size = result.Link.Size
	case cid.DagProtobuf:
		link.Size = result.Link.Size
	}

	return link
}
