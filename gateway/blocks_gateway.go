package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	gopath "path"
	"strings"

	"github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	nsopts "github.com/ipfs/boxo/coreiface/options/namesys"
	ifacepath "github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/fetcher"
	bsfetcher "github.com/ipfs/boxo/fetcher/impl/blockservice"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/boxo/ipld/car/v2/storage"
	"github.com/ipfs/boxo/ipld/merkledag"
	ufile "github.com/ipfs/boxo/ipld/unixfs/file"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/namesys/resolve"
	ipfspath "github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/path/resolver"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	mc "github.com/multiformats/go-multicodec"

	// Ensure basic codecs are registered.
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
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

func (api *BlocksGateway) Get(ctx context.Context, path ImmutablePath, ranges ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	md, nd, err := api.getNode(ctx, path)
	if err != nil {
		return md, nil, err
	}

	rootCodec := nd.Cid().Prefix().GetCodec()
	// This covers both Raw blocks and terminal IPLD codecs like dag-cbor and dag-json
	// Note: while only cbor, json, dag-cbor, and dag-json are currently supported by gateways this could change
	if rootCodec != uint64(mc.DagPb) {
		return md, NewGetResponseFromFile(files.NewBytesFile(nd.RawData())), nil
	}

	// This code path covers full graph, single file/directory, and range requests
	f, err := ufile.NewUnixfsFile(ctx, api.dagService, nd)
	// Note: there is an assumption here that non-UnixFS dag-pb should not be returned which is currently valid
	if err != nil {
		return md, nil, err
	}

	if d, ok := f.(files.Directory); ok {
		dir, err := uio.NewDirectoryFromNode(api.dagService, nd)
		if err != nil {
			return md, nil, err
		}
		sz, err := d.Size()
		if err != nil {
			return ContentPathMetadata{}, nil, fmt.Errorf("could not get cumulative directory DAG size: %w", err)
		}
		if sz < 0 {
			return ContentPathMetadata{}, nil, fmt.Errorf("directory cumulative DAG size cannot be negative")
		}
		return md, NewGetResponseFromDirectoryListing(uint64(sz), dir.EnumLinksAsync(ctx)), nil
	}
	if file, ok := f.(files.File); ok {
		return md, NewGetResponseFromFile(file), nil
	}

	return ContentPathMetadata{}, nil, fmt.Errorf("data was not a valid file or directory: %w", ErrInternalServerError) // TODO: should there be a gateway invalid content type to abstract over the various IPLD error types?
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

func (api *BlocksGateway) GetCAR(ctx context.Context, p ImmutablePath, params CarParams) (io.ReadCloser, error) {
	contentPathStr := p.String()
	if !strings.HasPrefix(contentPathStr, "/ipfs/") {
		return nil, fmt.Errorf("path does not have /ipfs/ prefix")
	}
	firstSegment, _, _ := strings.Cut(contentPathStr[6:], "/")
	rootCid, err := cid.Decode(firstSegment)
	if err != nil {
		return nil, err
	}

	r, w := io.Pipe()
	go func() {
		cw, err := storage.NewWritable(w, []cid.Cid{rootCid}, car.WriteAsCarV1(true))
		if err != nil {
			// io.PipeWriter.CloseWithError always returns nil.
			_ = w.CloseWithError(err)
			return
		}

		blockGetter := merkledag.NewDAGService(api.blockService).Session(ctx)

		blockGetter = &nodeGetterToCarExporer{
			ng: blockGetter,
			cw: cw,
		}

		// Setup the UnixFS resolver.
		f := newNodeGetterFetcherSingleUseFactory(ctx, blockGetter)
		pathResolver := resolver.NewBasicResolver(f)

		lsys := cidlink.DefaultLinkSystem()
		unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
		lsys.StorageReadOpener = blockOpener(ctx, blockGetter)

		// TODO: support selectors passed as request param: https://github.com/ipfs/kubo/issues/8769
		// TODO: this is very slow if blocks are remote due to linear traversal. Do we need deterministic traversals here?
		carWriteErr := walkGatewaySimpleSelector(ctx, ipfspath.Path(contentPathStr), params, &lsys, pathResolver)

		// io.PipeWriter.CloseWithError always returns nil.
		_ = w.CloseWithError(carWriteErr)
	}()

	return r, nil
}

// walkGatewaySimpleSelector walks the subgraph described by the path and terminal element parameters
func walkGatewaySimpleSelector(ctx context.Context, p ipfspath.Path, params CarParams, lsys *ipld.LinkSystem, pathResolver resolver.Resolver) error {
	// First resolve the path since we always need to.
	lastCid, remainder, err := pathResolver.ResolveToLastNode(ctx, p)
	if err != nil {
		return err
	}

	lctx := ipld.LinkContext{Ctx: ctx}
	pathTerminalCidLink := cidlink.Link{Cid: lastCid}

	// If the scope is the block, now we only need to retrieve the root block of the last element of the path.
	if params.Scope == dagScopeBlock {
		_, err = lsys.LoadRaw(lctx, pathTerminalCidLink)
		return err
	}

	// If we're asking for everything then give it
	if params.Scope == dagScopeAll {
		lastCidNode, err := lsys.Load(lctx, pathTerminalCidLink, basicnode.Prototype.Any)
		if err != nil {
			return err
		}

		sel, err := selector.ParseSelector(selectorparse.CommonSelector_ExploreAllRecursively)
		if err != nil {
			return err
		}

		progress := traversal.Progress{
			Cfg: &traversal.Config{
				Ctx:                            ctx,
				LinkSystem:                     *lsys,
				LinkTargetNodePrototypeChooser: bsfetcher.DefaultPrototypeChooser,
				LinkVisitOnlyOnce:              true, // This is safe for the "all" selector
			},
		}

		if err := progress.WalkMatching(lastCidNode, sel, func(progress traversal.Progress, node datamodel.Node) error {
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	// From now on, dag-scope=entity!
	// Since we need more of the graph load it to figure out what we have
	// This includes determining if the terminal node is UnixFS or not
	pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})

	np, err := pc(pathTerminalCidLink, lctx)
	if err != nil {
		return err
	}

	lastCidNode, err := lsys.Load(lctx, pathTerminalCidLink, np)
	if err != nil {
		return err
	}

	if pbn, ok := lastCidNode.(dagpb.PBNode); !ok {
		// If it's not valid dag-pb then we're done
		return nil
	} else if len(remainder) > 0 {
		// If we're trying to path into dag-pb node that's invalid and we're done
		return nil
	} else if !pbn.FieldData().Exists() {
		// If it's not valid UnixFS then we're done
		return nil
	} else if unixfsFieldData, decodeErr := data.DecodeUnixFSData(pbn.Data.Must().Bytes()); decodeErr != nil {
		// If it's not valid dag-pb and UnixFS then we're done
		return nil
	} else {
		switch unixfsFieldData.FieldDataType().Int() {
		case data.Data_Directory, data.Data_Symlink:
			// These types are non-recursive so we're done
			return nil
		case data.Data_Raw, data.Data_Metadata:
			// TODO: Is Data_Raw actually different from Data_File or is that a fiction?
			// TODO: Deal with UnixFS Metadata being handled differently in boxo/ipld/unixfs than go-unixfsnode
			// Not sure what to do here so just returning
			return nil
		case data.Data_HAMTShard:
			// Return all elements in the map
			_, err := lsys.KnownReifiers["unixfs-preload"](lctx, lastCidNode, lsys)
			if err != nil {
				return err
			}
			return nil
		case data.Data_File:
			nd, err := unixfsnode.Reify(lctx, lastCidNode, lsys)
			if err != nil {
				return err
			}

			fnd, ok := nd.(datamodel.LargeBytesNode)
			if !ok {
				return fmt.Errorf("could not process file since it did not present as large bytes")
			}
			f, err := fnd.AsLargeBytes()
			if err != nil {
				return err
			}

			// Get the entity range. If it's empty, assume the defaults (whole file).
			entityRange := params.Range
			if entityRange == nil {
				entityRange = &DagEntityByteRange{
					From: 0,
				}
			}

			from := entityRange.From

			// If we're starting to read based on the end of the file, find out where that is.
			var fileLength int64
			foundFileLength := false
			if entityRange.From < 0 {
				fileLength, err = f.Seek(0, io.SeekEnd)
				if err != nil {
					return err
				}
				from = fileLength + entityRange.From
				foundFileLength = true
			}

			// If we're reading until the end of the file then do it
			if entityRange.To == nil {
				if _, err := f.Seek(from, io.SeekStart); err != nil {
					return err
				}
				_, err = io.Copy(io.Discard, f)
				return err
			}

			to := *entityRange.To
			if (*entityRange.To) < 0 && !foundFileLength {
				fileLength, err = f.Seek(0, io.SeekEnd)
				if err != nil {
					return err
				}
				to = fileLength + *entityRange.To
				foundFileLength = true
			}

			numToRead := 1 + to - from
			if numToRead < 0 {
				return fmt.Errorf("tried to read less than zero bytes")
			}

			if _, err := f.Seek(from, io.SeekStart); err != nil {
				return err
			}
			_, err = io.CopyN(io.Discard, f, numToRead)
			return err
		default:
			// Not a supported type, so we're done
			return nil
		}
	}
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

type nodeGetterToCarExporer struct {
	ng format.NodeGetter
	cw storage.WritableCar
}

func (n *nodeGetterToCarExporer) Get(ctx context.Context, c cid.Cid) (format.Node, error) {
	nd, err := n.ng.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	if err := n.trySendBlock(ctx, nd); err != nil {
		return nil, err
	}

	return nd, nil
}

func (n *nodeGetterToCarExporer) GetMany(ctx context.Context, cids []cid.Cid) <-chan *format.NodeOption {
	ndCh := n.ng.GetMany(ctx, cids)
	outCh := make(chan *format.NodeOption)
	go func() {
		defer close(outCh)
		for nd := range ndCh {
			if nd.Err == nil {
				if err := n.trySendBlock(ctx, nd.Node); err != nil {
					select {
					case outCh <- &format.NodeOption{Err: err}:
					case <-ctx.Done():
					}
					return
				}
				select {
				case outCh <- nd:
				case <-ctx.Done():
				}
			}
		}
	}()
	return outCh
}

func (n *nodeGetterToCarExporer) trySendBlock(ctx context.Context, block blocks.Block) error {
	return n.cw.Put(ctx, block.Cid().KeyString(), block.RawData())
}

var _ format.NodeGetter = (*nodeGetterToCarExporer)(nil)

type nodeGetterFetcherSingleUseFactory struct {
	linkSystem   ipld.LinkSystem
	protoChooser traversal.LinkTargetNodePrototypeChooser
}

func newNodeGetterFetcherSingleUseFactory(ctx context.Context, ng format.NodeGetter) *nodeGetterFetcherSingleUseFactory {
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = blockOpener(ctx, ng)
	ls.NodeReifier = unixfsnode.Reify

	pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})

	return &nodeGetterFetcherSingleUseFactory{ls, pc}
}

func (n *nodeGetterFetcherSingleUseFactory) NewSession(ctx context.Context) fetcher.Fetcher {
	return n
}

func (n *nodeGetterFetcherSingleUseFactory) NodeMatching(ctx context.Context, root ipld.Node, selector ipld.Node, cb fetcher.FetchCallback) error {
	return n.nodeMatching(ctx, n.blankProgress(ctx), root, selector, cb)
}

func (n *nodeGetterFetcherSingleUseFactory) BlockOfType(ctx context.Context, link ipld.Link, nodePrototype ipld.NodePrototype) (ipld.Node, error) {
	return n.linkSystem.Load(ipld.LinkContext{}, link, nodePrototype)
}

func (n *nodeGetterFetcherSingleUseFactory) BlockMatchingOfType(ctx context.Context, root ipld.Link, selector ipld.Node, nodePrototype ipld.NodePrototype, cb fetcher.FetchCallback) error {
	// retrieve first node
	prototype, err := n.PrototypeFromLink(root)
	if err != nil {
		return err
	}
	node, err := n.BlockOfType(ctx, root, prototype)
	if err != nil {
		return err
	}

	progress := n.blankProgress(ctx)
	progress.LastBlock.Link = root
	return n.nodeMatching(ctx, progress, node, selector, cb)
}

func (n *nodeGetterFetcherSingleUseFactory) PrototypeFromLink(lnk ipld.Link) (ipld.NodePrototype, error) {
	return n.protoChooser(lnk, ipld.LinkContext{})
}

func (n *nodeGetterFetcherSingleUseFactory) nodeMatching(ctx context.Context, initialProgress traversal.Progress, node ipld.Node, match ipld.Node, cb fetcher.FetchCallback) error {
	matchSelector, err := selector.ParseSelector(match)
	if err != nil {
		return err
	}
	return initialProgress.WalkMatching(node, matchSelector, func(prog traversal.Progress, n ipld.Node) error {
		return cb(fetcher.FetchResult{
			Node:          n,
			Path:          prog.Path,
			LastBlockPath: prog.LastBlock.Path,
			LastBlockLink: prog.LastBlock.Link,
		})
	})
}

func (n *nodeGetterFetcherSingleUseFactory) blankProgress(ctx context.Context) traversal.Progress {
	return traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem:                     n.linkSystem,
			LinkTargetNodePrototypeChooser: n.protoChooser,
		},
	}
}

func blockOpener(ctx context.Context, ng format.NodeGetter) ipld.BlockReadOpener {
	return func(_ ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}

		blk, err := ng.Get(ctx, cidLink.Cid)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(blk.RawData()), nil
	}
}

var _ fetcher.Fetcher = (*nodeGetterFetcherSingleUseFactory)(nil)
var _ fetcher.Factory = (*nodeGetterFetcherSingleUseFactory)(nil)
