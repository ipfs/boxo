package common

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	gopath "path"

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
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
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
