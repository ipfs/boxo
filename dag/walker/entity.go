package walker

import (
	"context"

	blockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/unixfs"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

// EntityType represents the semantic type of a DAG entity.
type EntityType int

const (
	EntityUnknown   EntityType = iota
	EntityFile                 // UnixFS file root (not its chunks)
	EntityDirectory            // UnixFS flat directory
	EntityHAMTShard            // UnixFS HAMT sharded directory bucket
	EntitySymlink              // UnixFS symbolic link
)

// NodeFetcher returns child link CIDs and entity type for a given CID.
// Used by [WalkEntityRoots] which needs UnixFS type detection to decide
// whether to descend into children (directories, HAMT shards) or stop
// (files, symlinks).
type NodeFetcher func(ctx context.Context, c cid.Cid) (linkCIDs []cid.Cid, entityType EntityType, err error)

// NodeFetcherFromBlockstore creates a [NodeFetcher] backed by a local
// blockstore. Like [LinksFetcherFromBlockstore], it decodes blocks via
// ipld-prime's global multicodec registry (dag-pb, dag-cbor, raw, etc.)
// and handles identity CIDs transparently via [blockstore.NewIdStore].
//
// Entity type detection:
//   - dag-pb with valid UnixFS Data: file, directory, HAMT shard, or symlink
//   - dag-pb without valid UnixFS Data: EntityUnknown
//   - raw codec: EntityFile (small file stored as a single raw block)
//   - all other codecs (dag-cbor, dag-json, etc.): EntityUnknown
func NodeFetcherFromBlockstore(bs blockstore.Blockstore) NodeFetcher {
	ls := linkSystemForBlockstore(bs)

	return func(ctx context.Context, c cid.Cid) ([]cid.Cid, EntityType, error) {
		lnk := cidlink.Link{Cid: c}
		nd, err := ls.Load(ipld.LinkContext{Ctx: ctx}, lnk, basicnode.Prototype.Any)
		if err != nil {
			return nil, EntityUnknown, err
		}

		links := collectLinks(c, nd)
		entityType := detectEntityType(c, nd)
		return links, entityType, nil
	}
}

// detectEntityType infers the UnixFS entity type from an ipld-prime
// decoded node. For dag-pb nodes, it reads the "Data" field and parses
// it as UnixFS protobuf. For raw codec nodes, it returns EntityFile.
// For everything else, it returns EntityUnknown.
func detectEntityType(c cid.Cid, nd ipld.Node) EntityType {
	// raw codec: small file stored as a single block
	if c.Prefix().Codec == cid.Raw {
		return EntityFile
	}

	// only dag-pb has UnixFS semantics; other codecs are unknown
	if c.Prefix().Codec != cid.DagProtobuf {
		return EntityUnknown
	}

	// dag-pb: try to read the "Data" field for UnixFS type
	dataField, err := nd.LookupByString("Data")
	if err != nil || dataField.IsAbsent() || dataField.IsNull() {
		return EntityUnknown
	}

	dataBytes, err := dataField.AsBytes()
	if err != nil {
		return EntityUnknown
	}

	fsn, err := unixfs.FSNodeFromBytes(dataBytes)
	if err != nil {
		return EntityUnknown
	}

	switch fsn.Type() {
	case unixfs.TFile, unixfs.TRaw:
		return EntityFile
	case unixfs.TDirectory:
		return EntityDirectory
	case unixfs.THAMTShard:
		return EntityHAMTShard
	case unixfs.TSymlink:
		return EntitySymlink
	default:
		return EntityUnknown
	}
}

// WalkEntityRoots traverses a DAG calling emit for each entity root.
//
// Entity roots are semantic boundaries in the DAG:
//   - File/symlink roots: emitted, children (chunks) NOT traversed
//   - Directory roots: emitted, children recursed
//   - HAMT shard nodes: emitted (needed for directory enumeration),
//     children recursed
//   - Non-UnixFS nodes (dag-cbor, dag-json, etc.): emitted AND children
//     recursed to discover further content. The +entities optimization
//     (skip chunks) only applies to UnixFS files; for all other codecs,
//     every reachable CID is emitted.
//   - Raw leaf nodes: emitted (no children to recurse)
//
// Uses the same option types as [WalkDAG]: [WithVisitedTracker] for
// bloom/map dedup across walks, [WithLocality] for MFS locality checks.
func WalkEntityRoots(
	ctx context.Context,
	root cid.Cid,
	fetch NodeFetcher,
	emit func(cid.Cid) bool,
	opts ...Option,
) error {
	cfg := &walkConfig{}
	for _, o := range opts {
		o(cfg)
	}

	stack := []cid.Cid{root}

	for len(stack) > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// pop
		c := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// dedup via tracker
		if cfg.tracker != nil && !cfg.tracker.Visit(c) {
			continue
		}

		// locality check
		if cfg.locality != nil {
			local, err := cfg.locality(ctx, c)
			if err != nil {
				log.Errorf("entity walk: locality check %s: %s", c, err)
				continue
			}
			if !local {
				continue
			}
		}

		// fetch block and detect entity type
		children, entityType, err := fetch(ctx, c)
		if err != nil {
			log.Errorf("entity walk: fetch %s: %s", c, err)
			continue
		}

		// decide whether to descend into children
		descend := entityType != EntityFile && entityType != EntitySymlink
		if descend {
			stack = append(stack, children...)
		}

		// identity CIDs embed data inline -- never emit them, but
		// we still descend (above) so an inlined dag-pb directory's
		// normal children get provided
		if isIdentityCID(c) {
			continue
		}

		if !emit(c) {
			return nil
		}
	}

	return nil
}
