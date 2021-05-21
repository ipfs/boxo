package io

import (
	"context"
	"fmt"
	"os"

	mdag "github.com/ipfs/go-merkledag"

	format "github.com/ipfs/go-unixfs"
	hamt "github.com/ipfs/go-unixfs/hamt"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("unixfs")

// HAMTShardingSize is a global option that allows switching to a HAMTDirectory
// when the BasicDirectory grows above the size (in bytes) signalled by this
// flag. The default size of 0 disables the option.
// The size is not the *exact* block size of the encoded BasicDirectory but just
// the estimated size based byte length of links name and CID (BasicDirectory's
// ProtoNode doesn't use the Data field so this estimate is pretty accurate).
var HAMTShardingSize = 0

// DefaultShardWidth is the default value used for hamt sharding width.
var DefaultShardWidth = 256

// Directory defines a UnixFS directory. It is used for creating, reading and
// editing directories. It allows to work with different directory schemes,
// like the basic or the HAMT implementation.
//
// It just allows to perform explicit edits on a single directory, working with
// directory trees is out of its scope, they are managed by the MFS layer
// (which is the main consumer of this interface).
type Directory interface {

	// SetCidBuilder sets the CID Builder of the root node.
	SetCidBuilder(cid.Builder)

	// AddChild adds a (name, key) pair to the root node.
	AddChild(context.Context, string, ipld.Node) error

	// ForEachLink applies the given function to Links in the directory.
	ForEachLink(context.Context, func(*ipld.Link) error) error

	// EnumLinksAsync returns a channel which will receive Links in the directory
	// as they are enumerated, where order is not gauranteed
	EnumLinksAsync(context.Context) <-chan format.LinkResult

	// Links returns the all the links in the directory node.
	Links(context.Context) ([]*ipld.Link, error)

	// Find returns the root node of the file named 'name' within this directory.
	// In the case of HAMT-directories, it will traverse the tree.
	//
	// Returns os.ErrNotExist if the child does not exist.
	Find(context.Context, string) (ipld.Node, error)

	// RemoveChild removes the child with the given name.
	//
	// Returns os.ErrNotExist if the child doesn't exist.
	RemoveChild(context.Context, string) error

	// GetNode returns the root of this directory.
	GetNode() (ipld.Node, error)

	// GetCidBuilder returns the CID Builder used.
	GetCidBuilder() cid.Builder
}

// TODO: Evaluate removing `dserv` from this layer and providing it in MFS.
// (The functions should in that case add a `DAGService` argument.)

// BasicDirectory is the basic implementation of `Directory`. All the entries
// are stored in a single node.
type BasicDirectory struct {
	node  *mdag.ProtoNode
	dserv ipld.DAGService

	// Internal variable used to cache the estimated size of the basic directory:
	// for each link, aggregate link name + link CID. DO NOT CHANGE THIS
	// as it will affect the HAMT transition behavior in HAMTShardingSize.
	// (We maintain this value up to date even if the HAMTShardingSize is off
	// since potentially the option could be activated on the fly.)
	estimatedSize int
}

// HAMTDirectory is the HAMT implementation of `Directory`.
// (See package `hamt` for more information.)
type HAMTDirectory struct {
	shard *hamt.Shard
	dserv ipld.DAGService
}

func newEmptyBasicDirectory(dserv ipld.DAGService) *BasicDirectory {
	return newBasicDirectoryFromNode(dserv, format.EmptyDirNode())
}

func newBasicDirectoryFromNode(dserv ipld.DAGService, node *mdag.ProtoNode) *BasicDirectory {
	basicDir := new(BasicDirectory)
	basicDir.node = node
	basicDir.dserv = dserv

	// Scan node links (if any) to restore estimated size.
	basicDir.computeEstimatedSize()

	return basicDir
}

// NewDirectory returns a Directory implemented by UpgradeableDirectory
// containing a BasicDirectory that can be converted to a HAMTDirectory.
func NewDirectory(dserv ipld.DAGService) Directory {
	return &UpgradeableDirectory{newEmptyBasicDirectory(dserv)}
}

// ErrNotADir implies that the given node was not a unixfs directory
var ErrNotADir = fmt.Errorf("merkledag node was not a directory or shard")

// NewDirectoryFromNode loads a unixfs directory from the given IPLD node and
// DAGService.
func NewDirectoryFromNode(dserv ipld.DAGService, node ipld.Node) (Directory, error) {
	protoBufNode, ok := node.(*mdag.ProtoNode)
	if !ok {
		return nil, ErrNotADir
	}

	fsNode, err := format.FSNodeFromBytes(protoBufNode.Data())
	if err != nil {
		return nil, err
	}

	switch fsNode.Type() {
	case format.TDirectory:
		return &UpgradeableDirectory{newBasicDirectoryFromNode(dserv, protoBufNode.Copy().(*mdag.ProtoNode))}, nil
	case format.THAMTShard:
		shard, err := hamt.NewHamtFromDag(dserv, node)
		if err != nil {
			return nil, err
		}
		return &HAMTDirectory{
			dserv: dserv,
			shard: shard,
		}, nil
	}

	return nil, ErrNotADir
}

func (d *BasicDirectory) computeEstimatedSize() {
	d.estimatedSize = 0
	d.ForEachLink(nil, func(l *ipld.Link) error {
		d.addToEstimatedSize(l.Name, l.Cid)
		return nil
	})
}

func estimatedLinkSize(linkName string, linkCid cid.Cid) int {
	return len(linkName) + linkCid.ByteLen()
}

func (d *BasicDirectory) addToEstimatedSize(name string, linkCid cid.Cid) {
	d.estimatedSize += estimatedLinkSize(name, linkCid)
}

func (d *BasicDirectory) removeFromEstimatedSize(name string, linkCid cid.Cid) {
	d.estimatedSize -= estimatedLinkSize(name, linkCid)
	if d.estimatedSize < 0 {
		// Something has gone very wrong. Log an error and recompute the
		// size from scratch.
		log.Error("BasicDirectory's estimatedSize went below 0")
		d.computeEstimatedSize()
	}
}

// SetCidBuilder implements the `Directory` interface.
func (d *BasicDirectory) SetCidBuilder(builder cid.Builder) {
	d.node.SetCidBuilder(builder)
}

// AddChild implements the `Directory` interface. It adds (or replaces)
// a link to the given `node` under `name`.
func (d *BasicDirectory) AddChild(ctx context.Context, name string, node ipld.Node) error {
	// Remove old link (if it existed; ignore `ErrNotExist` otherwise).
	err := d.RemoveChild(ctx, name)
	if err != nil && err != os.ErrNotExist {
		return err
	}

	err = d.node.AddNodeLink(name, node)
	if err != nil {
		return err
	}
	d.addToEstimatedSize(name, node.Cid())
	return nil
}

// EnumLinksAsync returns a channel which will receive Links in the directory
// as they are enumerated, where order is not gauranteed
func (d *BasicDirectory) EnumLinksAsync(ctx context.Context) <-chan format.LinkResult {
	linkResults := make(chan format.LinkResult)
	go func() {
		defer close(linkResults)
		for _, l := range d.node.Links() {
			select {
			case linkResults <- format.LinkResult{
				Link: l,
				Err:  nil,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return linkResults
}

// ForEachLink implements the `Directory` interface.
func (d *BasicDirectory) ForEachLink(ctx context.Context, f func(*ipld.Link) error) error {
	for _, l := range d.node.Links() {
		if err := f(l); err != nil {
			return err
		}
	}
	return nil
}

// Links implements the `Directory` interface.
func (d *BasicDirectory) Links(ctx context.Context) ([]*ipld.Link, error) {
	return d.node.Links(), nil
}

// Find implements the `Directory` interface.
func (d *BasicDirectory) Find(ctx context.Context, name string) (ipld.Node, error) {
	lnk, err := d.node.GetNodeLink(name)
	if err == mdag.ErrLinkNotFound {
		err = os.ErrNotExist
	}
	if err != nil {
		return nil, err
	}

	return d.dserv.Get(ctx, lnk.Cid)
}

// RemoveChild implements the `Directory` interface.
func (d *BasicDirectory) RemoveChild(ctx context.Context, name string) error {
	// We need to *retrieve* the link before removing it to update the estimated
	// size. This means we may iterate the links slice twice: if traversing this
	// becomes a problem, a factor of 2 isn't going to make much of a difference.
	// We'd likely need to cache a link resolution map in that case.
	link, err := d.node.GetNodeLink(name)
	if err == mdag.ErrLinkNotFound {
		return os.ErrNotExist
	}
	if err != nil {
		return err // at the moment there is no other error besides ErrLinkNotFound
	}

	// The name actually existed so we should update the estimated size.
	d.removeFromEstimatedSize(link.Name, link.Cid)

	return d.node.RemoveNodeLink(name)
	// GetNodeLink didn't return ErrLinkNotFound so this won't fail with that
	// and we don't need to convert the error again.
}

// GetNode implements the `Directory` interface.
func (d *BasicDirectory) GetNode() (ipld.Node, error) {
	return d.node, nil
}

// GetCidBuilder implements the `Directory` interface.
func (d *BasicDirectory) GetCidBuilder() cid.Builder {
	return d.node.CidBuilder()
}

// SwitchToSharding returns a HAMT implementation of this directory.
func (d *BasicDirectory) SwitchToSharding(ctx context.Context) (Directory, error) {
	hamtDir := new(HAMTDirectory)
	hamtDir.dserv = d.dserv

	shard, err := hamt.NewShard(d.dserv, DefaultShardWidth)
	if err != nil {
		return nil, err
	}
	shard.SetCidBuilder(d.node.CidBuilder())
	hamtDir.shard = shard

	for _, lnk := range d.node.Links() {
		node, err := d.dserv.Get(ctx, lnk.Cid)
		if err != nil {
			return nil, err
		}

		err = hamtDir.shard.Set(ctx, lnk.Name, node)
		if err != nil {
			return nil, err
		}
	}

	return hamtDir, nil
}

// SetCidBuilder implements the `Directory` interface.
func (d *HAMTDirectory) SetCidBuilder(builder cid.Builder) {
	d.shard.SetCidBuilder(builder)
}

// AddChild implements the `Directory` interface.
func (d *HAMTDirectory) AddChild(ctx context.Context, name string, nd ipld.Node) error {
	return d.shard.Set(ctx, name, nd)
}

// ForEachLink implements the `Directory` interface.
func (d *HAMTDirectory) ForEachLink(ctx context.Context, f func(*ipld.Link) error) error {
	return d.shard.ForEachLink(ctx, f)
}

// EnumLinksAsync returns a channel which will receive Links in the directory
// as they are enumerated, where order is not gauranteed
func (d *HAMTDirectory) EnumLinksAsync(ctx context.Context) <-chan format.LinkResult {
	return d.shard.EnumLinksAsync(ctx)
}

// Links implements the `Directory` interface.
func (d *HAMTDirectory) Links(ctx context.Context) ([]*ipld.Link, error) {
	return d.shard.EnumLinks(ctx)
}

// Find implements the `Directory` interface. It will traverse the tree.
func (d *HAMTDirectory) Find(ctx context.Context, name string) (ipld.Node, error) {
	lnk, err := d.shard.Find(ctx, name)
	if err != nil {
		return nil, err
	}

	return lnk.GetNode(ctx, d.dserv)
}

// RemoveChild implements the `Directory` interface.
func (d *HAMTDirectory) RemoveChild(ctx context.Context, name string) error {
	return d.shard.Remove(ctx, name)
}

// GetNode implements the `Directory` interface.
func (d *HAMTDirectory) GetNode() (ipld.Node, error) {
	return d.shard.Node()
}

// GetCidBuilder implements the `Directory` interface.
func (d *HAMTDirectory) GetCidBuilder() cid.Builder {
	return d.shard.CidBuilder()
}

// UpgradeableDirectory wraps a Directory interface and provides extra logic
// to upgrade from its BasicDirectory implementation to HAMTDirectory.
type UpgradeableDirectory struct {
	Directory
}

var _ Directory = (*UpgradeableDirectory)(nil)

// AddChild implements the `Directory` interface. We check when adding new entries
// if we should switch to HAMTDirectory according to global option(s).
func (d *UpgradeableDirectory) AddChild(ctx context.Context, name string, nd ipld.Node) error {
	err := d.Directory.AddChild(ctx, name, nd)
	if err != nil {
		return err
	}

	// Evaluate possible HAMT upgrade.
	if HAMTShardingSize == 0 {
		return nil
	}
	basicDir, ok := d.Directory.(*BasicDirectory)
	if !ok {
		return nil
	}
	if basicDir.estimatedSize >= HAMTShardingSize {
		// Ideally to minimize performance we should check if this last
		// `AddChild` call would bring the directory size over the threshold
		// *before* executing it since we would end up switching anyway and
		// that call would be "wasted". This is a minimal performance impact
		// and we prioritize a simple code base.
		hamtDir, err := basicDir.SwitchToSharding(ctx)
		if err != nil {
			return err
		}
		d.Directory = hamtDir
	}

	return nil
}
