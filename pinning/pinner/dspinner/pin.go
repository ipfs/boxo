// Package dspinner implements structures and methods to keep track of
// which objects a user wants to keep stored locally.  This implementation
// stores pin data in a datastore.
package dspinner

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	ipfspinner "github.com/ipfs/go-ipfs-pinner"
	"github.com/ipfs/go-ipfs-pinner/dsindex"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	mdag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-merkledag/dagutils"
	"github.com/polydawn/refmt/cbor"
	"github.com/polydawn/refmt/obj/atlas"
)

const (
	basePath     = "/pins"
	pinKeyPath   = "/pins/pin"
	indexKeyPath = "/pins/index"
	dirtyKeyPath = "/pins/state/dirty"
)

var (
	// ErrNotPinned is returned when trying to unpin items that are not pinned.
	ErrNotPinned = errors.New("not pinned or pinned indirectly")

	log logging.StandardLogger = logging.Logger("pin")

	linkDirect, linkRecursive string

	pinCidDIndexPath string
	pinCidRIndexPath string
	pinNameIndexPath string

	dirtyKey = ds.NewKey(dirtyKeyPath)

	pinAtl atlas.Atlas
)

func init() {
	directStr, ok := ipfspinner.ModeToString(ipfspinner.Direct)
	if !ok {
		panic("could not find Direct pin enum")
	}
	linkDirect = directStr

	recursiveStr, ok := ipfspinner.ModeToString(ipfspinner.Recursive)
	if !ok {
		panic("could not find Recursive pin enum")
	}
	linkRecursive = recursiveStr

	pinCidRIndexPath = path.Join(indexKeyPath, "cidRindex")
	pinCidDIndexPath = path.Join(indexKeyPath, "cidDindex")
	pinNameIndexPath = path.Join(indexKeyPath, "nameIndex")

	pinAtl = atlas.MustBuild(
		atlas.BuildEntry(pin{}).StructMap().
			AddField("Cid", atlas.StructMapEntry{SerialName: "cid"}).
			AddField("Metadata", atlas.StructMapEntry{SerialName: "metadata", OmitEmpty: true}).
			AddField("Mode", atlas.StructMapEntry{SerialName: "mode"}).
			AddField("Name", atlas.StructMapEntry{SerialName: "name", OmitEmpty: true}).
			Complete(),
		atlas.BuildEntry(cid.Cid{}).Transform().
			TransformMarshal(atlas.MakeMarshalTransformFunc(func(live cid.Cid) ([]byte, error) { return live.MarshalBinary() })).
			TransformUnmarshal(atlas.MakeUnmarshalTransformFunc(func(serializable []byte) (cid.Cid, error) {
				c := cid.Cid{}
				err := c.UnmarshalBinary(serializable)
				if err != nil {
					return cid.Cid{}, err
				}
				return c, nil
			})).Complete(),
	)
	pinAtl = pinAtl.WithMapMorphism(atlas.MapMorphism{KeySortMode: atlas.KeySortMode_Strings})
}

// pinner implements the Pinner interface
type pinner struct {
	lock sync.RWMutex

	dserv  ipld.DAGService
	dstore ds.Datastore

	cidDIndex dsindex.Indexer
	cidRIndex dsindex.Indexer
	nameIndex dsindex.Indexer

	clean int64
	dirty int64
}

var _ ipfspinner.Pinner = (*pinner)(nil)

type pin struct {
	Id       string
	Cid      cid.Cid
	Metadata map[string]interface{}
	Mode     ipfspinner.Mode
	Name     string
}

func (p *pin) dsKey() ds.Key {
	return ds.NewKey(path.Join(pinKeyPath, p.Id))
}

func newPin(c cid.Cid, mode ipfspinner.Mode, name string) *pin {
	return &pin{
		Id:   ds.RandomKey().String(),
		Cid:  c,
		Name: name,
		Mode: mode,
	}
}

type syncDAGService interface {
	ipld.DAGService
	Sync() error
}

// New creates a new pinner and loads its keysets from the given datastore. If
// there is no data present in the datastore, then an empty pinner is returned.
func New(ctx context.Context, dstore ds.Datastore, dserv ipld.DAGService) (ipfspinner.Pinner, error) {
	p := &pinner{
		cidDIndex: dsindex.New(dstore, ds.NewKey(pinCidDIndexPath)),
		cidRIndex: dsindex.New(dstore, ds.NewKey(pinCidRIndexPath)),
		nameIndex: dsindex.New(dstore, ds.NewKey(pinNameIndexPath)),
		dserv:     dserv,
		dstore:    dstore,
	}

	data, err := dstore.Get(dirtyKey)
	if err != nil {
		if err == ds.ErrNotFound {
			return p, nil
		}
		return nil, fmt.Errorf("cannot load dirty flag: %v", err)
	}
	if data[0] == 1 {
		p.dirty = 1

		pins, err := p.loadAllPins(ctx)
		if err != nil {
			return nil, fmt.Errorf("cannot load pins: %v", err)
		}

		err = p.rebuildIndexes(ctx, pins)
		if err != nil {
			return nil, fmt.Errorf("cannot rebuild indexes: %v", err)
		}
	}

	return p, nil
}

// Pin the given node, optionally recursive
func (p *pinner) Pin(ctx context.Context, node ipld.Node, recurse bool) error {
	err := p.dserv.Add(ctx, node)
	if err != nil {
		return err
	}

	c := node.Cid()
	cidKey := c.KeyString()

	p.lock.Lock()
	defer p.lock.Unlock()

	if recurse {
		found, err := p.cidRIndex.HasAny(ctx, cidKey)
		if err != nil {
			return err
		}
		if found {
			return nil
		}

		dirtyBefore := p.dirty

		// temporary unlock to fetch the entire graph
		p.lock.Unlock()
		// Fetch graph starting at node identified by cid
		err = mdag.FetchGraph(ctx, c, p.dserv)
		p.lock.Lock()
		if err != nil {
			return err
		}

		// Only look again if something has changed.
		if p.dirty != dirtyBefore {
			found, err = p.cidRIndex.HasAny(ctx, cidKey)
			if err != nil {
				return err
			}
			if found {
				return nil
			}
		}

		// TODO: remove this to support multiple pins per CID
		found, err = p.cidDIndex.HasAny(ctx, cidKey)
		if err != nil {
			return err
		}
		if found {
			p.removePinsForCid(ctx, c, ipfspinner.Direct)
		}

		_, err = p.addPin(ctx, c, ipfspinner.Recursive, "")
		if err != nil {
			return err
		}
	} else {
		found, err := p.cidRIndex.HasAny(ctx, cidKey)
		if err != nil {
			return err
		}
		if found {
			return fmt.Errorf("%s already pinned recursively", c.String())
		}

		_, err = p.addPin(ctx, c, ipfspinner.Direct, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *pinner) addPin(ctx context.Context, c cid.Cid, mode ipfspinner.Mode, name string) (string, error) {
	// Create new pin and store in datastore
	pp := newPin(c, mode, name)

	// Serialize pin
	pinData, err := encodePin(pp)
	if err != nil {
		return "", fmt.Errorf("could not encode pin: %v", err)
	}

	p.setDirty(ctx, true)

	// Store CID index
	switch mode {
	case ipfspinner.Recursive:
		err = p.cidRIndex.Add(ctx, c.KeyString(), pp.Id)
	case ipfspinner.Direct:
		err = p.cidDIndex.Add(ctx, c.KeyString(), pp.Id)
	default:
		panic("pin mode must be recursive or direct")
	}
	if err != nil {
		return "", fmt.Errorf("could not add pin cid index: %v", err)
	}

	if name != "" {
		// Store name index
		err = p.nameIndex.Add(ctx, name, pp.Id)
		if err != nil {
			return "", fmt.Errorf("could not add pin name index: %v", err)
		}
	}

	// Store the pin.  Pin must be stored after index for recovery to work.
	err = p.dstore.Put(pp.dsKey(), pinData)
	if err != nil {
		if mode == ipfspinner.Recursive {
			p.cidRIndex.Delete(ctx, c.KeyString(), pp.Id)
		} else {
			p.cidDIndex.Delete(ctx, c.KeyString(), pp.Id)
		}
		if name != "" {
			p.nameIndex.Delete(ctx, name, pp.Id)
		}
		return "", err
	}

	return pp.Id, nil
}

func (p *pinner) removePin(ctx context.Context, pp *pin) error {
	p.setDirty(ctx, true)

	// Remove pin from datastore.  Pin must be removed before index for
	// recovery to work.
	err := p.dstore.Delete(pp.dsKey())
	if err != nil {
		return err
	}
	// Remove cid index from datastore
	if pp.Mode == ipfspinner.Recursive {
		err = p.cidRIndex.Delete(ctx, pp.Cid.KeyString(), pp.Id)
	} else {
		err = p.cidDIndex.Delete(ctx, pp.Cid.KeyString(), pp.Id)
	}
	if err != nil {
		return err
	}

	if pp.Name != "" {
		// Remove name index from datastore
		err = p.nameIndex.Delete(ctx, pp.Name, pp.Id)
		if err != nil {
			return err
		}
	}

	return nil
}

// Unpin a given key
func (p *pinner) Unpin(ctx context.Context, c cid.Cid, recursive bool) error {
	cidKey := c.KeyString()

	p.lock.Lock()
	defer p.lock.Unlock()

	// TODO: use Ls() to lookup pins when new pinning API available
	/*
		matchSpec := map[string][]string {
			"cid": []string{c.String}
		}
		matches := p.Ls(matchSpec)
	*/
	has, err := p.cidRIndex.HasAny(ctx, cidKey)
	if err != nil {
		return err
	}

	if has {
		if !recursive {
			return fmt.Errorf("%s is pinned recursively", c.String())
		}
	} else {
		has, err = p.cidDIndex.HasAny(ctx, cidKey)
		if err != nil {
			return err
		}
		if !has {
			return ErrNotPinned
		}
	}

	_, err = p.removePinsForCid(ctx, c, ipfspinner.Any)
	if err != nil {
		return err
	}

	return nil
}

// IsPinned returns whether or not the given key is pinned
// and an explanation of why its pinned
func (p *pinner) IsPinned(ctx context.Context, c cid.Cid) (string, bool, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.isPinnedWithType(ctx, c, ipfspinner.Any)
}

// IsPinnedWithType returns whether or not the given cid is pinned with the
// given pin type, as well as returning the type of pin its pinned with.
func (p *pinner) IsPinnedWithType(ctx context.Context, c cid.Cid, mode ipfspinner.Mode) (string, bool, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.isPinnedWithType(ctx, c, mode)
}

func (p *pinner) isPinnedWithType(ctx context.Context, c cid.Cid, mode ipfspinner.Mode) (string, bool, error) {
	cidKey := c.KeyString()
	switch mode {
	case ipfspinner.Recursive:
		has, err := p.cidRIndex.HasAny(ctx, cidKey)
		if err != nil {
			return "", false, err
		}
		if has {
			return linkRecursive, true, nil
		}
		return "", false, nil
	case ipfspinner.Direct:
		has, err := p.cidDIndex.HasAny(ctx, cidKey)
		if err != nil {
			return "", false, err
		}
		if has {
			return linkDirect, true, nil
		}
		return "", false, nil
	case ipfspinner.Internal:
		return "", false, nil
	case ipfspinner.Indirect:
	case ipfspinner.Any:
		has, err := p.cidRIndex.HasAny(ctx, cidKey)
		if err != nil {
			return "", false, err
		}
		if has {
			return linkRecursive, true, nil
		}
		has, err = p.cidDIndex.HasAny(ctx, cidKey)
		if err != nil {
			return "", false, err
		}
		if has {
			return linkDirect, true, nil
		}
	default:
		err := fmt.Errorf(
			"invalid Pin Mode '%d', must be one of {%d, %d, %d, %d, %d}",
			mode, ipfspinner.Direct, ipfspinner.Indirect, ipfspinner.Recursive,
			ipfspinner.Internal, ipfspinner.Any)
		return "", false, err
	}

	// Default is Indirect
	visitedSet := cid.NewSet()

	// No index for given CID, so search children of all recursive pinned CIDs
	var has bool
	var rc cid.Cid
	var e error
	err := p.cidRIndex.ForEach(ctx, "", func(key, value string) bool {
		rc, e = cid.Cast([]byte(key))
		if e != nil {
			return false
		}
		has, e = hasChild(ctx, p.dserv, rc, c, visitedSet.Visit)
		if e != nil {
			return false
		}
		if has {
			return false
		}
		return true
	})
	if err != nil {
		return "", false, err
	}
	if e != nil {
		return "", false, e
	}

	if has {
		return rc.String(), true, nil
	}

	return "", false, nil
}

// CheckIfPinned checks if a set of keys are pinned, more efficient than
// calling IsPinned for each key, returns the pinned status of cid(s)
//
// TODO: If a CID is pinned by multiple pins, should they all be reported?
func (p *pinner) CheckIfPinned(ctx context.Context, cids ...cid.Cid) ([]ipfspinner.Pinned, error) {
	pinned := make([]ipfspinner.Pinned, 0, len(cids))
	toCheck := cid.NewSet()

	p.lock.RLock()
	defer p.lock.RUnlock()

	// First check for non-Indirect pins directly
	for _, c := range cids {
		cidKey := c.KeyString()
		has, err := p.cidRIndex.HasAny(ctx, cidKey)
		if err != nil {
			return nil, err
		}
		if has {
			pinned = append(pinned, ipfspinner.Pinned{Key: c, Mode: ipfspinner.Recursive})
		} else {
			has, err = p.cidDIndex.HasAny(ctx, cidKey)
			if err != nil {
				return nil, err
			}
			if has {
				pinned = append(pinned, ipfspinner.Pinned{Key: c, Mode: ipfspinner.Direct})
			} else {
				toCheck.Add(c)
			}
		}
	}

	// Now walk all recursive pins to check for indirect pins
	var checkChildren func(cid.Cid, cid.Cid) error
	checkChildren = func(rk, parentKey cid.Cid) error {
		links, err := ipld.GetLinks(ctx, p.dserv, parentKey)
		if err != nil {
			return err
		}
		for _, lnk := range links {
			c := lnk.Cid

			if toCheck.Has(c) {
				pinned = append(pinned,
					ipfspinner.Pinned{Key: c, Mode: ipfspinner.Indirect, Via: rk})
				toCheck.Remove(c)
			}

			err = checkChildren(rk, c)
			if err != nil {
				return err
			}

			if toCheck.Len() == 0 {
				return nil
			}
		}
		return nil
	}

	var e error
	err := p.cidRIndex.ForEach(ctx, "", func(key, value string) bool {
		var rk cid.Cid
		rk, e = cid.Cast([]byte(key))
		if e != nil {
			return false
		}
		e = checkChildren(rk, rk)
		if e != nil {
			return false
		}
		if toCheck.Len() == 0 {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if e != nil {
		return nil, e
	}

	// Anything left in toCheck is not pinned
	for _, k := range toCheck.Keys() {
		pinned = append(pinned, ipfspinner.Pinned{Key: k, Mode: ipfspinner.NotPinned})
	}

	return pinned, nil
}

// RemovePinWithMode is for manually editing the pin structure.
// Use with care! If used improperly, garbage collection may not
// be successful.
func (p *pinner) RemovePinWithMode(c cid.Cid, mode ipfspinner.Mode) {
	ctx := context.TODO()
	// Check cache to see if CID is pinned
	switch mode {
	case ipfspinner.Direct, ipfspinner.Recursive:
	default:
		// programmer error, panic OK
		panic("unrecognized pin type")
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.removePinsForCid(ctx, c, mode)
}

// removePinsForCid removes all pins for a cid that has the specified mode.
// Returns true if any pins, and all corresponding CID index entries, were
// removed.  Otherwise, returns false.
func (p *pinner) removePinsForCid(ctx context.Context, c cid.Cid, mode ipfspinner.Mode) (bool, error) {
	// Search for pins by CID
	var ids []string
	var err error
	cidKey := c.KeyString()
	switch mode {
	case ipfspinner.Recursive:
		ids, err = p.cidRIndex.Search(ctx, cidKey)
	case ipfspinner.Direct:
		ids, err = p.cidDIndex.Search(ctx, cidKey)
	case ipfspinner.Any:
		ids, err = p.cidRIndex.Search(ctx, cidKey)
		if err != nil {
			return false, err
		}
		dIds, err := p.cidDIndex.Search(ctx, cidKey)
		if err != nil {
			return false, err
		}
		if len(dIds) != 0 {
			ids = append(ids, dIds...)
		}
	}
	if err != nil {
		return false, err
	}

	var removed bool

	// Remove the pin with the requested mode
	for _, pid := range ids {
		var pp *pin
		pp, err = p.loadPin(ctx, pid)
		if err != nil {
			if err == ds.ErrNotFound {
				p.setDirty(ctx, true)
				// Fix index; remove index for pin that does not exist
				switch mode {
				case ipfspinner.Recursive:
					p.cidRIndex.DeleteKey(ctx, cidKey)
				case ipfspinner.Direct:
					p.cidDIndex.DeleteKey(ctx, cidKey)
				case ipfspinner.Any:
					p.cidRIndex.DeleteKey(ctx, cidKey)
					p.cidDIndex.DeleteKey(ctx, cidKey)
				}
				log.Error("found CID index with missing pin")
				continue
			}
			return false, err
		}
		if mode == ipfspinner.Any || pp.Mode == mode {
			err = p.removePin(ctx, pp)
			if err != nil {
				return false, err
			}
			removed = true
		}
	}
	return removed, nil
}

// loadPin loads a single pin from the datastore.
func (p *pinner) loadPin(ctx context.Context, pid string) (*pin, error) {
	pinData, err := p.dstore.Get(ds.NewKey(path.Join(pinKeyPath, pid)))
	if err != nil {
		return nil, err
	}
	return decodePin(pid, pinData)
}

// loadAllPins loads all pins from the datastore.
func (p *pinner) loadAllPins(ctx context.Context) ([]*pin, error) {
	q := query.Query{
		Prefix: pinKeyPath,
	}
	results, err := p.dstore.Query(q)
	if err != nil {
		return nil, err
	}
	ents, err := results.Rest()
	if err != nil {
		return nil, err
	}
	if len(ents) == 0 {
		return nil, nil
	}

	pins := make([]*pin, len(ents))
	for i := range ents {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var p *pin
		p, err = decodePin(path.Base(ents[i].Key), ents[i].Value)
		if err != nil {
			return nil, err
		}
		pins[i] = p
	}
	return pins, nil
}

// rebuildIndexes uses the stored pins to rebuild secondary indexes.  This
// resolves any discrepancy between secondary indexes and pins that could
// result from a program termination between saving the two.
func (p *pinner) rebuildIndexes(ctx context.Context, pins []*pin) error {
	// Build temporary in-memory CID index from pins
	dstoreMem := ds.NewMapDatastore()
	tmpCidDIndex := dsindex.New(dstoreMem, ds.NewKey(pinCidDIndexPath))
	tmpCidRIndex := dsindex.New(dstoreMem, ds.NewKey(pinCidRIndexPath))
	tmpNameIndex := dsindex.New(dstoreMem, ds.NewKey(pinNameIndexPath))
	var hasNames bool
	for _, pp := range pins {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if pp.Mode == ipfspinner.Recursive {
			tmpCidRIndex.Add(ctx, pp.Cid.KeyString(), pp.Id)
		} else if pp.Mode == ipfspinner.Direct {
			tmpCidDIndex.Add(ctx, pp.Cid.KeyString(), pp.Id)
		}
		if pp.Name != "" {
			tmpNameIndex.Add(ctx, pp.Name, pp.Id)
			hasNames = true
		}
	}

	// Sync the CID index to what was build from pins.  This fixes any invalid
	// indexes, which could happen if ipfs was terminated between writing pin
	// and writing secondary index.
	changed, err := dsindex.SyncIndex(ctx, tmpCidRIndex, p.cidRIndex)
	if err != nil {
		return fmt.Errorf("cannot sync indexes: %v", err)
	}
	if changed {
		log.Info("invalid recursive indexes detected - rebuilt")
	}

	changed, err = dsindex.SyncIndex(ctx, tmpCidDIndex, p.cidDIndex)
	if err != nil {
		return fmt.Errorf("cannot sync indexes: %v", err)
	}
	if changed {
		log.Info("invalid direct indexes detected - rebuilt")
	}

	if hasNames {
		changed, err = dsindex.SyncIndex(ctx, tmpNameIndex, p.nameIndex)
		if err != nil {
			return fmt.Errorf("cannot sync name indexes: %v", err)
		}
		if changed {
			log.Info("invalid name indexes detected - rebuilt")
		}
	}

	return p.Flush(ctx)
}

// DirectKeys returns a slice containing the directly pinned keys
func (p *pinner) DirectKeys(ctx context.Context) ([]cid.Cid, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	cidSet := cid.NewSet()
	var e error
	err := p.cidDIndex.ForEach(ctx, "", func(key, value string) bool {
		var c cid.Cid
		c, e = cid.Cast([]byte(key))
		if e != nil {
			return false
		}
		cidSet.Add(c)
		return true
	})
	if err != nil {
		return nil, err
	}
	if e != nil {
		return nil, e
	}

	return cidSet.Keys(), nil
}

// RecursiveKeys returns a slice containing the recursively pinned keys
func (p *pinner) RecursiveKeys(ctx context.Context) ([]cid.Cid, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	cidSet := cid.NewSet()
	var e error
	err := p.cidRIndex.ForEach(ctx, "", func(key, value string) bool {
		var c cid.Cid
		c, e = cid.Cast([]byte(key))
		if e != nil {
			return false
		}
		cidSet.Add(c)
		return true
	})
	if err != nil {
		return nil, err
	}
	if e != nil {
		return nil, e
	}

	return cidSet.Keys(), nil
}

// InternalPins returns all cids kept pinned for the internal state of the
// pinner
func (p *pinner) InternalPins(ctx context.Context) ([]cid.Cid, error) {
	return nil, nil
}

// Update updates a recursive pin from one cid to another.  This is equivalent
// to pinning the new one and unpinning the old one.
//
// TODO: This will not work when multiple pins are supported
func (p *pinner) Update(ctx context.Context, from, to cid.Cid, unpin bool) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	found, err := p.cidRIndex.HasAny(ctx, from.KeyString())
	if err != nil {
		return err
	}
	if !found {
		return errors.New("'from' cid was not recursively pinned already")
	}

	// If `from` already recursively pinned and `to` is the same, then all done
	if from == to {
		return nil
	}

	// Check if the `to` cid is already recursively pinned
	found, err = p.cidRIndex.HasAny(ctx, to.KeyString())
	if err != nil {
		return err
	}
	if found {
		return errors.New("'to' cid was already recursively pinned")
	}

	// Temporarily unlock while we fetch the differences.
	p.lock.Unlock()
	err = dagutils.DiffEnumerate(ctx, p.dserv, from, to)
	p.lock.Lock()

	if err != nil {
		return err
	}

	_, err = p.addPin(ctx, to, ipfspinner.Recursive, "")
	if err != nil {
		return err
	}

	if !unpin {
		return nil
	}

	_, err = p.removePinsForCid(ctx, from, ipfspinner.Recursive)
	if err != nil {
		return err
	}

	return nil
}

// Flush encodes and writes pinner keysets to the datastore
func (p *pinner) Flush(ctx context.Context) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if syncDServ, ok := p.dserv.(syncDAGService); ok {
		if err := syncDServ.Sync(); err != nil {
			return fmt.Errorf("cannot sync pinned data: %v", err)
		}
	}

	// Sync pins and indexes
	if err := p.dstore.Sync(ds.NewKey(basePath)); err != nil {
		return fmt.Errorf("cannot sync pin state: %v", err)
	}

	p.setDirty(ctx, false)

	return nil
}

// PinWithMode allows the user to have fine grained control over pin
// counts
func (p *pinner) PinWithMode(c cid.Cid, mode ipfspinner.Mode) {
	ctx := context.TODO()

	p.lock.Lock()
	defer p.lock.Unlock()

	// TODO: remove his to support multiple pins per CID
	switch mode {
	case ipfspinner.Recursive:
		if has, _ := p.cidRIndex.HasAny(ctx, c.KeyString()); has {
			return // already a recursive pin for this CID
		}
	case ipfspinner.Direct:
		if has, _ := p.cidDIndex.HasAny(ctx, c.KeyString()); has {
			return // already a direct pin for this CID
		}
	default:
		panic("unrecognized pin mode")
	}

	_, err := p.addPin(ctx, c, mode, "")
	if err != nil {
		return
	}
}

// hasChild recursively looks for a Cid among the children of a root Cid.
// The visit function can be used to shortcut already-visited branches.
func hasChild(ctx context.Context, ng ipld.NodeGetter, root cid.Cid, child cid.Cid, visit func(cid.Cid) bool) (bool, error) {
	links, err := ipld.GetLinks(ctx, ng, root)
	if err != nil {
		return false, err
	}
	for _, lnk := range links {
		c := lnk.Cid
		if lnk.Cid.Equals(child) {
			return true, nil
		}
		if visit(c) {
			has, err := hasChild(ctx, ng, c, child, visit)
			if err != nil {
				return false, err
			}

			if has {
				return has, nil
			}
		}
	}
	return false, nil
}

func encodePin(p *pin) ([]byte, error) {
	b, err := cbor.MarshalAtlased(p, pinAtl)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func decodePin(pid string, data []byte) (*pin, error) {
	p := &pin{Id: pid}
	err := cbor.UnmarshalAtlased(cbor.DecodeOptions{}, data, p, pinAtl)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// setDirty saves a boolean dirty flag in the datastore whenever there is a
// transition between a dirty (counter > 0) and non-dirty (counter == 0) state.
func (p *pinner) setDirty(ctx context.Context, dirty bool) {
	isClean := p.dirty == p.clean
	if dirty {
		p.dirty++
		if !isClean {
			return // do not save; was already dirty
		}
	} else if isClean {
		return // already clean
	} else {
		p.clean = p.dirty // set clean
	}

	// Do edge-triggered write to datastore
	data := []byte{0}
	if dirty {
		data[0] = 1
	}
	p.dstore.Put(dirtyKey, data)
	p.dstore.Sync(dirtyKey)
}
