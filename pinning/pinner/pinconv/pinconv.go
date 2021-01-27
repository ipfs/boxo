// Package pinconv converts pins between the dag-based ipldpinner and the
// datastore-based dspinner.  Once conversion is complete, the pins from the
// source pinner are removed.
package pinconv

import (
	"context"
	"fmt"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ipfspinner "github.com/ipfs/go-ipfs-pinner"
	"github.com/ipfs/go-ipfs-pinner/dspinner"
	"github.com/ipfs/go-ipfs-pinner/ipldpinner"
	ipld "github.com/ipfs/go-ipld-format"
)

// ConvertPinsFromIPLDToDS converts pins stored in mdag based storage to pins
// stores in the datastore.  Returns a dspinner loaded with the converted pins,
// and a count of the recursive and direct pins converted.
//
// After pins are stored in datastore, the root pin key is deleted to unlink
// the pin data in the DAGService.
func ConvertPinsFromIPLDToDS(ctx context.Context, dstore ds.Datastore, dserv ipld.DAGService, internal ipld.DAGService) (ipfspinner.Pinner, int, error) {
	const ipldPinPath = "/local/pins"

	dsPinner, err := dspinner.New(ctx, dstore, dserv)
	if err != nil {
		return nil, 0, err
	}

	var convCount int
	keyChan := make(chan cid.Cid)

	go func() {
		err = ipldpinner.LoadKeys(ctx, dstore, dserv, internal, true, keyChan)
		close(keyChan)
	}()
	for key := range keyChan {
		dsPinner.PinWithMode(key, ipfspinner.Recursive)
		convCount++
	}
	if err != nil {
		return nil, 0, fmt.Errorf("cannot load recursive keys: %s", err)
	}

	keyChan = make(chan cid.Cid)
	go func() {
		err = ipldpinner.LoadKeys(ctx, dstore, dserv, internal, false, keyChan)
		close(keyChan)
	}()
	for key := range keyChan {
		dsPinner.PinWithMode(key, ipfspinner.Direct)
		convCount++
	}
	if err != nil {
		return nil, 0, fmt.Errorf("cannot load direct keys: %s", err)
	}

	err = dsPinner.Flush(ctx)
	if err != nil {
		return nil, 0, err
	}

	// Delete root mdag key from datastore to remove old pin storage.
	ipldPinDatastoreKey := ds.NewKey(ipldPinPath)
	if err = dstore.Delete(ipldPinDatastoreKey); err != nil {
		return nil, 0, fmt.Errorf("cannot delete old pin state: %v", err)
	}
	if err = dstore.Sync(ipldPinDatastoreKey); err != nil {
		return nil, 0, fmt.Errorf("cannot sync old pin state: %v", err)
	}

	return dsPinner, convCount, nil
}

// ConvertPinsFromDSToIPLD converts the pins stored in the datastore by
// dspinner, into pins stored in the given internal DAGService by ipldpinner.
// Returns an ipldpinner loaded with the converted pins, and a count of the
// recursive and direct pins converted.
//
// After the pins are stored in the DAGService, the pins and their indexes are
// removed from the dspinner.
func ConvertPinsFromDSToIPLD(ctx context.Context, dstore ds.Datastore, dserv ipld.DAGService, internal ipld.DAGService) (ipfspinner.Pinner, int, error) {
	dsPinner, err := dspinner.New(ctx, dstore, dserv)
	if err != nil {
		return nil, 0, err
	}

	ipldPinner, err := ipldpinner.New(dstore, dserv, internal)
	if err != nil {
		return nil, 0, err
	}

	cids, err := dsPinner.RecursiveKeys(ctx)
	if err != nil {
		return nil, 0, err
	}
	for i := range cids {
		ipldPinner.PinWithMode(cids[i], ipfspinner.Recursive)
		dsPinner.RemovePinWithMode(cids[i], ipfspinner.Recursive)
	}
	convCount := len(cids)

	cids, err = dsPinner.DirectKeys(ctx)
	if err != nil {
		return nil, 0, err
	}
	for i := range cids {
		ipldPinner.PinWithMode(cids[i], ipfspinner.Direct)
		dsPinner.RemovePinWithMode(cids[i], ipfspinner.Direct)
	}
	convCount += len(cids)

	// Save the ipldpinner pins
	err = ipldPinner.Flush(ctx)
	if err != nil {
		return nil, 0, err
	}

	err = dsPinner.Flush(ctx)
	if err != nil {
		return nil, 0, err
	}

	return ipldPinner, convCount, nil
}
