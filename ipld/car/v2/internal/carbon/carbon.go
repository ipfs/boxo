package carbon

import (
	"errors"
	"fmt"
	"os"

	carblockstore "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carv1 "github.com/ipld/go-car"
)

// Carbon is a carbs-index-compatible blockstore supporting appending additional blocks
type Carbon interface {
	blockstore.Blockstore
	Checkpoint() error
	Finish() error
}

// errUnsupported is returned for unsupported blockstore operations (like delete)
var errUnsupported = errors.New("unsupported by carbon")

// New creates a new Carbon blockstore
func New(path string) (Carbon, error) {
	return NewWithRoots(path, []cid.Cid{})
}

// NewWithRoots creates a new Carbon blockstore with a provided set of root cids as the car roots
func NewWithRoots(path string, roots []cid.Cid) (Carbon, error) {
	wfd, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o666)
	if err != nil {
		return nil, fmt.Errorf("couldn't create backing car: %w", err)
	}
	rfd, err := os.OpenFile(path, os.O_RDONLY, 0o666)
	if err != nil {
		return nil, fmt.Errorf("could not re-open read handle: %w", err)
	}

	hdr := carv1.CarHeader{
		Roots:   roots,
		Version: 1,
	}
	writer := poswriter{wfd, 0}
	if err := carv1.WriteHeader(&hdr, &writer); err != nil {
		return nil, fmt.Errorf("couldn't write car header: %w", err)
	}

	indexcls, ok := index.IndexAtlas[index.IndexInsertion]
	if !ok {
		return nil, fmt.Errorf("unknownindex  codec: %#v", index.IndexInsertion)
	}

	idx := (indexcls()).(*index.InsertionIndex)
	f := carbonFD{
		path,
		&writer,
		*carblockstore.ReadOnlyOf(rfd, idx),
		idx,
	}
	return &f, nil
}
