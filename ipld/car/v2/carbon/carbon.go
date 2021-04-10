package carbon

import (
	"errors"
	"os"

	"github.com/ipfs/go-cid"
	bs "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car"
	"github.com/willscott/carbs"
)

// Carbon is a carbs-index-compatible blockstore supporting appending additional blocks
type Carbon interface {
	bs.Blockstore
	Checkpoint() error
	Finish() error
}

// errUnsupported is returned for unsupported blockstore operations (like delete)
var errUnsupported = errors.New("unsupported by carbon")

// errNotFound is returned for lookups to entries that don't exist
var errNotFound = errors.New("not found")

// New creates a new Carbon blockstore
func New(path string) (Carbon, error) {
	return NewWithRoots(path, []cid.Cid{})
}

// NewWithRoots creates a new Carbon blockstore with a provided set of root cids as the car roots
func NewWithRoots(path string, roots []cid.Cid) (Carbon, error) {
	wfd, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	rfd, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	hdr := car.CarHeader{
		Roots:   roots,
		Version: 1,
	}
	if err := car.WriteHeader(&hdr, wfd); err != nil {
		return nil, err
	}

	idx := insertionIndex{}
	f := carbonFD{
		path,
		&poswriter{wfd, 0},
		*carbs.Of(rfd, &idx),
		&idx,
	}
	return &f, nil
}
