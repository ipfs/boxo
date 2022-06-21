package index_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
)

// ExampleReadFrom unmarshalls an index from an indexed CARv2 file, and for each root CID prints the
// offset at which its corresponding block starts relative to the wrapped CARv1 data payload.
func ExampleReadFrom() {
	// Open the CARv2 file
	cr, err := carv2.OpenReader("../testdata/sample-wrapped-v2.car")
	if err != nil {
		panic(err)
	}
	defer cr.Close()

	// Get root CIDs in the CARv1 file.
	roots, err := cr.Roots()
	if err != nil {
		panic(err)
	}

	// Read and unmarshall index within CARv2 file.
	idx, err := index.ReadFrom(cr.IndexReader())
	if err != nil {
		panic(err)
	}

	// For each root CID print the offset relative to CARv1 data payload.
	for _, r := range roots {
		offset, err := index.GetFirst(idx, r)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Frame with CID %v starts at offset %v relative to CARv1 data payload.\n", r, offset)
	}

	// Output:
	// Frame with CID bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy starts at offset 61 relative to CARv1 data payload.
}

// ExampleWriteTo unmarshalls an index from an indexed CARv2 file, and stores it as a separate
// file on disk.
func ExampleWriteTo() {
	// Open the CARv2 file
	src := "../testdata/sample-wrapped-v2.car"
	cr, err := carv2.OpenReader(src)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := cr.Close(); err != nil {
			panic(err)
		}
	}()

	// Read and unmarshall index within CARv2 file.
	idx, err := index.ReadFrom(cr.IndexReader())
	if err != nil {
		panic(err)
	}

	// Store the index alone onto destination file.
	f, err := ioutil.TempFile(os.TempDir(), "example-index-*.carindex")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	_, err = index.WriteTo(idx, f)
	if err != nil {
		panic(err)
	}

	// Seek to the beginning of tile to read it back.
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}

	// Read and unmarshall the destination file as a separate index instance.
	reReadIdx, err := index.ReadFrom(f)
	if err != nil {
		panic(err)
	}

	// Expect indices to be equal - collect all of the multihashes and their
	// offsets from the first and compare to the second
	mha := make(map[string]uint64, 0)
	_ = idx.ForEach(func(mh multihash.Multihash, off uint64) error {
		mha[mh.HexString()] = off
		return nil
	})
	var count int
	_ = reReadIdx.ForEach(func(mh multihash.Multihash, off uint64) error {
		count++
		if expectedOffset, ok := mha[mh.HexString()]; !ok || expectedOffset != off {
			panic("expected to get the same index as the CARv2 file")
		}
		return nil
	})
	if count != len(mha) {
		panic("expected to get the same index as the CARv2 file")
	}

	fmt.Printf("Saved index file matches the index embedded in CARv2 at %v.\n", src)

	// Output:
	// Saved index file matches the index embedded in CARv2 at ../testdata/sample-wrapped-v2.car.
}
