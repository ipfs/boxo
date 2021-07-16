package index_test

import (
	"fmt"
	"os"
	"reflect"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
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
		offset, err := idx.Get(r)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Frame with CID %v starts at offset %v relative to CARv1 data payload.\n", r, offset)
	}

	// Output:
	// Frame with CID bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy starts at offset 61 relative to CARv1 data payload.
}

// ExampleSave unmarshalls an index from an indexed CARv2 file, and stores it as a separate
// file on disk.
func ExampleSave() {
	// Open the CARv2 file
	src := "../testdata/sample-wrapped-v2.car"
	cr, err := carv2.OpenReader(src)
	if err != nil {
		panic(err)
	}
	defer cr.Close()

	// Read and unmarshall index within CARv2 file.
	idx, err := index.ReadFrom(cr.IndexReader())
	if err != nil {
		panic(err)
	}

	// Store the index alone onto destination file.
	dest := "../testdata/sample-index.carindex"
	err = index.Save(idx, dest)
	if err != nil {
		panic(err)
	}

	// Open the destination file that contains the index only.
	f, err := os.Open(dest)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Read and unmarshall the destination file as a separate index instance.
	reReadIdx, err := index.ReadFrom(f)
	if err != nil {
		panic(err)
	}

	// Expect indices to be equal.
	if reflect.DeepEqual(idx, reReadIdx) {
		fmt.Printf("Saved index file at %v matches the index embedded in CARv2 at %v.\n", dest, src)
	} else {
		panic("expected to get the same index as the CARv2 file")
	}

	// Output:
	// Saved index file at ../testdata/sample-index.carindex matches the index embedded in CARv2 at ../testdata/sample-wrapped-v2.car.
}
