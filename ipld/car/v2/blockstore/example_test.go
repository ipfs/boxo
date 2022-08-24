package blockstore_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
)

const cidPrintCount = 5

// ExampleOpenReadOnly opens a read-only blockstore from a CARv1 file, and prints its root CIDs
// along with CID mapping to raw data size of blocks for first five sections in the CAR file.
func ExampleOpenReadOnly() {
	// Open a new ReadOnly blockstore from a CARv1 file.
	// Note, `OpenReadOnly` accepts bot CARv1 and CARv2 formats and transparently generate index
	// in the background if necessary.
	// This instance sets ZeroLengthSectionAsEOF option to treat zero sized sections in file as EOF.
	robs, err := blockstore.OpenReadOnly("../testdata/sample-v1.car",
		blockstore.UseWholeCIDs(true),
		carv2.ZeroLengthSectionAsEOF(true),
	)
	if err != nil {
		panic(err)
	}
	defer robs.Close()

	// Print root CIDs.
	roots, err := robs.Roots()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Contains %v root CID(s):\n", len(roots))
	for _, r := range roots {
		fmt.Printf("\t%v\n", r)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Print the raw data size for the first 5 CIDs in the CAR file.
	keysChan, err := robs.AllKeysChan(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("List of first %v CIDs and their raw data size:\n", cidPrintCount)
	i := 1
	for k := range keysChan {
		if i > cidPrintCount {
			cancel()
			break
		}
		size, err := robs.GetSize(context.TODO(), k)
		if err != nil {
			panic(err)
		}
		fmt.Printf("\t%v -> %v bytes\n", k, size)
		i++
	}

	// Output:
	// Contains 1 root CID(s):
	// 	bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy
	// List of first 5 CIDs and their raw data size:
	// 	bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy -> 821 bytes
	// 	bafy2bzaceaycv7jhaegckatnncu5yugzkrnzeqsppzegufr35lroxxnsnpspu -> 1053 bytes
	// 	bafy2bzaceb62wdepofqu34afqhbcn4a7jziwblt2ih5hhqqm6zitd3qpzhdp4 -> 1094 bytes
	// 	bafy2bzaceb3utcspm5jqcdqpih3ztbaztv7yunzkiyfq7up7xmokpxemwgu5u -> 1051 bytes
	// 	bafy2bzacedjwekyjresrwjqj4n2r5bnuuu3klncgjo2r3slsp6wgqb37sz4ck -> 821 bytes
}

// ExampleOpenReadWrite creates a read-write blockstore and puts
func ExampleOpenReadWrite() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	thisBlock := merkledag.NewRawNode([]byte("fish")).Block
	thatBlock := merkledag.NewRawNode([]byte("lobster")).Block
	andTheOtherBlock := merkledag.NewRawNode([]byte("barreleye")).Block

	tdir, err := os.MkdirTemp(os.TempDir(), "example-*")
	if err != nil {
		panic(err)
	}
	dst := filepath.Join(tdir, "sample-rw-bs-v2.car")
	roots := []cid.Cid{thisBlock.Cid(), thatBlock.Cid(), andTheOtherBlock.Cid()}

	rwbs, err := blockstore.OpenReadWrite(dst, roots, carv2.UseDataPadding(1413), carv2.UseIndexPadding(42))
	if err != nil {
		panic(err)
	}

	// Put all blocks onto the blockstore.
	blocks := []blocks.Block{thisBlock, thatBlock}
	if err := rwbs.PutMany(ctx, blocks); err != nil {
		panic(err)
	}
	fmt.Printf("Successfully wrote %v blocks into the blockstore.\n", len(blocks))

	// Any blocks put can be read back using the same blockstore instance.
	block, err := rwbs.Get(ctx, thatBlock.Cid())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read back block just put with raw value of `%v`.\n", string(block.RawData()))

	// Finalize the blockstore to flush out the index and make a complete CARv2.
	if err := rwbs.Finalize(); err != nil {
		panic(err)
	}

	// Resume from the same file to add more blocks.
	// Note the UseDataPadding and roots must match the values passed to the blockstore instance
	// that created the original file. Otherwise, we cannot resume from the same file.
	resumedRwbos, err := blockstore.OpenReadWrite(dst, roots, carv2.UseDataPadding(1413))
	if err != nil {
		panic(err)
	}

	// Put another block, appending it to the set of blocks that are written previously.
	if err := resumedRwbos.Put(ctx, andTheOtherBlock); err != nil {
		panic(err)
	}

	// Read back the the block put before resumption.
	// Blocks previously put are present.
	block, err = resumedRwbos.Get(ctx, thatBlock.Cid())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Resumed blockstore contains blocks put previously with raw value of `%v`.\n", string(block.RawData()))

	// Put an additional block to the CAR.
	// Blocks put after resumption are also present.
	block, err = resumedRwbos.Get(ctx, andTheOtherBlock.Cid())
	if err != nil {
		panic(err)
	}
	fmt.Printf("It also contains the block put after resumption with raw value of `%v`.\n", string(block.RawData()))

	// Finalize the blockstore to flush out the index and make a complete CARv2.
	// Note, Finalize must be called on an open ReadWrite blockstore to flush out a complete CARv2.
	if err := resumedRwbos.Finalize(); err != nil {
		panic(err)
	}

	// Output:
	// Successfully wrote 2 blocks into the blockstore.
	// Read back block just put with raw value of `lobster`.
	// Resumed blockstore contains blocks put previously with raw value of `lobster`.
	// It also contains the block put after resumption with raw value of `barreleye`.
}
