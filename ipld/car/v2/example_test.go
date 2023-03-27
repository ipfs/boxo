package car_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	carv2 "github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/boxo/ipld/car/v2/blockstore"
)

func ExampleWrapV1File() {
	// We have a sample CARv1 file.
	// Wrap it as-is in a CARv2, with an index.
	// Writing the result to testdata allows reusing that file in other tests,
	// and also helps ensure that the result is deterministic.
	src := "testdata/sample-v1.car"
	tdir, err := os.MkdirTemp(os.TempDir(), "example-*")
	if err != nil {
		panic(err)
	}
	dst := filepath.Join(tdir, "wrapped-v2.car")
	if err := carv2.WrapV1File(src, dst); err != nil {
		panic(err)
	}

	// Open our new CARv2 file and show some info about it.
	cr, err := carv2.OpenReader(dst)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := cr.Close(); err != nil {
			panic(err)
		}
	}()

	roots, err := cr.Roots()
	if err != nil {
		panic(err)
	}
	fmt.Println("Roots:", roots)
	fmt.Println("Has index:", cr.Header.HasIndex())

	// Verify that the CARv1 remains exactly the same.
	orig, err := os.ReadFile(src)
	if err != nil {
		panic(err)
	}
	dr, err := cr.DataReader()
	if err != nil {
		panic(err)
	}
	inner, err := io.ReadAll(dr)
	if err != nil {
		panic(err)
	}
	fmt.Println("Inner CARv1 is exactly the same:", bytes.Equal(orig, inner))

	// Verify that the CARv2 works well with its index.
	bs, err := blockstore.OpenReadOnly(dst)
	if err != nil {
		panic(err)
	}
	fmt.Println(bs.Get(context.TODO(), roots[0]))

	// Output:
	// Roots: [bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy]
	// Has index: true
	// Inner CARv1 is exactly the same: true
	// [Block bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy] <nil>
}

// ExampleNewBlockReader instantiates a new BlockReader for a CARv1 file and its wrapped CARv2
// version. For each file, it prints the version, the root CIDs and the first five block CIDs.
// Note, the roots and first five block CIDs are identical in both files since both represent the
// same root CIDs and data blocks.
func ExampleNewBlockReader() {
	for _, path := range []string{
		"testdata/sample-v1.car",
		"testdata/sample-wrapped-v2.car",
	} {
		fmt.Println("File:", path)
		f, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		br, err := carv2.NewBlockReader(f)
		if err != nil {
			panic(err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				panic(err)
			}
		}()
		fmt.Println("Version:", br.Version)
		fmt.Println("Roots:", br.Roots)
		fmt.Println("First 5 block CIDs:")
		for i := 0; i < 5; i++ {
			bl, err := br.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			fmt.Printf("\t%v\n", bl.Cid())
		}
	}
	// Output:
	// File: testdata/sample-v1.car
	// Version: 1
	// Roots: [bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy]
	// First 5 block CIDs:
	// 	bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy
	// 	bafy2bzaceaycv7jhaegckatnncu5yugzkrnzeqsppzegufr35lroxxnsnpspu
	// 	bafy2bzaceb62wdepofqu34afqhbcn4a7jziwblt2ih5hhqqm6zitd3qpzhdp4
	// 	bafy2bzaceb3utcspm5jqcdqpih3ztbaztv7yunzkiyfq7up7xmokpxemwgu5u
	// 	bafy2bzacedjwekyjresrwjqj4n2r5bnuuu3klncgjo2r3slsp6wgqb37sz4ck
	// File: testdata/sample-wrapped-v2.car
	// Version: 2
	// Roots: [bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy]
	// First 5 block CIDs:
	// 	bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy
	// 	bafy2bzaceaycv7jhaegckatnncu5yugzkrnzeqsppzegufr35lroxxnsnpspu
	// 	bafy2bzaceb62wdepofqu34afqhbcn4a7jziwblt2ih5hhqqm6zitd3qpzhdp4
	// 	bafy2bzaceb3utcspm5jqcdqpih3ztbaztv7yunzkiyfq7up7xmokpxemwgu5u
	// 	bafy2bzacedjwekyjresrwjqj4n2r5bnuuu3klncgjo2r3slsp6wgqb37sz4ck
}
