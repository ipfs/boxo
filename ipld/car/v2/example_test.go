package car_test

import (
	"bytes"
	"fmt"
	"io/ioutil"

	carv2 "github.com/ipld/go-car/v2"
)

func ExampleWrapV1File() {
	// We have a sample CARv1 file.
	// Wrap it as-is in a CARv2, with an index.
	// Writing the result to testdata allows reusing that file in other tests,
	// and also helps ensure that the result is deterministic.
	src := "testdata/sample-v1.car"
	dst := "testdata/sample-wrapped-v2.car"
	if err := carv2.WrapV1File(src, dst); err != nil {
		panic(err)
	}

	// Open our new CARv2 file and show some info about it.
	cr, err := carv2.NewReaderMmap(dst)
	if err != nil {
		panic(err)
	}
	defer cr.Close()
	roots, err := cr.Roots()
	if err != nil {
		panic(err)
	}
	fmt.Println("Roots:", roots)
	fmt.Println("Has index:", cr.Header.HasIndex())

	// Verify that the CARv1 remains exactly the same.
	orig, err := ioutil.ReadFile(src)
	if err != nil {
		panic(err)
	}
	inner, err := ioutil.ReadAll(cr.CarV1Reader())
	if err != nil {
		panic(err)
	}
	fmt.Println("Inner CARv1 is exactly the same:", bytes.Equal(orig, inner))

	// Output:
	// Roots: [bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy]
	// Has index: true
	// Inner CARv1 is exactly the same: true
}
