//go:build go1.18

package car_test

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	car "github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/boxo/ipld/car/v2/internal/carv1"
)

// v1FixtureStr is a clean carv1 single-block, single-root CAR
const v1FixtureStr = "3aa265726f6f747381d82a58250001711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80b6776657273696f6e012c01711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80ba165646f646779f5"

func seedWithCarFiles(f *testing.F) {
	fixture, err := hex.DecodeString(v1FixtureStr)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(fixture)
	files, err := filepath.Glob("testdata/*.car")
	if err != nil {
		f.Fatal(err)
	}
	for _, fname := range files {
		func() {
			file, err := os.Open(fname)
			if err != nil {
				f.Fatal(err)
			}
			defer file.Close()
			data, err := io.ReadAll(file)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(data)
		}()
	}
}

func FuzzBlockReader(f *testing.F) {
	seedWithCarFiles(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := car.NewBlockReader(bytes.NewReader(data))
		if err != nil {
			return
		}

		for {
			_, err = r.Next()
			if err == io.EOF {
				return
			}
		}
	})
}

func FuzzReader(f *testing.F) {
	seedWithCarFiles(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		subject, err := car.NewReader(bytes.NewReader(data))
		if err != nil {
			return
		}

		subject.Roots()
		_, err = subject.IndexReader()
		if err != nil {
			return
		}
		dr, err := subject.DataReader()
		if err != nil {
			return
		}
		car.GenerateIndex(dr)
	})
}

func FuzzInspect(f *testing.F) {
	seedWithCarFiles(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		reader, err := car.NewReader(bytes.NewReader(data))
		if err != nil {
			return
		}

		// Do differential fuzzing between Inspect and the normal parser
		_, inspectErr := reader.Inspect(true)
		if inspectErr == nil {
			return
		}

		reader, err = car.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal("second NewReader on same data failed", err.Error())
		}
		i, err := reader.IndexReader()
		if err != nil {
			return
		}
		// FIXME: Once indexes are safe to parse, do not skip .car with index in the differential fuzzing.
		if i == nil {
			return
		}
		dr, err := reader.DataReader()
		if err != nil {
			return
		}

		_, err = carv1.ReadHeader(dr, carv1.DefaultMaxAllowedHeaderSize)
		if err != nil {
			return
		}

		blocks, err := car.NewBlockReader(dr)
		if err != nil {
			return
		}

		for {
			_, err := blocks.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				// caught error as expected
				return
			}
		}

		t.Fatal("Inspect found error but we red this file correctly:", inspectErr.Error())
	})
}
