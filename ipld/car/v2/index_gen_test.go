package car_test

import (
	"io"
	"os"
	"testing"

	"github.com/multiformats/go-multihash"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/internal/carv1"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"

	"github.com/ipld/go-car/v2/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadOrGenerateIndex(t *testing.T) {
	tests := []struct {
		name        string
		carPath     string
		readOpts    []carv2.ReadOption
		wantIndexer func(t *testing.T) index.Index
		wantErr     bool
	}{
		{
			"CarV1IsIndexedAsExpected",
			"testdata/sample-v1.car",
			[]carv2.ReadOption{},
			func(t *testing.T) index.Index {
				v1, err := os.Open("testdata/sample-v1.car")
				require.NoError(t, err)
				defer v1.Close()
				want, err := carv2.GenerateIndex(v1)
				require.NoError(t, err)
				return want
			},
			false,
		},
		{
			"CarV2WithIndexIsReturnedAsExpected",
			"testdata/sample-wrapped-v2.car",
			[]carv2.ReadOption{},
			func(t *testing.T) index.Index {
				v2, err := os.Open("testdata/sample-wrapped-v2.car")
				require.NoError(t, err)
				defer v2.Close()
				reader, err := carv2.NewReader(v2)
				require.NoError(t, err)
				want, err := index.ReadFrom(reader.IndexReader())
				require.NoError(t, err)
				return want
			},
			false,
		},
		{
			"CarV1WithZeroLenSectionIsGeneratedAsExpected",
			"testdata/sample-v1-with-zero-len-section.car",
			[]carv2.ReadOption{carv2.ZeroLengthSectionAsEOF(true)},
			func(t *testing.T) index.Index {
				v1, err := os.Open("testdata/sample-v1-with-zero-len-section.car")
				require.NoError(t, err)
				defer v1.Close()
				want, err := carv2.GenerateIndex(v1, carv2.ZeroLengthSectionAsEOF(true))
				require.NoError(t, err)
				return want
			},
			false,
		},
		{
			"AnotherCarV1WithZeroLenSectionIsGeneratedAsExpected",
			"testdata/sample-v1-with-zero-len-section2.car",
			[]carv2.ReadOption{carv2.ZeroLengthSectionAsEOF(true)},
			func(t *testing.T) index.Index {
				v1, err := os.Open("testdata/sample-v1-with-zero-len-section2.car")
				require.NoError(t, err)
				defer v1.Close()
				want, err := carv2.GenerateIndex(v1, carv2.ZeroLengthSectionAsEOF(true))
				require.NoError(t, err)
				return want
			},
			false,
		},
		{
			"CarV1WithZeroLenSectionWithoutOptionIsError",
			"testdata/sample-v1-with-zero-len-section.car",
			[]carv2.ReadOption{},
			func(t *testing.T) index.Index { return nil },
			true,
		},
		{
			"CarOtherThanV1OrV2IsError",
			"testdata/sample-rootless-v42.car",
			[]carv2.ReadOption{},
			func(t *testing.T) index.Index { return nil },
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			carFile, err := os.Open(tt.carPath)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, carFile.Close()) })
			got, err := carv2.ReadOrGenerateIndex(carFile, tt.readOpts...)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				want := tt.wantIndexer(t)
				require.Equal(t, want, got)
			}
		})
	}
}

func TestGenerateIndexFromFile(t *testing.T) {
	tests := []struct {
		name        string
		carPath     string
		wantIndexer func(t *testing.T) index.Index
		wantErr     bool
	}{
		{
			"CarV1IsIndexedAsExpected",
			"testdata/sample-v1.car",
			func(t *testing.T) index.Index {
				v1, err := os.Open("testdata/sample-v1.car")
				require.NoError(t, err)
				defer v1.Close()
				want, err := carv2.GenerateIndex(v1)
				require.NoError(t, err)
				return want
			},
			false,
		},
		{
			"CarV2IsErrorSinceOnlyV1PayloadIsExpected",
			"testdata/sample-wrapped-v2.car",
			func(t *testing.T) index.Index { return nil },
			true,
		},
		{
			"CarOtherThanV1OrV2IsError",
			"testdata/sample-rootless-v42.car",
			func(t *testing.T) index.Index { return nil },
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := carv2.GenerateIndexFromFile(tt.carPath)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				want := tt.wantIndexer(t)
				require.Equal(t, want, got)
			}
		})
	}
}

func TestMultihashIndexSortedConsistencyWithIndexSorted(t *testing.T) {
	path := "testdata/sample-v1.car"

	sortedIndex, err := carv2.GenerateIndexFromFile(path)
	require.NoError(t, err)
	require.Equal(t, multicodec.CarMultihashIndexSorted, sortedIndex.Codec())

	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	br, err := carv2.NewBlockReader(f)
	require.NoError(t, err)

	subject := generateMultihashSortedIndex(t, path)
	for {
		wantNext, err := br.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		dmh, err := multihash.Decode(wantNext.Cid().Hash())
		require.NoError(t, err)
		if dmh.Code == multihash.IDENTITY {
			continue
		}

		wantCid := wantNext.Cid()
		var wantOffsets []uint64
		err = sortedIndex.GetAll(wantCid, func(o uint64) bool {
			wantOffsets = append(wantOffsets, o)
			return false
		})
		require.NoError(t, err)

		var gotOffsets []uint64
		err = subject.GetAll(wantCid, func(o uint64) bool {
			gotOffsets = append(gotOffsets, o)
			return false
		})

		require.NoError(t, err)
		require.Equal(t, wantOffsets, gotOffsets)
	}
}

func TestMultihashSorted_ForEachIsConsistentWithGetAll(t *testing.T) {
	path := "testdata/sample-v1.car"
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	br, err := carv2.NewBlockReader(f)
	require.NoError(t, err)
	subject := generateMultihashSortedIndex(t, path)

	gotForEach := make(map[string]uint64)
	err = subject.ForEach(func(mh multihash.Multihash, offset uint64) error {
		gotForEach[mh.String()] = offset
		return nil
	})
	require.NoError(t, err)

	for {
		b, err := br.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		c := b.Cid()
		dmh, err := multihash.Decode(c.Hash())
		require.NoError(t, err)
		if dmh.Code == multihash.IDENTITY {
			continue
		}

		wantMh := c.Hash()

		var wantOffset uint64
		err = subject.GetAll(c, func(u uint64) bool {
			wantOffset = u
			return false
		})
		require.NoError(t, err)

		s := wantMh.String()
		gotOffset, ok := gotForEach[s]
		require.True(t, ok)
		require.Equal(t, wantOffset, gotOffset)
	}
}

func generateMultihashSortedIndex(t *testing.T, path string) *index.MultihashIndexSorted {
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	reader := internalio.ToByteReadSeeker(f)
	header, err := carv1.ReadHeader(reader)
	require.NoError(t, err)
	require.Equal(t, uint64(1), header.Version)

	idx := index.NewMultihashSorted()
	records := make([]index.Record, 0)

	var sectionOffset int64
	sectionOffset, err = reader.Seek(0, io.SeekCurrent)
	require.NoError(t, err)

	for {
		sectionLen, err := varint.ReadUvarint(reader)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		if sectionLen == 0 {
			break
		}

		cidLen, c, err := cid.CidFromReader(reader)
		require.NoError(t, err)
		records = append(records, index.Record{Cid: c, Offset: uint64(sectionOffset)})
		remainingSectionLen := int64(sectionLen) - int64(cidLen)
		sectionOffset, err = reader.Seek(remainingSectionLen, io.SeekCurrent)
		require.NoError(t, err)
	}

	err = idx.Load(records)
	require.NoError(t, err)

	return idx
}
