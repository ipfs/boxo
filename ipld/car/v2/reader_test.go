package car_test

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"

	carv2 "github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/boxo/ipld/car/v2/index"
	"github.com/ipfs/boxo/ipld/car/v2/internal/carv1"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestReadVersion(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    uint64
		wantErr bool
	}{
		{
			name: "CarV1VersionIsOne",
			path: "testdata/sample-v1.car",
			want: 1,
		},
		{
			name: "CarV2VersionIsTwo",
			path: "testdata/sample-rw-bs-v2.car",
			want: 2,
		},
		{
			name: "CarV1VersionWithZeroLenSectionIsOne",
			path: "testdata/sample-v1-with-zero-len-section.car",
			want: 1,
		},
		{
			name: "AnotherCarV1VersionWithZeroLenSectionIsOne",
			path: "testdata/sample-v1-with-zero-len-section2.car",
			want: 1,
		},
		{
			name: "WrappedCarV1InCarV2VersionIsTwo",
			path: "testdata/sample-wrapped-v2.car",
			want: 2,
		},
		{
			name: "FutureVersionWithCorrectPragmaIsAsExpected",
			path: "testdata/sample-rootless-v42.car",
			want: 42,
		},
		{
			name: "CarV1WithValidHeaderButCorruptSectionIsOne",
			path: "testdata/sample-v1-tailing-corrupt-section.car",
			want: 1,
		},
		{
			name: "CarV2WithValidHeaderButCorruptSectionAndIndexIsTwo",
			path: "testdata/sample-v2-corrupt-data-and-index.car",
			want: 2,
		},
		{
			name:    "CarFileWithCorruptPragmaIsError",
			path:    "testdata/sample-corrupt-pragma.car",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(tt.path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, f.Close()) })

			got, err := carv2.ReadVersion(f)
			if tt.wantErr {
				require.Error(t, err, "ReadVersion() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got, "ReadVersion() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReaderFailsOnUnknownVersion(t *testing.T) {
	_, err := carv2.OpenReader("testdata/sample-rootless-v42.car")
	require.EqualError(t, err, "invalid car version: 42")
}

func TestReaderFailsOnCorruptPragma(t *testing.T) {
	_, err := carv2.OpenReader("testdata/sample-corrupt-pragma.car")
	require.EqualError(t, err, "unexpected EOF")
}

func TestReader_WithCarV1Consistency(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		zerLenAsEOF bool
	}{
		{
			name: "CarV1WithoutZeroLengthSection",
			path: "testdata/sample-v1.car",
		},
		{
			name:        "CarV1WithZeroLenSection",
			path:        "testdata/sample-v1-with-zero-len-section.car",
			zerLenAsEOF: true,
		},
		{
			name:        "AnotherCarV1WithZeroLenSection",
			path:        "testdata/sample-v1-with-zero-len-section2.car",
			zerLenAsEOF: true,
		},
		{
			name: "CarV1WithZeroLenSectionWithoutOption",
			path: "testdata/sample-v1-with-zero-len-section.car",
		},
		{
			name: "AnotherCarV1WithZeroLenSectionWithoutOption",
			path: "testdata/sample-v1-with-zero-len-section2.car",
		},
		{
			name: "CorruptCarV1",
			path: "testdata/sample-v1-tailing-corrupt-section.car",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject, err := carv2.OpenReader(tt.path, carv2.ZeroLengthSectionAsEOF(tt.zerLenAsEOF))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, subject.Close()) })
			wantReader := requireNewCarV1ReaderFromV1File(t, tt.path, tt.zerLenAsEOF)

			require.Equal(t, uint64(1), subject.Version)
			gotRoots, err := subject.Roots()
			require.NoError(t, err)
			require.Equal(t, wantReader.Header.Roots, gotRoots)
			ir, err := subject.IndexReader()
			require.Nil(t, ir)
			require.NoError(t, err)
		})
	}
}

func TestReader_WithCarV2Consistency(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		zerLenAsEOF bool
	}{
		{
			name: "CarV2WrappingV1",
			path: "testdata/sample-wrapped-v2.car",
		},
		{
			name: "CarV2ProducedByBlockstore",
			path: "testdata/sample-rw-bs-v2.car",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject, err := carv2.OpenReader(tt.path, carv2.ZeroLengthSectionAsEOF(tt.zerLenAsEOF))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, subject.Close()) })
			wantReader := requireNewCarV1ReaderFromV2File(t, tt.path, tt.zerLenAsEOF)

			require.Equal(t, uint64(2), subject.Version)
			gotRoots, err := subject.Roots()
			require.NoError(t, err)
			require.Equal(t, wantReader.Header.Roots, gotRoots)

			gotIndexReader, err := subject.IndexReader()
			require.NoError(t, err)
			require.NotNil(t, gotIndexReader)
			gotIndex, err := index.ReadFrom(gotIndexReader)
			require.NoError(t, err)
			dr, err := subject.DataReader()
			require.NoError(t, err)
			wantIndex, err := carv2.GenerateIndex(dr)
			require.NoError(t, err)
			require.Equal(t, wantIndex, gotIndex)
		})
	}
}

func TestOpenReader_DoesNotPanicForReadersCreatedBeforeClosure(t *testing.T) {
	subject, err := carv2.OpenReader("testdata/sample-wrapped-v2.car")
	require.NoError(t, err)
	dReaderBeforeClosure, err := subject.DataReader()
	require.NoError(t, err)
	iReaderBeforeClosure, err := subject.IndexReader()
	require.NoError(t, err)
	require.NoError(t, subject.Close())

	buf := make([]byte, 1)
	panicTest := func(r io.Reader) {
		_, err := r.Read(buf)
		require.EqualError(t, err, "mmap: closed")
	}

	require.NotPanics(t, func() { panicTest(dReaderBeforeClosure) })
	require.NotPanics(t, func() { panicTest(iReaderBeforeClosure) })
}

func TestOpenReader_DoesNotPanicForReadersCreatedAfterClosure(t *testing.T) {
	subject, err := carv2.OpenReader("testdata/sample-wrapped-v2.car")
	require.NoError(t, err)
	require.NoError(t, subject.Close())
	dReaderAfterClosure, err := subject.DataReader()
	require.NoError(t, err)
	iReaderAfterClosure, err := subject.IndexReader()
	require.NoError(t, err)

	buf := make([]byte, 1)
	panicTest := func(r io.Reader) {
		_, err := r.Read(buf)
		require.EqualError(t, err, "mmap: closed")
	}

	require.NotPanics(t, func() { panicTest(dReaderAfterClosure) })
	require.NotPanics(t, func() { panicTest(iReaderAfterClosure) })
}

func TestReader_ReturnsNilWhenThereIsNoIndex(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{
			name: "IndexlessCarV2",
			path: "testdata/sample-v2-indexless.car",
		},
		{
			name: "CarV1",
			path: "testdata/sample-v1.car",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject, err := carv2.OpenReader(tt.path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, subject.Close()) })
			ir, err := subject.IndexReader()
			require.NoError(t, err)
			require.Nil(t, ir)
		})
	}
}

func requireNewCarV1ReaderFromV2File(t *testing.T, carV12Path string, zerLenAsEOF bool) *carv1.CarReader {
	f, err := os.Open(carV12Path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	_, err = f.Seek(carv2.PragmaSize, io.SeekStart)
	require.NoError(t, err)
	header := carv2.Header{}
	_, err = header.ReadFrom(f)
	require.NoError(t, err)
	return requireNewCarV1Reader(t, io.NewSectionReader(f, int64(header.DataOffset), int64(header.DataSize)), zerLenAsEOF)
}

func requireNewCarV1ReaderFromV1File(t *testing.T, carV1Path string, zerLenAsEOF bool) *carv1.CarReader {
	f, err := os.Open(carV1Path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	return requireNewCarV1Reader(t, f, zerLenAsEOF)
}

func requireNewCarV1Reader(t *testing.T, r io.Reader, zerLenAsEOF bool) *carv1.CarReader {
	var cr *carv1.CarReader
	var err error
	if zerLenAsEOF {
		cr, err = carv1.NewCarReaderWithZeroLengthSectionAsEOF(r)
	} else {
		cr, err = carv1.NewCarReader(r)
	}
	require.NoError(t, err)
	return cr
}

func TestInspect(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		carHex        string
		zerLenAsEOF   bool
		expectedStats carv2.Stats
	}{
		{
			name: "IndexlessCarV2",
			path: "testdata/sample-v2-indexless.car",
			expectedStats: carv2.Stats{
				Version: 2,
				Header: carv2.Header{
					Characteristics: carv2.Characteristics{0, 0},
					DataOffset:      51,
					DataSize:        479907,
					IndexOffset:     0,
				},
				Roots:          []cid.Cid{mustCidDecode("bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy")},
				RootsPresent:   true,
				AvgBlockLength: 417, // 417.6644423260248
				MinBlockLength: 1,
				MaxBlockLength: 1342,
				AvgCidLength:   37, // 37.86939942802669
				MinCidLength:   14,
				MaxCidLength:   38,
				BlockCount:     1049,
				CodecCounts: map[multicodec.Code]uint64{
					multicodec.Raw:     6,
					multicodec.DagCbor: 1043,
				},
				MhTypeCounts: map[multicodec.Code]uint64{
					multicodec.Identity:   6,
					multicodec.Blake2b256: 1043,
				},
			},
		},
		{
			// same payload as IndexlessCarV2, so only difference is the Version & Header
			name: "CarV1",
			path: "testdata/sample-v1.car",
			expectedStats: carv2.Stats{
				Version:        1,
				Header:         carv2.Header{},
				Roots:          []cid.Cid{mustCidDecode("bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy")},
				RootsPresent:   true,
				AvgBlockLength: 417, // 417.6644423260248
				MinBlockLength: 1,
				MaxBlockLength: 1342,
				AvgCidLength:   37, // 37.86939942802669
				MinCidLength:   14,
				MaxCidLength:   38,
				BlockCount:     1049,
				CodecCounts: map[multicodec.Code]uint64{
					multicodec.Raw:     6,
					multicodec.DagCbor: 1043,
				},
				MhTypeCounts: map[multicodec.Code]uint64{
					multicodec.Identity:   6,
					multicodec.Blake2b256: 1043,
				},
			},
		},
		{
			// same payload as IndexlessCarV2, so only difference is the Header
			name: "CarV2ProducedByBlockstore",
			path: "testdata/sample-rw-bs-v2.car",
			expectedStats: carv2.Stats{
				Version: 2,
				Header: carv2.Header{
					DataOffset:  1464,
					DataSize:    273,
					IndexOffset: 1737,
				},
				Roots: []cid.Cid{
					mustCidDecode("bafkreifuosuzujyf4i6psbneqtwg2fhplc2wxptc5euspa2gn3bwhnihfu"),
					mustCidDecode("bafkreifc4hca3inognou377hfhvu2xfchn2ltzi7yu27jkaeujqqqdbjju"),
					mustCidDecode("bafkreig5lvr4l6b4fr3un4xvzeyt3scevgsqjgrhlnwxw2unwbn5ro276u"),
				},
				RootsPresent:   true,
				BlockCount:     3,
				CodecCounts:    map[multicodec.Code]uint64{multicodec.Raw: 3},
				MhTypeCounts:   map[multicodec.Code]uint64{multicodec.Sha2_256: 3},
				AvgCidLength:   36,
				MaxCidLength:   36,
				MinCidLength:   36,
				AvgBlockLength: 6,
				MaxBlockLength: 9,
				MinBlockLength: 4,
				IndexCodec:     multicodec.CarMultihashIndexSorted,
			},
		},
		// same as CarV1 but with a zero-byte EOF to test options
		{
			name:        "CarV1VersionWithZeroLenSectionIsOne",
			path:        "testdata/sample-v1-with-zero-len-section.car",
			zerLenAsEOF: true,
			expectedStats: carv2.Stats{
				Version:        1,
				Header:         carv2.Header{},
				Roots:          []cid.Cid{mustCidDecode("bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oy")},
				RootsPresent:   true,
				AvgBlockLength: 417, // 417.6644423260248
				MinBlockLength: 1,
				MaxBlockLength: 1342,
				AvgCidLength:   37, // 37.86939942802669
				MinCidLength:   14,
				MaxCidLength:   38,
				BlockCount:     1049,
				CodecCounts: map[multicodec.Code]uint64{
					multicodec.Raw:     6,
					multicodec.DagCbor: 1043,
				},
				MhTypeCounts: map[multicodec.Code]uint64{
					multicodec.Identity:   6,
					multicodec.Blake2b256: 1043,
				},
			},
		},
		{
			// A case where this _could_ be a valid CAR if we allowed identity CIDs
			// and not matching block contents to exist, there's no block bytes in
			// this. It will only fail if you don't validate the CID matches the,
			// bytes (see TestInspectError for that case).
			name: "IdentityCID",
			//       47 {version:1,roots:[identity cid]}                                                               25 identity cid (dag-json {"identity":"block"})
			carHex: "2f a265726f6f747381d82a581a0001a90200147b226964656e74697479223a22626c6f636b227d6776657273696f6e01 19 01a90200147b226964656e74697479223a22626c6f636b227d",
			expectedStats: carv2.Stats{
				Version:      1,
				Roots:        []cid.Cid{mustCidDecode("baguqeaaupmrgszdfnz2gs5dzei5ceytmn5rwwit5")},
				RootsPresent: true,
				BlockCount:   1,
				CodecCounts:  map[multicodec.Code]uint64{multicodec.DagJson: 1},
				MhTypeCounts: map[multicodec.Code]uint64{multicodec.Identity: 1},
				AvgCidLength: 25,
				MaxCidLength: 25,
				MinCidLength: 25,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reader *carv2.Reader
			var err error
			if tt.path != "" {
				reader, err = carv2.OpenReader(tt.path, carv2.ZeroLengthSectionAsEOF(tt.zerLenAsEOF))
				require.NoError(t, err)
			} else {
				byts, err := hex.DecodeString(strings.ReplaceAll(tt.carHex, " ", ""))
				require.NoError(t, err)
				reader, err = carv2.NewReader(bytes.NewReader(byts), carv2.ZeroLengthSectionAsEOF(tt.zerLenAsEOF))
				require.NoError(t, err)
			}
			t.Cleanup(func() { require.NoError(t, reader.Close()) })
			stats, err := reader.Inspect(false)
			require.NoError(t, err)
			require.Equal(t, tt.expectedStats, stats)
		})
	}
}

func TestInspectError(t *testing.T) {
	tests := []struct {
		name                 string
		carHex               string
		expectedOpenError    string
		expectedInspectError string
		validateBlockHash    bool
	}{
		{
			name:                 "BadCidV0",
			carHex:               "3aa265726f6f747381d8305825000130302030303030303030303030303030303030303030303030303030303030303030306776657273696f6e010130",
			expectedInspectError: "invalid cid: expected 1 as the cid version number, got: 48",
		},
		{
			name:              "BadHeaderLength",
			carHex:            "e0e0e0e0a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80b6776657273696f6e19",
			expectedOpenError: "invalid header data, length of read beyond allowable maximum",
		},
		{
			name:                 "BadSectionLength",
			carHex:               "11a265726f6f7473806776657273696f6e01e0e0e0e0a7060155122001d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca00000000000000000000",
			expectedInspectError: "invalid section data, length of read beyond allowable maximum",
		},
		{
			name:                 "BadSectionLength2",
			carHex:               "3aa265726f6f747381d8305825000130302030303030303030303030303030303030303030303030303030303030303030306776657273696f6e01200130302030303030303030303030303030303030303030303030303030303030303030303030303030303030",
			expectedInspectError: "section length shorter than CID length",
			validateBlockHash:    true,
		},
		{
			name:                 "BadSectionLength3",
			carHex:               "11a265726f6f7473f66776657273696f6e0180",
			expectedInspectError: "unexpected EOF",
		},
		{
			name: "BadBlockHash(SanityCheck)", // this should pass because we don't ask the CID be validated even though it doesn't match
			//       header                             cid                                                                          data
			carHex: "11a265726f6f7473806776657273696f6e 012e0155122001d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca ffffffffffffffffffff",
		},
		{
			name: "BadBlockHash", // same as above, but we ask for CID validation
			//       header                             cid                                                                          data
			carHex:               "11a265726f6f7473806776657273696f6e 012e0155122001d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca ffffffffffffffffffff",
			validateBlockHash:    true,
			expectedInspectError: "mismatch in content integrity, expected: bafkreiab2rek7wjiazkfrt3hbnqpljmu24226alszdlh6ivic2abgjubzi, got: bafkreiaaqoxrddiyuy6gxnks6ioqytxhq5a7tchm2mm5htigznwiljukmm",
		},
		{
			name: "IdentityCID", // a case where this _could_ be a valid CAR if we allowed identity CIDs and not matching block contents to exist, there's no block bytes in this
			//                  47 {version:1,roots:[identity cid]}                                                               25 identity cid (dag-json {"identity":"block"})
			carHex:               "2f a265726f6f747381d82a581a0001a90200147b226964656e74697479223a22626c6f636b227d6776657273696f6e01 19 01a90200147b226964656e74697479223a22626c6f636b227d",
			validateBlockHash:    true,
			expectedInspectError: "mismatch in content integrity, expected: baguqeaaupmrgszdfnz2gs5dzei5ceytmn5rwwit5, got: baguqeaaa",
		},
		// the bad index tests are manually constructed from this single-block CARv2 by adjusting the Uint32 and Uint64 values in the index:
		// pragma                 carv2 header                                                                     carv1                                                                                                                              icodec count  codec            count (swi) width dataLen          mh                                                               offset
		// 0aa16776657273696f6e02 00000000000000000000000000000000330000000000000041000000000000007400000000000000 11a265726f6f7473806776657273696f6e012e0155122001d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca00000000000000000000 8108 01000000 1200000000000000 01000000 28000000 2800000000000000 01d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca 1200000000000000
		// we don't test any further into the index, to do that, a user should do a ForEach across the loaded index (and sanity check the offsets)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			car, _ := hex.DecodeString(strings.ReplaceAll(tt.carHex, " ", ""))
			reader, err := carv2.NewReader(bytes.NewReader(car))
			if tt.expectedOpenError != "" {
				require.Error(t, err)
				require.Equal(t, tt.expectedOpenError, err.Error())
				return
			} else {
				require.NoError(t, err)
			}
			t.Cleanup(func() { require.NoError(t, reader.Close()) })
			_, err = reader.Inspect(tt.validateBlockHash)
			if tt.expectedInspectError != "" {
				require.Error(t, err)
				require.Equal(t, tt.expectedInspectError, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIndex_ReadFromCorruptIndex(t *testing.T) {
	tests := []struct {
		name        string
		givenCarHex string
		wantErr     string
	}{
		{
			name: "BadIndexCountOverflow",
			//                     pragma                 carv2 header                                                                     carv1                                                                                                                              icodec count  codec            count (swi) width dataLen          mh                                                               offset
			givenCarHex: "0aa16776657273696f6e02 00000000000000000000000000000000330000000000000041000000000000007400000000000000 11a265726f6f7473806776657273696f6e012e0155122001d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca00000000000000000000 8108 ffffffff 1200000000000000 01000000 28000000 2800000000000000 01d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca 1200000000000000",
			wantErr:     "index too big; MultihashIndexSorted count is overflowing int32",
		},
		{
			name: "BadIndexCountTooMany",
			//                     pragma                 carv2 header                                                                     carv1                                                                                                                              icodec count  codec            count (swi) width dataLen          mh                                                               offset
			givenCarHex: "0aa16776657273696f6e02 00000000000000000000000000000000330000000000000041000000000000007400000000000000 11a265726f6f7473806776657273696f6e012e0155122001d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca00000000000000000000 8108 ffffff7f 1200000000000000 01000000 28000000 2800000000000000 01d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca 1200000000000000",
			wantErr:     "unexpected EOF",
		},
		{
			name: "BadIndexMultiWidthOverflow",
			//                     pragma                 carv2 header                                                                     carv1                                                                                                                              icodec count  codec            count (swi) width dataLen          mh                                                               offset
			givenCarHex: "0aa16776657273696f6e02 00000000000000000000000000000000330000000000000041000000000000007400000000000000 11a265726f6f7473806776657273696f6e012e0155122001d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca00000000000000000000 8108 01000000 1200000000000000 ffffffff 28000000 2800000000000000 01d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca 1200000000000000",
			wantErr:     "index too big; multiWidthIndex count is overflowing int32",
		},
		{
			name: "BadIndexMultiWidthTooMany",
			//                     pragma                 carv2 header                                                                     carv1                                                                                                                              icodec count  codec            count (swi) width dataLen          mh                                                               offset
			givenCarHex: "0aa16776657273696f6e02 00000000000000000000000000000000330000000000000041000000000000007400000000000000 11a265726f6f7473806776657273696f6e012e0155122001d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca00000000000000000000 8108 01000000 1200000000000000 ffffff7f 28000000 2800000000000000 01d448afd928065458cf670b60f5a594d735af0172c8d67f22a81680132681ca 1200000000000000",
			wantErr:     "unexpected EOF",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			car, _ := hex.DecodeString(strings.ReplaceAll(test.givenCarHex, " ", ""))
			reader, err := carv2.NewReader(bytes.NewReader(car))
			require.NoError(t, err)

			ir, err := reader.IndexReader()
			require.NoError(t, err)
			require.NotNil(t, ir)

			gotIdx, err := index.ReadFrom(ir)
			if test.wantErr == "" {
				require.NoError(t, err)
				require.NotNil(t, gotIdx)
			} else {
				require.Error(t, err)
				require.Equal(t, test.wantErr, err.Error())
				require.Nil(t, gotIdx)
			}
		})
	}
}

func mustCidDecode(s string) cid.Cid {
	c, err := cid.Decode(s)
	if err != nil {
		panic(err)
	}
	return c
}
