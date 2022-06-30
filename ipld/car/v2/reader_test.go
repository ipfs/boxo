package car_test

import (
	"io"
	"os"
	"testing"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/index/testutil"
	"github.com/ipld/go-car/v2/internal/carv1"
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
			require.Nil(t, subject.IndexReader())
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

			gotIndexReader := subject.IndexReader()
			require.NotNil(t, gotIndexReader)
			gotIndex, err := index.ReadFrom(gotIndexReader)
			require.NoError(t, err)
			wantIndex, err := carv2.GenerateIndex(subject.DataReader())
			require.NoError(t, err)
			testutil.AssertIdenticalIndexes(t, wantIndex, gotIndex)
		})
	}
}

func TestOpenReader_DoesNotPanicForReadersCreatedBeforeClosure(t *testing.T) {
	subject, err := carv2.OpenReader("testdata/sample-wrapped-v2.car")
	require.NoError(t, err)
	dReaderBeforeClosure := subject.DataReader()
	iReaderBeforeClosure := subject.IndexReader()
	require.NoError(t, subject.Close())

	buf := make([]byte, 1)
	panicTest := func(r io.ReaderAt) {
		_, err := r.ReadAt(buf, 0)
		require.EqualError(t, err, "mmap: closed")
	}

	require.NotPanics(t, func() { panicTest(dReaderBeforeClosure) })
	require.NotPanics(t, func() { panicTest(iReaderBeforeClosure) })
}

func TestOpenReader_DoesNotPanicForReadersCreatedAfterClosure(t *testing.T) {
	subject, err := carv2.OpenReader("testdata/sample-wrapped-v2.car")
	require.NoError(t, err)
	require.NoError(t, subject.Close())
	dReaderAfterClosure := subject.DataReader()
	iReaderAfterClosure := subject.IndexReader()

	buf := make([]byte, 1)
	panicTest := func(r io.ReaderAt) {
		_, err := r.ReadAt(buf, 0)
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
			require.Nil(t, subject.IndexReader())
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
