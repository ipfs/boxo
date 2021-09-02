package index

import (
	"encoding/binary"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/ipfs/go-merkledag"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestSortedIndexCodec(t *testing.T) {
	require.Equal(t, multicodec.CarIndexSorted, newSorted().Codec())
}

func TestIndexSorted_GetReturnsNotFoundWhenCidDoesNotExist(t *testing.T) {
	nonExistingKey := merkledag.NewRawNode([]byte("lobstermuncher")).Block.Cid()
	tests := []struct {
		name    string
		subject Index
	}{
		{
			"Sorted",
			newSorted(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOffset, err := GetFirst(tt.subject, nonExistingKey)
			require.Equal(t, ErrNotFound, err)
			require.Equal(t, uint64(0), gotOffset)
		})
	}
}

func TestSingleWidthIndex_GetAll(t *testing.T) {
	l := 4
	width := 9
	buf := make([]byte, width*l)

	// Populate the index bytes as total of four records.
	// The last record should not match the getAll.
	for i := 0; i < l; i++ {
		if i < l-1 {
			buf[i*width] = 1
		} else {
			buf[i*width] = 2
		}
		binary.LittleEndian.PutUint64(buf[(i*width)+1:(i*width)+width], uint64(14))
	}
	subject := &singleWidthIndex{
		width: 9,
		len:   uint64(l),
		index: buf,
	}

	var foundCount int
	err := subject.getAll([]byte{1}, func(u uint64) bool {
		foundCount++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 3, foundCount)
}

func TestIndexSorted_IgnoresIdentityCids(t *testing.T) {
	data := []byte("🐟 in da 🌊d")
	// Generate a record with IDENTITY multihash
	idMh, err := multihash.Sum(data, multihash.IDENTITY, -1)
	require.NoError(t, err)
	idRec := Record{
		Cid:    cid.NewCidV1(cid.Raw, idMh),
		Offset: 1,
	}
	// Generate a record with non-IDENTITY multihash
	nonIdMh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err)
	noIdRec := Record{
		Cid:    cid.NewCidV1(cid.Raw, nonIdMh),
		Offset: 2,
	}

	subject := newSorted()
	err = subject.Load([]Record{idRec, noIdRec})
	require.NoError(t, err)

	// Assert record with IDENTITY CID is not present.
	err = subject.GetAll(idRec.Cid, func(u uint64) bool {
		require.Fail(t, "no IDENTITY record shoul be found")
		return false
	})
	require.Equal(t, ErrNotFound, err)

	// Assert record with non-IDENTITY CID is indeed present.
	var found bool
	err = subject.GetAll(noIdRec.Cid, func(gotOffset uint64) bool {
		found = true
		require.Equal(t, noIdRec.Offset, gotOffset)
		return false
	})
	require.NoError(t, err)
	require.True(t, found)
}
