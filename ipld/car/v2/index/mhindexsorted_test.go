package index_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/multiformats/go-multicodec"

	"github.com/ipfs/boxo/ipld/car/v2/index"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestMutilhashSortedIndex_Codec(t *testing.T) {
	subject, err := index.New(multicodec.CarMultihashIndexSorted)
	require.NoError(t, err)
	require.Equal(t, multicodec.CarMultihashIndexSorted, subject.Codec())
}

func TestMultiWidthCodedIndex_MarshalUnmarshal(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	records := generateIndexRecords(t, multihash.SHA2_256, rng)

	// Create a new mh sorted index and load randomly generated records into it.
	subject, err := index.New(multicodec.CarMultihashIndexSorted)
	require.NoError(t, err)
	err = subject.Load(records)
	require.NoError(t, err)

	// Marshal the index.
	buf := new(bytes.Buffer)
	_, err = subject.Marshal(buf)
	require.NoError(t, err)

	// Unmarshal it back to another instance of mh sorted index.
	umSubject, err := index.New(multicodec.CarMultihashIndexSorted)
	require.NoError(t, err)
	err = umSubject.Unmarshal(buf)
	require.NoError(t, err)

	// Assert original records are present in both index instances with expected offset.
	requireContainsAll(t, subject, records)
	requireContainsAll(t, umSubject, records)
}

func TestMultiWidthCodedIndex_StableIterate(t *testing.T) {
	rng := rand.New(rand.NewSource(1414))
	records := generateIndexRecords(t, multihash.SHA2_256, rng)
	records = append(records, generateIndexRecords(t, multihash.SHA2_512, rng)...)
	records = append(records, generateIndexRecords(t, multihash.IDENTITY, rng)...)

	// Create a new mh sorted index and load randomly generated records into it.
	idx, err := index.New(multicodec.CarMultihashIndexSorted)
	require.NoError(t, err)
	err = idx.Load(records)
	require.NoError(t, err)

	subject, ok := idx.(index.IterableIndex)
	require.True(t, ok)

	mh := make([]multihash.Multihash, 0, len(records))
	require.NoError(t, subject.ForEach(func(m multihash.Multihash, _ uint64) error {
		mh = append(mh, m)
		return nil
	}))

	for i := 0; i < 10; i++ {
		candidate := make([]multihash.Multihash, 0, len(records))
		require.NoError(t, subject.ForEach(func(m multihash.Multihash, _ uint64) error {
			candidate = append(candidate, m)
			return nil
		}))
		require.Equal(t, mh, candidate)
	}
}

func generateIndexRecords(t *testing.T, hasherCode uint64, rng *rand.Rand) []index.Record {
	var records []index.Record
	recordCount := rng.Intn(99) + 1 // Up to 100 records
	for i := 0; i < recordCount; i++ {
		records = append(records, index.Record{
			Cid:    generateCidV1(t, hasherCode, rng),
			Offset: rng.Uint64(),
		})
	}
	return records
}

func generateCidV1(t *testing.T, hasherCode uint64, rng *rand.Rand) cid.Cid {
	data := []byte(fmt.Sprintf("🌊d-%d", rng.Uint64()))
	mh, err := multihash.Sum(data, hasherCode, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}

func requireContainsAll(t *testing.T, subject index.Index, nonIdentityRecords []index.Record) {
	for _, r := range nonIdentityRecords {
		wantCid := r.Cid
		wantOffset := r.Offset

		var gotOffsets []uint64
		err := subject.GetAll(wantCid, func(o uint64) bool {
			gotOffsets = append(gotOffsets, o)
			return false
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(gotOffsets))
		require.Equal(t, wantOffset, gotOffsets[0])
	}
}
