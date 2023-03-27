package car_test

import (
	"math"
	"testing"

	carv2 "github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/boxo/ipld/car/v2/blockstore"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestApplyOptions_SetsExpectedDefaults(t *testing.T) {
	require.Equal(t, carv2.Options{
		IndexCodec:            multicodec.CarMultihashIndexSorted,
		MaxIndexCidSize:       carv2.DefaultMaxIndexCidSize,
		MaxTraversalLinks:     math.MaxInt64,
		MaxAllowedHeaderSize:  32 << 20,
		MaxAllowedSectionSize: 8 << 20,
	}, carv2.ApplyOptions())
}

func TestApplyOptions_AppliesOptions(t *testing.T) {
	require.Equal(t,
		carv2.Options{
			DataPadding:                  123,
			IndexPadding:                 456,
			IndexCodec:                   multicodec.CarIndexSorted,
			ZeroLengthSectionAsEOF:       true,
			MaxIndexCidSize:              789,
			StoreIdentityCIDs:            true,
			BlockstoreAllowDuplicatePuts: true,
			BlockstoreUseWholeCIDs:       true,
			MaxTraversalLinks:            math.MaxInt64,
			MaxAllowedHeaderSize:         101,
			MaxAllowedSectionSize:        202,
		},
		carv2.ApplyOptions(
			carv2.UseDataPadding(123),
			carv2.UseIndexPadding(456),
			carv2.UseIndexCodec(multicodec.CarIndexSorted),
			carv2.ZeroLengthSectionAsEOF(true),
			carv2.MaxIndexCidSize(789),
			carv2.StoreIdentityCIDs(true),
			carv2.MaxAllowedHeaderSize(101),
			carv2.MaxAllowedSectionSize(202),
			blockstore.AllowDuplicatePuts(true),
			blockstore.UseWholeCIDs(true),
		))
}
