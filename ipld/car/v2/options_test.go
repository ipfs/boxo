package car_test

import (
	"testing"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestApplyOptions_SetsExpectedDefaults(t *testing.T) {
	require.Equal(t, carv2.Options{
		IndexCodec:      multicodec.CarMultihashIndexSorted,
		MaxIndexCidSize: carv2.DefaultMaxIndexCidSize,
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
		},
		carv2.ApplyOptions(
			carv2.UseDataPadding(123),
			carv2.UseIndexPadding(456),
			carv2.UseIndexCodec(multicodec.CarIndexSorted),
			carv2.ZeroLengthSectionAsEOF(true),
			carv2.MaxIndexCidSize(789),
			carv2.StoreIdentityCIDs(true),
			blockstore.AllowDuplicatePuts(true),
			blockstore.UseWholeCIDs(true),
		))
}
