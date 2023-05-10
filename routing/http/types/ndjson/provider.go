package ndjson

import (
	"encoding/json"
	"io"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
)

// NewReadProvidersResponseIter returns an iterator that reads Read Provider Records from the given reader.
func NewReadProvidersResponseIter(r io.Reader) iter.Iter[iter.Result[types.ProviderResponse]] {
	jsonIter := iter.FromReaderJSON[types.UnknownProviderRecord](r)
	mapFn := func(upr iter.Result[types.UnknownProviderRecord]) iter.Result[types.ProviderResponse] {
		var result iter.Result[types.ProviderResponse]
		if upr.Err != nil {
			result.Err = upr.Err
			return result
		}
		switch upr.Val.Schema {
		case types.SchemaBitswap:
			var prov types.ReadBitswapProviderRecord
			err := json.Unmarshal(upr.Val.Bytes, &prov)
			if err != nil {
				result.Err = err
				return result
			}
			result.Val = &prov
		default:
			result.Val = &upr.Val
		}
		return result
	}

	return iter.Map[iter.Result[types.UnknownProviderRecord]](jsonIter, mapFn)
}
