package ndjson

import (
	"encoding/json"
	"io"

	"github.com/ipfs/go-libipfs/routing/http/types"
	"github.com/ipfs/go-libipfs/routing/http/types/iter"
)

type readProvidersResponseIter struct {
	iter.Iter[types.UnknownProviderRecord]
}

func NewReadProvidersResponseIter(r io.Reader) *readProvidersResponseIter {
	return &readProvidersResponseIter{Iter: iter.FromReaderJSON[types.UnknownProviderRecord](r)}
}

func (p *readProvidersResponseIter) Next() (types.ProviderResponse, bool, error) {
	v, ok, err := p.Iter.Next()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	switch v.Schema {
	case types.SchemaBitswap:
		var prov types.ReadBitswapProviderRecord
		err := json.Unmarshal(v.Bytes, &prov)
		if err != nil {
			return nil, false, err
		}
		return &prov, true, nil
	default:
		return &v, true, nil
	}
}

func NewWriteProvidersRequestIter(r io.Reader) *writeProvidersRequestIter {
	return &writeProvidersRequestIter{Iter: iter.FromReaderJSON[types.UnknownProviderRecord](r)}
}

type writeProvidersRequestIter struct {
	iter.Iter[types.UnknownProviderRecord]
}

func (p *writeProvidersRequestIter) Next() (types.WriteProviderRecord, bool, error) {
	v, ok, err := p.Iter.Next()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	switch v.Schema {
	case types.SchemaBitswap:
		var prov types.WriteBitswapProviderRecord
		err := json.Unmarshal(v.Bytes, &prov)
		if err != nil {
			return nil, false, err
		}
		return &prov, true, nil
	default:
		return &v, true, nil
	}
}

func NewWriteProvidersResponseIter(r io.Reader) *writeProvidersResponseIter {
	return &writeProvidersResponseIter{Iter: iter.FromReaderJSON[types.UnknownProviderRecord](r)}
}

type writeProvidersResponseIter struct {
	iter.Iter[types.UnknownProviderRecord]
}

func (p *writeProvidersResponseIter) Next() (types.ProviderResponse, bool, error) {
	v, ok, err := p.Iter.Next()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	switch v.Schema {
	case types.SchemaBitswap:
		var prov types.WriteBitswapProviderRecordResponse
		err := json.Unmarshal(v.Bytes, &prov)
		if err != nil {
			return nil, false, err
		}
		return &prov, true, nil
	default:
		return &v, true, nil
	}
}
