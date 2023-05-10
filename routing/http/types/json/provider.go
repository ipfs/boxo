package json

import (
	"encoding/json"

	"github.com/ipfs/boxo/routing/http/types"
)

// ReadProvidersResponse is the result of a Provide request
type ReadProvidersResponse struct {
	Providers []types.ProviderResponse
}

func (r *ReadProvidersResponse) UnmarshalJSON(b []byte) error {
	var tempFPR struct{ Providers []json.RawMessage }
	err := json.Unmarshal(b, &tempFPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempFPR.Providers {
		var readProv types.UnknownProviderRecord
		err := json.Unmarshal(provBytes, &readProv)
		if err != nil {
			return err
		}

		switch readProv.Schema {
		case types.SchemaBitswap:
			var prov types.ReadBitswapProviderRecord
			err := json.Unmarshal(readProv.Bytes, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		default:
			r.Providers = append(r.Providers, &readProv)
		}

	}
	return nil
}

type WriteProvidersRequest struct {
	Providers []types.WriteProviderRecord
}

func (r *WriteProvidersRequest) UnmarshalJSON(b []byte) error {
	type wpr struct{ Providers []json.RawMessage }
	var tempWPR wpr
	err := json.Unmarshal(b, &tempWPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempWPR.Providers {
		var rawProv types.UnknownProviderRecord
		err := json.Unmarshal(provBytes, &rawProv)
		if err != nil {
			return err
		}

		switch rawProv.Schema {
		case types.SchemaBitswap:
			var prov types.WriteBitswapProviderRecord
			err := json.Unmarshal(rawProv.Bytes, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		default:
			var prov types.UnknownProviderRecord
			err := json.Unmarshal(b, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		}
	}
	return nil
}

// WriteProvidersResponse is the result of a Provide operation
type WriteProvidersResponse struct {
	ProvideResults []types.ProviderResponse
}

func (r *WriteProvidersResponse) UnmarshalJSON(b []byte) error {
	var tempWPR struct{ ProvideResults []json.RawMessage }
	err := json.Unmarshal(b, &tempWPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempWPR.ProvideResults {
		var rawProv types.UnknownProviderRecord
		err := json.Unmarshal(provBytes, &rawProv)
		if err != nil {
			return err
		}

		switch rawProv.Schema {
		case types.SchemaBitswap:
			var prov types.WriteBitswapProviderRecordResponse
			err := json.Unmarshal(rawProv.Bytes, &prov)
			if err != nil {
				return err
			}
			r.ProvideResults = append(r.ProvideResults, &prov)
		default:
			r.ProvideResults = append(r.ProvideResults, &rawProv)
		}
	}

	return nil
}
