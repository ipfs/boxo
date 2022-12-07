package types

import (
	"encoding/json"
)

// WriteProviderRecord is a type that enforces structs to imlement it to avoid confusion
type WriteProviderRecord interface {
	IsWriteProviderRecord()
}

// ReadProviderRecord is a type that enforces structs to imlement it to avoid confusion
type ReadProviderRecord interface {
	IsReadProviderRecord()
}

type WriteProvidersRequest struct {
	Providers []WriteProviderRecord
}

func (r *WriteProvidersRequest) UnmarshalJSON(b []byte) error {
	type wpr struct {
		Providers []json.RawMessage
	}
	var tempWPR wpr
	err := json.Unmarshal(b, &tempWPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempWPR.Providers {
		var rawProv UnknownProviderRecord
		err := json.Unmarshal(provBytes, &rawProv)
		if err != nil {
			return err
		}

		switch rawProv.Schema {
		case SchemaBitswap:
			var prov WriteBitswapProviderRecord
			err := json.Unmarshal(rawProv.Bytes, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		default:
			var prov UnknownProviderRecord
			err := json.Unmarshal(b, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		}
	}
	return nil
}

// ProviderResponse is implemented for any ProviderResponse. It needs to have a Protocol field.
type ProviderResponse interface {
	GetProtocol() string
	GetSchema() string
}

// WriteProvidersResponse is the result of a Provide operation
type WriteProvidersResponse struct {
	ProvideResults []ProviderResponse
}

// rawWriteProvidersResponse is a helper struct to make possible to parse WriteProvidersResponse's
type rawWriteProvidersResponse struct {
	ProvideResults []json.RawMessage
}

func (r *WriteProvidersResponse) UnmarshalJSON(b []byte) error {
	var tempWPR rawWriteProvidersResponse
	err := json.Unmarshal(b, &tempWPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempWPR.ProvideResults {
		var rawProv UnknownProviderRecord
		err := json.Unmarshal(provBytes, &rawProv)
		if err != nil {
			return err
		}

		switch rawProv.Schema {
		case SchemaBitswap:
			var prov WriteBitswapProviderRecordResponse
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

// ReadProvidersResponse is the result of a Provide request
type ReadProvidersResponse struct {
	Providers []ProviderResponse
}

// rawReadProvidersResponse is a helper struct to make possible to parse ReadProvidersResponse's
type rawReadProvidersResponse struct {
	Providers []json.RawMessage
}

func (r *ReadProvidersResponse) UnmarshalJSON(b []byte) error {
	var tempFPR rawReadProvidersResponse
	err := json.Unmarshal(b, &tempFPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempFPR.Providers {
		var readProv UnknownProviderRecord
		err := json.Unmarshal(provBytes, &readProv)
		if err != nil {
			return err
		}

		switch readProv.Schema {
		case SchemaBitswap:
			var prov ReadBitswapProviderRecord
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
