package json

import (
	"encoding/json"

	"github.com/ipfs/boxo/routing/http/types"
)

// ProvidersResponse is the result of a GET Providers request.
type ProvidersResponse struct {
	Providers []types.Record
}

func (r *ProvidersResponse) UnmarshalJSON(b []byte) error {
	var tempFPR struct{ Providers []json.RawMessage }
	err := json.Unmarshal(b, &tempFPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempFPR.Providers {
		var readProv types.UnknownRecord
		err := json.Unmarshal(provBytes, &readProv)
		if err != nil {
			return err
		}

		switch readProv.Schema {
		case types.SchemaPeer:
			var prov types.PeerRecord
			err := json.Unmarshal(provBytes, &prov)
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
