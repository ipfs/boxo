package json

import (
	"encoding/json"

	"github.com/ipfs/boxo/routing/http/types"
)

// ProvidersResponse is the result of a GET Providers request.
type ProvidersResponse struct {
	Providers RecordsArray
}

// PeersResponse is the result of a GET Peers request.
type PeersResponse struct {
	Peers RecordsArray
}

// RecordsArray is an array of [types.Record]
type RecordsArray []types.Record

func (r *RecordsArray) UnmarshalJSON(b []byte) error {
	var tempRecords []json.RawMessage
	err := json.Unmarshal(b, &tempRecords)
	if err != nil {
		return err
	}

	for _, provBytes := range tempRecords {
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
			*r = append(*r, &prov)
		default:
			*r = append(*r, &readProv)
		}

	}
	return nil
}
