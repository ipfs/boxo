package types

import (
	"encoding/json"

	"github.com/ipfs/go-libipfs/routing/http/internal/drjson"
)

var _ ReadProviderRecord = &UnknownProviderRecord{}
var _ WriteProviderRecord = &UnknownProviderRecord{}
var _ ProviderResponse = &UnknownProviderRecord{}

// UnknownProviderRecord is used when we cannot parse the provider record using `GetProtocol`
type UnknownProviderRecord struct {
	Protocol string
	Schema   string
	Bytes    []byte
}

func (u *UnknownProviderRecord) GetProtocol() string {
	return u.Protocol
}

func (u *UnknownProviderRecord) GetSchema() string {
	return u.Schema
}

func (u *UnknownProviderRecord) IsReadProviderRecord() {}
func (u UnknownProviderRecord) IsWriteProviderRecord() {}

func (u *UnknownProviderRecord) UnmarshalJSON(b []byte) error {
	m := map[string]interface{}{}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	ps, ok := m["Protocol"].(string)
	if ok {
		u.Protocol = ps
	}
	schema, ok := m["Schema"].(string)
	if ok {
		u.Schema = schema
	}

	u.Bytes = b

	return nil
}

func (u UnknownProviderRecord) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	err := json.Unmarshal(u.Bytes, &m)
	if err != nil {
		return nil, err
	}
	m["Protocol"] = u.Protocol
	m["Schema"] = u.Schema

	return drjson.MarshalJSONBytes(m)
}
