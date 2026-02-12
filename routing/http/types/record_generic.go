package types

import (
	"encoding/json"

	"github.com/ipfs/boxo/routing/http/internal/drjson"
)

const SchemaGeneric = "generic"

// MaxGenericRecordSize is the maximum serialized size of a single generic
// record, as specified by IPIP-518.
const MaxGenericRecordSize = 10 << 10 // 10 KiB

var _ Record = (*GenericRecord)(nil)

// GenericRecord is a duck-typed record that can hold both multiaddrs and URIs
// in its Addrs field. It is introduced by IPIP-518 as the successor to
// PeerRecord for providers that expose non-libp2p transports.
type GenericRecord struct {
	Schema    string
	ID        string
	Addrs     Addresses
	Protocols []string

	// Extra contains extra fields that were included in the original JSON raw
	// message, except for the known ones represented by the remaining fields.
	Extra map[string]json.RawMessage
}

func (gr *GenericRecord) GetSchema() string {
	return gr.Schema
}

func (gr *GenericRecord) UnmarshalJSON(b []byte) error {
	// Unmarshal all known fields and assign them.
	v := struct {
		Schema    string
		ID        string
		Addrs     Addresses
		Protocols []string
	}{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	gr.Schema = v.Schema
	gr.ID = v.ID
	gr.Addrs = v.Addrs
	gr.Protocols = v.Protocols

	// Unmarshal everything into the Extra field and remove the
	// known fields to avoid conflictual usages of the struct.
	err = json.Unmarshal(b, &gr.Extra)
	if err != nil {
		return err
	}
	delete(gr.Extra, "Schema")
	delete(gr.Extra, "ID")
	delete(gr.Extra, "Addrs")
	delete(gr.Extra, "Protocols")

	return nil
}

func (gr GenericRecord) MarshalJSON() ([]byte, error) {
	m := map[string]any{}
	if gr.Extra != nil {
		for key, val := range gr.Extra {
			m[key] = val
		}
	}

	// Schema and ID must always be set.
	m["Schema"] = gr.Schema
	m["ID"] = gr.ID

	if gr.Addrs != nil {
		m["Addrs"] = gr.Addrs
	}

	if gr.Protocols != nil {
		m["Protocols"] = gr.Protocols
	}

	return drjson.MarshalJSONBytes(m)
}
