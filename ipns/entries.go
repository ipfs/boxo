package ipns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	ipns_pb "github.com/ipfs/boxo/ipns/pb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	mbase "github.com/multiformats/go-multibase"
)

// IpnsInspectEntry contains the deserialized values from an IPNS Entry:
// https://github.com/ipfs/specs/blob/main/ipns/IPNS.md#record-serialization-format
type IpnsInspectEntry struct {
	Value        string
	ValidityType *ipns_pb.IpnsEntry_ValidityType
	Validity     *time.Time
	Sequence     uint64
	TTL          *uint64
	PublicKey    string
	SignatureV1  string
	SignatureV2  string
	Data         interface{}
}

type IpnsInspectValidation struct {
	Valid     bool
	Reason    string
	PublicKey peer.ID
}

func UnmarshalIpnsEntry(data []byte) (*ipns_pb.IpnsEntry, error) {
	var entry ipns_pb.IpnsEntry
	err := proto.Unmarshal(data, &entry)
	if err != nil {
		return nil, err
	}

	return &entry, nil
}

func InspectIpnsRecord(entry *ipns_pb.IpnsEntry) (*IpnsInspectEntry, error) {
	encoder, err := mbase.EncoderByName("base64")
	if err != nil {
		return nil, err
	}

	result := IpnsInspectEntry{
		Value:        string(entry.Value),
		ValidityType: entry.ValidityType,
		Sequence:     *entry.Sequence,
		TTL:          entry.Ttl,
		PublicKey:    encoder.Encode(entry.PubKey),
		SignatureV1:  encoder.Encode(entry.SignatureV1),
		SignatureV2:  encoder.Encode(entry.SignatureV2),
		Data:         nil,
	}

	if len(entry.Data) != 0 {
		// This is hacky. The variable node (datamodel.Node) doesn't directly marshal
		// to JSON. Therefore, we need to first decode from DAG-CBOR, then encode in
		// DAG-JSON and finally unmarshal it from JSON. Since DAG-JSON is a subset
		// of JSON, that should work. Then, we can store the final value in the
		// result.Entry.Data for further inspection.
		node, err := ipld.Decode(entry.Data, dagcbor.Decode)
		if err != nil {
			return nil, err
		}

		var buf bytes.Buffer
		err = dagjson.Encode(node, &buf)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(buf.Bytes(), &result.Data)
		if err != nil {
			return nil, err
		}
	}

	validity, err := GetEOL(entry)
	if err == nil {
		result.Validity = &validity
	}

	return &result, nil
}

func Verify(key string, entry *ipns_pb.IpnsEntry) (*IpnsInspectValidation, error) {
	id, err := peer.Decode(key)
	if err != nil {
		return nil, err
	}

	validation := &IpnsInspectValidation{
		PublicKey: id,
	}

	pub, err := id.ExtractPublicKey()
	if err != nil {
		// Make sure it works with all those RSA that cannot be embedded into the
		// Peer ID.
		if len(entry.PubKey) > 0 {
			pub, err = ic.UnmarshalPublicKey(entry.PubKey)
			if err != nil {
				return nil, err
			}

			// Verify the public key matches the name we are verifying.
			entryID, err := peer.IDFromPublicKey(pub)

			if err != nil {
				return nil, err
			}

			if id != entryID {
				return nil, fmt.Errorf("record public key does not match the verified name")
			}
		}
	}
	if err != nil {
		return nil, err
	}

	err = Validate(pub, entry)
	if err == nil {
		validation.Valid = true
	} else {
		validation.Reason = err.Error()
	}

	return validation, nil
}
