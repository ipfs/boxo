// Package ipns implements IPNS record creation, marshaling, and validation as
// specified in the [IPNS Record] specification.
//
// # Records
//
// An IPNS [Record] maps a [Name] to a content path, with a sequence number,
// expiration time, and TTL. Records are signed with the private key
// corresponding to the name.
//
//	record, err := ipns.NewRecord(privateKey, path, seq, eol, ttl)
//	if err != nil {
//	    // handle error
//	}
//
//	data, err := ipns.MarshalRecord(record)
//
// Records can be deserialized and validated:
//
//	record, err := ipns.UnmarshalRecord(data)
//	err = ipns.ValidateWithName(record, name)
//
// # Names
//
// A [Name] is a multihash of a serialized public key. Names can be created
// from peer IDs or strings:
//
//	name := ipns.NameFromPeer(peerID)
//	name, err := ipns.NameFromString("k51...")
//
// [IPNS Record]: https://specs.ipfs.tech/ipns/ipns-record/
package ipns
