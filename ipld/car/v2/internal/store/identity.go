package store

import (
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

func IsIdentity(key cid.Cid) (digest []byte, ok bool, err error) {
	dmh, err := multihash.Decode(key.Hash())
	if err != nil {
		return nil, false, err
	}
	ok = dmh.Code == multihash.IDENTITY
	digest = dmh.Digest
	return digest, ok, nil
}
