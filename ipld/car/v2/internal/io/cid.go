package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

var cidv0Pref = []byte{0x12, 0x20}

func ReadCid(store io.ReaderAt, at int64) (cid.Cid, int, error) {
	var tag [2]byte
	if _, err := store.ReadAt(tag[:], at); err != nil {
		return cid.Undef, 0, err
	}
	if bytes.Equal(tag[:], cidv0Pref) {
		cid0 := make([]byte, 34)
		if _, err := store.ReadAt(cid0, at); err != nil {
			return cid.Undef, 0, err
		}
		c, err := cid.Cast(cid0)
		return c, 34, err
	}

	// assume cidv1
	br := NewOffsetReader(store, at)
	vers, err := binary.ReadUvarint(br)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	// TODO: the go-cid package allows version 0 here as well
	if vers != 1 {
		return cid.Cid{}, 0, fmt.Errorf("invalid cid version number: %d", vers)
	}

	codec, err := binary.ReadUvarint(br)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	mhr := multihash.NewReader(br)
	h, err := mhr.ReadMultihash()
	if err != nil {
		return cid.Cid{}, 0, err
	}

	return cid.NewCidV1(codec, h), int(br.Offset() - at), nil
}
