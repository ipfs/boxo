package util

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

var cidv0Pref = []byte{0x12, 0x20}

type BytesReader interface {
	io.Reader
	io.ByteReader
}

// TODO: this belongs in the go-cid package
func ReadCid(buf []byte) (cid.Cid, int, error) {
	if bytes.Equal(buf[:2], cidv0Pref) {
		c, err := cid.Cast(buf[:34])
		return c, 34, err
	}

	br := bytes.NewReader(buf)

	// assume cidv1
	vers, err := binary.ReadUvarint(br)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	// TODO: the go-cid package allows version 0 here as well
	if vers != 1 {
		return cid.Cid{}, 0, fmt.Errorf("invalid cid version number")
	}

	codec, err := binary.ReadUvarint(br)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	mhr := mh.NewReader(br)
	h, err := mhr.ReadMultihash()
	if err != nil {
		return cid.Cid{}, 0, err
	}

	return cid.NewCidV1(codec, h), len(buf) - br.Len(), nil
}

func ReadNode(br *bufio.Reader) (cid.Cid, []byte, error) {
	data, err := LdRead(br)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	c, n, err := ReadCid(data)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	return c, data[n:], nil
}

func LdWrite(w io.Writer, d ...[]byte) error {
	var sum uint64
	for _, s := range d {
		sum += uint64(len(s))
	}

	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, sum)
	_, err := w.Write(buf[:n])
	if err != nil {
		return err
	}

	for _, s := range d {
		_, err = w.Write(s)
		if err != nil {
			return err
		}
	}

	return nil
}

func LdSize(d ...[]byte) uint64 {
	var sum uint64
	for _, s := range d {
		sum += uint64(len(s))
	}
	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, sum)
	return sum + uint64(n)
}

func LdRead(r *bufio.Reader) ([]byte, error) {
	l, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return buf, nil
}
