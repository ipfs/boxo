package util

import (
	"bufio"
	"io"

	"github.com/multiformats/go-varint"

	cid "github.com/ipfs/go-cid"
)

type BytesReader interface {
	io.Reader
	io.ByteReader
}

func ReadNode(br *bufio.Reader) (cid.Cid, []byte, error) {
	data, err := LdRead(br)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	n, c, err := cid.CidFromBytes(data)
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
	n := varint.PutUvarint(buf, sum)
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
	s := varint.UvarintSize(sum)
	return sum + uint64(s)
}

func LdRead(r *bufio.Reader) ([]byte, error) {
	if _, err := r.Peek(1); err != nil { // no more blocks, likely clean io.EOF
		return nil, err
	}

	l, err := varint.ReadUvarint(r)
	if err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF // don't silently pretend this is a clean EOF
		}
		return nil, err
	}

	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return buf, nil
}
