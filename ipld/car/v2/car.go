package car

import (
	"encoding/binary"
	"io"
)

const (
	// PrefixSize is the size of the CAR v2 prefix in 11 bytes, (i.e. 11).
	PrefixSize = 11
	// HeaderSize is the fixed size of CAR v2 header in number of bytes.
	HeaderSize = 40
	// CharacteristicsSize is the fixed size of Characteristics bitfield within CAR v2 header in number of bytes.
	CharacteristicsSize = 16
	uint64Size          = 8
)

var (
	// The fixed prefix of a CAR v2, signalling the version number to previous versions for graceful fail over.
	PrefixBytes = []byte{
		0x0a,                                     // unit(10)
		0xa1,                                     // map(1)
		0x67,                                     // string(7)
		0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, // "version"
		0x02, // uint(2)
	}
)

type (
	// Header represents the CAR v2 header/pragma.
	Header struct {
		// 128-bit characteristics of this CAR v2 file, such as order, deduplication, etc. Reserved for future use.
		Characteristics Characteristics
		// The offset from the beginning of the file at which the dump of CAR v1 starts.
		CarV1Offset uint64
		// The size of CAR v1 encapsulated in this CAR v2 as bytes.
		CarV1Size uint64
		// The offset from the beginning of the file at which the CAR v2 index begins.
		IndexOffset uint64
	}
	// Characteristics is a bitfield placeholder for capturing the characteristics of a CAR v2 such as order and determinism.
	Characteristics struct {
		Hi uint64
		Lo uint64
	}
)

// WriteTo writes this characteristics to the given writer.
func (c Characteristics) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.LittleEndian, c.Hi); err != nil {
		return
	}
	n += uint64Size
	if err = binary.Write(w, binary.LittleEndian, c.Lo); err != nil {
		return
	}
	n += uint64Size
	return
}

func (c *Characteristics) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, CharacteristicsSize)
	read, err := io.ReadFull(r, buf)
	n := int64(read)
	if err != nil {
		return n, err
	}
	c.Hi = binary.LittleEndian.Uint64(buf[:uint64Size])
	c.Lo = binary.LittleEndian.Uint64(buf[uint64Size:])
	return n, nil
}

// NewHeader instantiates a new CAR v2 header, given the byte length of a CAR v1.
func NewHeader(carV1Size uint64) Header {
	header := Header{
		CarV1Size: carV1Size,
	}
	header.CarV1Offset = PrefixSize + HeaderSize
	header.IndexOffset = header.CarV1Offset + carV1Size
	return header
}

// WithIndexPadding sets the index offset from the beginning of the file for this header and returns the
// header for convenient chained calls.
// The index offset is calculated as the sum of PrefixBytesLen, HeaderBytesLen,
// Header.CarV1Len, and the given padding.
func (h Header) WithIndexPadding(padding uint64) Header {
	h.IndexOffset = h.IndexOffset + padding
	return h
}

// WithCarV1Padding sets the CAR v1 dump offset from the beginning of the file for this header and returns the
// header for convenient chained calls.
// The CAR v1 offset is calculated as the sum of PrefixBytesLen, HeaderBytesLen and the given padding.
// The call to this function also shifts the Header.IndexOffset forward by the given padding.
func (h Header) WithCarV1Padding(padding uint64) Header {
	h.CarV1Offset = h.CarV1Offset + padding
	h.IndexOffset = h.IndexOffset + padding
	return h
}

// WriteTo serializes this header as bytes and writes them using the given io.Writer.
func (h Header) WriteTo(w io.Writer) (n int64, err error) {
	// TODO optimize write by encoding all bytes in a slice and writing once.
	wn, err := h.Characteristics.WriteTo(w)
	if err != nil {
		return
	}
	n += wn
	if err = binary.Write(w, binary.LittleEndian, h.CarV1Offset); err != nil {
		return
	}
	n += uint64Size
	if err = binary.Write(w, binary.LittleEndian, h.CarV1Size); err != nil {
		return
	}
	n += uint64Size
	if err = binary.Write(w, binary.LittleEndian, h.IndexOffset); err != nil {
		return
	}
	n += uint64Size
	return
}

// ReadFrom populates fields of this header from the given r.
func (h *Header) ReadFrom(r io.Reader) (int64, error) {
	n, err := h.Characteristics.ReadFrom(r)
	if err != nil {
		return n, err
	}
	remainingSize := HeaderSize - CharacteristicsSize
	buf := make([]byte, remainingSize)
	read, err := io.ReadFull(r, buf)
	n += int64(read)
	if err != nil {
		return n, err
	}
	carV1RelOffset := uint64Size * 2
	h.CarV1Offset = binary.LittleEndian.Uint64(buf[:uint64Size])
	h.CarV1Size = binary.LittleEndian.Uint64(buf[uint64Size:carV1RelOffset])
	h.IndexOffset = binary.LittleEndian.Uint64(buf[carV1RelOffset:])
	return n, nil
}
