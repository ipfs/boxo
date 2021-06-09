package car

import (
	"encoding/binary"
	"io"
)

const (
	// HeaderBytesSize is the fixed size of CAR v2 header in number of bytes.
	HeaderBytesSize uint64 = 40
	// CharacteristicsBytesSize is the fixed size of Characteristics bitfield within CAR v2 header in number of bytes.
	CharacteristicsBytesSize uint64 = 16
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
	// The size of the CAR v2 prefix in 11 bytes, (i.e. 11).
	PrefixBytesSize = uint64(len(PrefixBytes))
	// Reserved 128 bits space to capture future characteristics of CAR v2 such as order, duplication, etc.
	EmptyCharacteristics = new(Characteristics)
)

type (
	// Header represents the CAR v2 header/pragma.
	Header struct {
		io.WriterTo
		// 128-bit characteristics of this CAR v2 file, such as order, deduplication, etc. Reserved for future use.
		Characteristics *Characteristics
		// The offset from the beginning of the file at which the dump of CAR v1 starts.
		CarV1Offset uint64
		// The size of CAR v1 encapsulated in this CAR v2 as bytes.
		CarV1Size uint64
		// The offset from the beginning of the file at which the CAR v2 index begins.
		IndexOffset uint64
	}
	// Characteristics is a bitfield placeholder for capturing the characteristics of a CAR v2 such as order and determinism.
	Characteristics struct {
		io.WriterTo
		Hi uint64
		Lo uint64
	}
)

// WriteTo writes this characteristics to the given writer.
func (c *Characteristics) WriteTo(w io.Writer) (n int64, err error) {
	wn, err := writeUint64To(w, c.Hi)
	if err != nil {
		return
	}
	n += wn
	wn, err = writeUint64To(w, c.Lo)
	if err != nil {
		return
	}
	n += wn
	return
}

// Size gets the size of Characteristics in number of bytes.
func (c *Characteristics) Size() uint64 {
	return CharacteristicsBytesSize
}

// NewHeader instantiates a new CAR v2 header, given the byte length of a CAR v1.
func NewHeader(carV1Size uint64) *Header {
	header := &Header{
		Characteristics: EmptyCharacteristics,
		CarV1Size:       carV1Size,
	}
	header.CarV1Offset = PrefixBytesSize + HeaderBytesSize
	header.IndexOffset = header.CarV1Offset + carV1Size
	return header
}

// Size gets the size of Header in number of bytes.
func (h *Header) Size() uint64 {
	return HeaderBytesSize
}

// WithIndexPadding sets the index offset from the beginning of the file for this header and returns the
// header for convenient chained calls.
// The index offset is calculated as the sum of PrefixBytesLen, HeaderBytesLen,
// Header#CarV1Len, and the given padding.
func (h *Header) WithIndexPadding(padding uint64) *Header {
	h.IndexOffset = h.IndexOffset + padding
	return h
}

// WithCarV1Padding sets the CAR v1 dump offset from the beginning of the file for this header and returns the
// header for convenient chained calls.
// The CAR v1 offset is calculated as the sum of PrefixBytesLen, HeaderBytesLen and the given padding.
// The call to this function also shifts the Header#IndexOffset forward by the given padding.
func (h *Header) WithCarV1Padding(padding uint64) *Header {
	h.CarV1Offset = h.CarV1Offset + padding
	h.IndexOffset = h.IndexOffset + padding
	return h
}

// WriteTo serializes this header as bytes and writes them using the given io.Writer.
func (h *Header) WriteTo(w io.Writer) (n int64, err error) {
	chars := h.Characteristics
	if chars == nil {
		chars = EmptyCharacteristics
	}
	wn, err := chars.WriteTo(w)
	if err != nil {
		return
	}
	n += wn
	wn, err = writeUint64To(w, h.CarV1Offset)
	if err != nil {
		return
	}
	n += wn
	wn, err = writeUint64To(w, h.CarV1Size)
	if err != nil {
		return
	}
	n += wn
	wn, err = writeUint64To(w, h.IndexOffset)
	if err != nil {
		return
	}
	n += wn
	return
}

func writeUint64To(w io.Writer, v uint64) (n int64, err error) {
	err = binary.Write(w, binary.LittleEndian, v)
	if err == nil {
		n = 8
	}
	return
}
