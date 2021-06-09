package car

const prefixBytesSize = 16
const headerBytesSize = 32

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
		// 128-bit characteristics of this CAR v2 file, such as order, deduplication, etc. Reserved for future use.
		Characteristics Characteristics
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

// Size gets the size of Header in number of bytes.
func (h *Header) Size() int {
	return headerBytesSize
}

// Size gets the size of Characteristics in number of bytes.
func (c *Characteristics) Size() int {
	return prefixBytesSize
}
