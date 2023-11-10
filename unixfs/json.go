package unixfs

import (
	"encoding"
	"errors"
	"fmt"
	"strconv"
)

var _ fmt.Stringer = AliasableString(nil)
var _ encoding.TextMarshaler = AliasableString(nil)
var _ encoding.TextUnmarshaler = (*AliasableString)(nil)

// AliasableString is a byte slice that have string json sementics, allowing to skip allocations while decoding.
type AliasableString []byte

func (s AliasableString) String() string {
	return string(s)
}

func (s AliasableString) MarshalText() ([]byte, error) {
	return s, nil
}

func (s *AliasableString) UnmarshalText(b []byte) error {
	// Sadly we must copy.
	// UnmarshalText must copy the text if it wishes to retain the text after returning.
	new := make([]byte, len(b))
	copy(new, b)
	*s = new
	return nil
}

var _ fmt.Stringer = Type(0)
var _ encoding.TextMarshaler = Type(0)
var _ encoding.TextUnmarshaler = (*Type)(nil)

// Type is an alternative to [Node] which allows for zero-allocation code.
type Type uint8

func (t Type) String() string {
	switch t {
	case TError:
		return "Error"
	case TFile:
		return "File"
	case TDirectory:
		return "Directory"
	case TSymlink:
		return "Symlink"
	default:
		return "error unknown type: " + strconv.FormatUint(uint64(t), 10)
	}
}

var (
	textError     = []byte("Error")
	textFile      = []byte("File")
	textDirectory = []byte("Directory")
	textSymlink   = []byte("Symlink")
)

func (t Type) MarshalText() ([]byte, error) {
	switch t {
	case TError:
		return textError, nil
	case TFile:
		return textFile, nil
	case TDirectory:
		return textDirectory, nil
	case TSymlink:
		return textSymlink, nil
	default:
		return nil, errors.New(t.String())
	}
}

func (t *Type) UnmarshalText(b []byte) error {
	switch string(b) {
	case "Error":
		*t = TError
		return nil
	case "File":
		*t = TFile
		return nil
	case "Directory":
		*t = TDirectory
		return nil
	case "Symlink":
		*t = TSymlink
		return nil
	default:
		return fmt.Errorf("unknown unixfs type: %q", string(b))
	}
}

const (
	// TError is returned when something wrong happend.
	TError Type = iota
	TFile
	TDirectory
	TSymlink
)
