package car_test

import (
	cbor "github.com/ipfs/go-ipld-cbor"
	car_v1 "github.com/ipld/go-car"
	car_v2 "github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCarV2PrefixLength(t *testing.T) {
	tests := []struct {
		name string
		want interface{}
		got  interface{}
	}{
		{
			"cached size should be 11 bytes",
			11,
			car_v2.PrefixBytesSize,
		},
		{
			"actual size should be 11 bytes",
			11,
			len(car_v2.PrefixBytes),
		},
		{
			"should start with varint(10)",
			car_v2.PrefixBytes[0],
			10,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			assert.EqualValues(t, tt.want, tt.got, "CarV2Prefix got = %v, want %v", tt.got, tt.want)
		})
	}
}

func TestCarV2PrefixIsValidCarV1Header(t *testing.T) {
	var v1h car_v1.CarHeader
	err := cbor.DecodeInto(car_v2.PrefixBytes[1:], &v1h)
	assert.NoError(t, err, "cannot decode prefix as CBOR with CAR v1 header structure")
	assert.Equal(t, car_v1.CarHeader{
		Roots:   nil,
		Version: 2,
	}, v1h, "CAR v2 prefix must be a valid CAR v1 header")
}

func TestEmptyCharacteristics(t *testing.T) {
	tests := []struct {
		name string
		want interface{}
		got  interface{}
	}{
		{
			"is of size 16 bytes",
			16,
			car_v2.EmptyCharacteristics.Size(),
		},
		{
			"is a whole lot of nothin'",
			&car_v2.Characteristics{},
			car_v2.EmptyCharacteristics,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			assert.EqualValues(t, tt.want, tt.got, "EmptyCharacteristics got = %v, want %v", tt.got, tt.want)
		})
	}
}

func TestHeader_SizeIs32Bytes(t *testing.T) {
	assert.Equal(t, 32, new(car_v2.Header).Size())
}
