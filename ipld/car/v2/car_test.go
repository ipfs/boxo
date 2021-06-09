package car_test

import (
	"bytes"
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
			car_v2.CharacteristicsBytesSize,
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

func TestHeader_SizeIs40Bytes(t *testing.T) {
	assert.Equal(t, uint64(40), new(car_v2.Header).Size())
}

func TestHeader_Marshal(t *testing.T) {
	tests := []struct {
		name        string
		target      car_v2.Header
		wantMarshal []byte
		wantErr     bool
	}{
		{
			"header with nil characteristics is marshalled as empty characteristics",
			car_v2.Header{
				Characteristics: nil,
			},
			[]byte{
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			},
			false,
		},
		{
			"header with empty characteristics is marshalled as expected",
			car_v2.Header{
				Characteristics: car_v2.EmptyCharacteristics,
			},
			[]byte{
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			},
			false,
		},
		{
			"non-empty header is marshalled as expected",
			car_v2.Header{
				Characteristics: &car_v2.Characteristics{
					Hi: 1001, Lo: 1002,
				},
				CarV1Offset: 99,
				CarV1Size:   100,
				IndexOffset: 101,
			},
			[]byte{
				0xe9, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0xea, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x63, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x64, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x65, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			written, err := tt.target.WriteTo(buf)
			if (err != nil) != tt.wantErr {
				t.Errorf("Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotMarshal := buf.Bytes()
			assert.Equal(t, tt.wantMarshal, gotMarshal, "Header.WriteTo() gotMarshal = %v, wantMarshal %v", gotMarshal, tt.wantMarshal)
			assert.EqualValues(t, car_v2.HeaderBytesSize, uint64(len(gotMarshal)), "WriteTo() CAR v2 header length must always be %v bytes long", car_v2.HeaderBytesSize)
			assert.EqualValues(t, car_v2.HeaderBytesSize, uint64(written), "WriteTo() CAR v2 header byte count must always be %v bytes long", car_v2.HeaderBytesSize)
		})
	}
}

func TestHeader_WithPadding(t *testing.T) {
	tests := []struct {
		name            string
		subject         *car_v2.Header
		wantCarV1Offset uint64
		wantIndexOffset uint64
	}{
		{
			"when no padding, offsets are sum of sizes",
			car_v2.NewHeader(123),
			car_v2.PrefixBytesSize + car_v2.HeaderBytesSize,
			car_v2.PrefixBytesSize + car_v2.HeaderBytesSize + 123,
		},
		{
			"when only padding car v1, both offsets shift",
			car_v2.NewHeader(123).WithCarV1Padding(3),
			car_v2.PrefixBytesSize + car_v2.HeaderBytesSize + 3,
			car_v2.PrefixBytesSize + car_v2.HeaderBytesSize + 3 + 123,
		},
		{
			"when padding both car v1 and index, both offsets shift with additional index shift",
			car_v2.NewHeader(123).WithCarV1Padding(3).WithIndexPadding(7),
			car_v2.PrefixBytesSize + car_v2.HeaderBytesSize + 3,
			car_v2.PrefixBytesSize + car_v2.HeaderBytesSize + 3 + 123 + 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.EqualValues(t, tt.wantCarV1Offset, tt.subject.CarV1Offset)
			assert.EqualValues(t, tt.wantIndexOffset, tt.subject.IndexOffset)
		})
	}
}

func TestNewHeaderHasExpectedValues(t *testing.T) {
	wantCarV1Len := uint64(1413)
	want := &car_v2.Header{
		Characteristics: car_v2.EmptyCharacteristics,
		CarV1Offset:     car_v2.PrefixBytesSize + car_v2.HeaderBytesSize,
		CarV1Size:       wantCarV1Len,
		IndexOffset:     car_v2.PrefixBytesSize + car_v2.HeaderBytesSize + wantCarV1Len,
	}
	got := car_v2.NewHeader(wantCarV1Len)
	assert.Equal(t, want, got, "NewHeader got = %v, want = %v", got, want)
}
