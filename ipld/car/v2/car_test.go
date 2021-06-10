package car_test

import (
	"bytes"
	cbor "github.com/ipfs/go-ipld-cbor"
	carv1 "github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

var prefixBytesSize = uint64(11)

func TestCarV2PrefixLength(t *testing.T) {
	tests := []struct {
		name string
		want interface{}
		got  interface{}
	}{
		{
			"ActualSizeShouldBe11",
			11,
			len(carv2.PrefixBytes),
		},
		{
			"ShouldStartWithVarint(10)",
			carv2.PrefixBytes[0],
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
	var v1h carv1.CarHeader
	err := cbor.DecodeInto(carv2.PrefixBytes[1:], &v1h)
	assert.NoError(t, err, "cannot decode prefix as CBOR with CAR v1 header structure")
	assert.Equal(t, carv1.CarHeader{
		Roots:   nil,
		Version: 2,
	}, v1h, "CAR v2 prefix must be a valid CAR v1 header")
}

func TestHeader_WriteTo(t *testing.T) {
	tests := []struct {
		name      string
		target    carv2.Header
		wantWrite []byte
		wantErr   bool
	}{
		{
			"HeaderWithEmptyCharacteristicsIsWrittenAsExpected",
			carv2.Header{
				Characteristics: carv2.Characteristics{},
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
			"NonEmptyHeaderIsWrittenAsExpected",
			carv2.Header{
				Characteristics: carv2.Characteristics{
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
				t.Errorf("WriteTo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotWrite := buf.Bytes()
			assert.Equal(t, tt.wantWrite, gotWrite, "Header.WriteTo() gotWrite = %v, wantWrite %v", gotWrite, tt.wantWrite)
			assert.EqualValues(t, carv2.HeaderBytesSize, uint64(len(gotWrite)), "WriteTo() CAR v2 header length must always be %v bytes long", carv2.HeaderBytesSize)
			assert.EqualValues(t, carv2.HeaderBytesSize, uint64(written), "WriteTo() CAR v2 header byte count must always be %v bytes long", carv2.HeaderBytesSize)
		})
	}
}

func TestHeader_WithPadding(t *testing.T) {
	tests := []struct {
		name            string
		subject         carv2.Header
		wantCarV1Offset uint64
		wantIndexOffset uint64
	}{
		{
			"WhenNoPaddingOffsetsAreSumOfSizes",
			carv2.NewHeader(123),
			prefixBytesSize + carv2.HeaderBytesSize,
			prefixBytesSize + carv2.HeaderBytesSize + 123,
		},
		{
			"WhenOnlyPaddingCarV1BothOffsetsShift",
			carv2.NewHeader(123).WithCarV1Padding(3),
			prefixBytesSize + carv2.HeaderBytesSize + 3,
			prefixBytesSize + carv2.HeaderBytesSize + 3 + 123,
		},
		{
			"WhenPaddingBothCarV1AndIndexBothOffsetsShiftWithAdditionalIndexShift",
			carv2.NewHeader(123).WithCarV1Padding(3).WithIndexPadding(7),
			prefixBytesSize + carv2.HeaderBytesSize + 3,
			prefixBytesSize + carv2.HeaderBytesSize + 3 + 123 + 7,
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
	want := carv2.Header{
		Characteristics: carv2.Characteristics{},
		CarV1Offset:     prefixBytesSize + carv2.HeaderBytesSize,
		CarV1Size:       wantCarV1Len,
		IndexOffset:     prefixBytesSize + carv2.HeaderBytesSize + wantCarV1Len,
	}
	got := carv2.NewHeader(wantCarV1Len)
	assert.Equal(t, want, got, "NewHeader got = %v, want = %v", got, want)
}
