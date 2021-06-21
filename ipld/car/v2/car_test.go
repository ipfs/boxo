package car_test

import (
	"bufio"
	"bytes"
	"testing"

	carv1 "github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/assert"
)

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
	v1h, err := carv1.ReadHeader(bufio.NewReader(bytes.NewReader(carv2.PrefixBytes)))
	assert.NoError(t, err, "cannot decode prefix as CBOR with CAR v1 header structure")
	assert.Equal(t, &carv1.CarHeader{
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
			assert.EqualValues(t, carv2.HeaderSize, uint64(len(gotWrite)), "WriteTo() CAR v2 header length must always be %v bytes long", carv2.HeaderSize)
			assert.EqualValues(t, carv2.HeaderSize, uint64(written), "WriteTo() CAR v2 header byte count must always be %v bytes long", carv2.HeaderSize)
		})
	}
}

func TestHeader_ReadFrom(t *testing.T) {
	tests := []struct {
		name       string
		target     []byte
		wantHeader carv2.Header
		wantErr    bool
	}{
		{
			"HeaderWithEmptyCharacteristicsIsWrittenAsExpected",
			[]byte{
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			},
			carv2.Header{
				Characteristics: carv2.Characteristics{},
			},
			false,
		},
		{
			"NonEmptyHeaderIsWrittenAsExpected",

			[]byte{
				0xe9, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0xea, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x63, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x64, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x65, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			},
			carv2.Header{
				Characteristics: carv2.Characteristics{
					Hi: 1001, Lo: 1002,
				},
				CarV1Offset: 99,
				CarV1Size:   100,
				IndexOffset: 101,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHeader := carv2.Header{}
			gotRead, err := gotHeader.ReadFrom(bytes.NewReader(tt.target))
			assert.NoError(t, err)
			assert.Equal(t, int64(carv2.HeaderSize), gotRead)
			assert.Equal(t, tt.wantHeader, gotHeader)
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
			carv2.PrefixSize + carv2.HeaderSize,
			carv2.PrefixSize + carv2.HeaderSize + 123,
		},
		{
			"WhenOnlyPaddingCarV1BothOffsetsShift",
			carv2.NewHeader(123).WithCarV1Padding(3),
			carv2.PrefixSize + carv2.HeaderSize + 3,
			carv2.PrefixSize + carv2.HeaderSize + 3 + 123,
		},
		{
			"WhenPaddingBothCarV1AndIndexBothOffsetsShiftWithAdditionalIndexShift",
			carv2.NewHeader(123).WithCarV1Padding(3).WithIndexPadding(7),
			carv2.PrefixSize + carv2.HeaderSize + 3,
			carv2.PrefixSize + carv2.HeaderSize + 3 + 123 + 7,
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
		CarV1Offset:     carv2.PrefixSize + carv2.HeaderSize,
		CarV1Size:       wantCarV1Len,
		IndexOffset:     carv2.PrefixSize + carv2.HeaderSize + wantCarV1Len,
	}
	got := carv2.NewHeader(wantCarV1Len)
	assert.Equal(t, want, got, "NewHeader got = %v, want = %v", got, want)
}
