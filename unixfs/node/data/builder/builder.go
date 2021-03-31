package builder

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
)

func BuildUnixFs(fn func(*Builder)) (data.UnixFSData, error) {
	nd, err := qp.BuildMap(data.Type.UnixFSData, -1, func(ma ipld.MapAssembler) {
		b := &Builder{MapAssembler: ma}
		fn(b)
		if !b.hasBlockSizes {
			qp.MapEntry(ma, "BlockSizes", qp.List(0, func(ipld.ListAssembler) {}))
		}
		if !b.hasDataType {
			qp.MapEntry(ma, "DataType", qp.Int(data.Data_File))
		}
	})
	if err != nil {
		return nil, err
	}
	return nd.(data.UnixFSData), nil
}

type Builder struct {
	ipld.MapAssembler
	hasDataType   bool
	hasBlockSizes bool
}

func DataType(b *Builder, dataType int64) {
	_, ok := data.DataTypeNames[dataType]
	if !ok {
		panic(fmt.Errorf("Type: %d is not valid", dataType))
	}
	qp.MapEntry(b.MapAssembler, "DataType", qp.Int(dataType))
	b.hasDataType = true
}

func Data(b *Builder, data []byte) {
	qp.MapEntry(b.MapAssembler, "Data", qp.Bytes(data))

}

func FileSize(b *Builder, fileSize uint64) {
	qp.MapEntry(b.MapAssembler, "FileSize", qp.Int(int64(fileSize)))
}

func BlockSizes(b *Builder, blockSizes []uint64) {
	qp.MapEntry(b.MapAssembler, "BlockSizes", qp.List(int64(len(blockSizes)), func(la ipld.ListAssembler) {
		for _, bs := range blockSizes {
			qp.ListEntry(la, qp.Int(int64(bs)))
		}
	}))
	b.hasBlockSizes = true
}

func HashFunc(b *Builder, hashFunc uint64) {
	qp.MapEntry(b.MapAssembler, "HashFunc", qp.Int(int64(hashFunc)))
}

func Fanout(b *Builder, fanout uint64) {
	qp.MapEntry(b.MapAssembler, "Fanout", qp.Int(int64(fanout)))
}

func Permissions(b *Builder, mode int) {
	mode = mode & 0xFFF
	qp.MapEntry(b.MapAssembler, "Mode", qp.Int(int64(mode)))
}

func parseModeString(modeString string) (uint64, error) {
	if len(modeString) > 0 && modeString[0] == '0' {
		return strconv.ParseUint(modeString, 8, 32)
	}
	return strconv.ParseUint(modeString, 10, 32)
}

func PermissionsString(b *Builder, modeString string) {
	mode64, err := parseModeString(modeString)
	if err != nil {
		panic(err)
	}
	mode64 = mode64 & 0xFFF
	qp.MapEntry(b.MapAssembler, "Mode", qp.Int(int64(mode64)))
}

func Mtime(b *Builder, fn func(tb TimeBuilder)) {
	qp.MapEntry(b.MapAssembler, "Mtime", qp.Map(-1, func(ma ipld.MapAssembler) {
		fn(ma)
	}))
}

type TimeBuilder ipld.MapAssembler

func Time(ma TimeBuilder, t time.Time) {
	Seconds(ma, t.Unix())
	FractionalNanoseconds(ma, int32(t.Nanosecond()))
}

func Seconds(ma TimeBuilder, seconds int64) {
	qp.MapEntry(ma, "Seconds", qp.Int(seconds))

}

func FractionalNanoseconds(ma TimeBuilder, nanoseconds int32) {
	if nanoseconds < 0 || nanoseconds > 999999999 {
		panic(errors.New("mtime-nsecs must be within the range [0,999999999]"))
	}
	qp.MapEntry(ma, "FractionalNanoseconds", qp.Int(int64(nanoseconds)))
}
