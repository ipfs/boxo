package data

import (
	"errors"
	"fmt"
	"math"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"google.golang.org/protobuf/encoding/protowire"
)

func DecodeUnixFSData(src []byte) (UnixFSData, error) {
	nd, err := qp.BuildMap(Type.UnixFSData, -1, func(ma ipld.MapAssembler) {
		err := consumeUnixFSData(src, ma)
		if err != nil {
			panic(err)
		}
	})
	if err != nil {
		return nil, err
	}
	return nd.(UnixFSData), nil
}

func consumeUnixFSData(remaining []byte, ma ipld.MapAssembler) error {
	var bsa ipld.NodeBuilder
	var la ipld.ListAssembler
	var packedBlockSizes bool
	for {
		if len(remaining) == 0 {
			break
		}

		fieldNum, wireType, n := protowire.ConsumeTag(remaining)
		if n < 0 {
			return protowire.ParseError(n)
		}
		remaining = remaining[n:]
		switch fieldNum {
		case Data_DataTypeWireNum:
			if wireType != protowire.VarintType {
				return fmt.Errorf("protobuf: (UnixFSData) invalid wireType, field: DataType, expected %d, got %d", protowire.VarintType, wireType)
			}
			dataType, n := protowire.ConsumeVarint(remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
			qp.MapEntry(ma, "DataType", qp.Int(int64(dataType)))
		case Data_DataWireNum:
			if wireType != protowire.BytesType {
				return fmt.Errorf("protobuf: (UnixFSData) invalid wireType, field: Data, expected %d, got %d", protowire.VarintType, wireType)
			}
			data, n := protowire.ConsumeBytes(remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
			qp.MapEntry(ma, "Data", qp.Bytes(data))
		case Data_FileSizeWireNum:
			if wireType != protowire.VarintType {
				return fmt.Errorf("protobuf: (UnixFSData) invalid wireType, field: FileSize, expected %d, got %d", protowire.VarintType, wireType)
			}
			fileSize, n := protowire.ConsumeVarint(remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
			qp.MapEntry(ma, "FileSize", qp.Int(int64(fileSize)))
		case Data_BlockSizesWireNum:
			switch wireType {
			case protowire.VarintType:
				if packedBlockSizes {
					return errors.New("cannot build blocksizes twice")
				}
				if la == nil {
					bsa = Type.BlockSizes.NewBuilder()
					var err error
					la, err = bsa.BeginList(1)
					if err != nil {
						return err
					}
				}
				blockSize, n := protowire.ConsumeVarint(remaining)
				if n < 0 {
					return protowire.ParseError(n)
				}
				remaining = remaining[n:]
				qp.ListEntry(la, qp.Int(int64(blockSize)))
			case protowire.BytesType:
				if la != nil {
					return errors.New("cannot build blocksizes twice")
				}
				blockSizesBytes, n := protowire.ConsumeBytes(remaining)
				if n < 0 {
					return protowire.ParseError(n)
				}
				remaining = remaining[n:]
				// count the number of varints in the array by looking at most
				// significant bit not set
				var blockSizeCount int64
				for _, integer := range blockSizesBytes {
					if integer < 128 {
						blockSizeCount++
					}
				}
				qp.MapEntry(ma, "BlockSizes", qp.List(blockSizeCount, func(la ipld.ListAssembler) {
					err := consumeBlockSizes(blockSizesBytes, blockSizeCount, la)
					if err != nil {
						panic(err)
					}
				}))
			default:
				return fmt.Errorf("protobuf: (UnixFSData) invalid wireType, field: BlockSizes, got %d", wireType)
			}
		case Data_HashTypeWireNum:
			if wireType != protowire.VarintType {
				return fmt.Errorf("protobuf: (UnixFSData) invalid wireType, field: HashType, expected %d, got %d", protowire.VarintType, wireType)
			}
			hashType, n := protowire.ConsumeVarint(remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
			qp.MapEntry(ma, "HashType", qp.Int(int64(hashType)))
		case Data_FanoutWireNum:
			if wireType != protowire.VarintType {
				return fmt.Errorf("protobuf: (UnixFSData) invalid wireType, field: Fanout, expected %d, got %d", protowire.VarintType, wireType)
			}
			fanout, n := protowire.ConsumeVarint(remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
			qp.MapEntry(ma, "Fanout", qp.Int(int64(fanout)))
		case Data_ModeWireNum:
			if wireType != protowire.VarintType {
				return fmt.Errorf("protobuf: (UnixFSData) invalid wireType, field: Mode, expected %d, got %d", protowire.VarintType, wireType)
			}
			mode, n := protowire.ConsumeVarint(remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			if mode > math.MaxUint32 {
				return errors.New("mode should be a 32 bit value")
			}
			remaining = remaining[n:]
			qp.MapEntry(ma, "Mode", qp.Int(int64(mode)))
		case Data_MTimeWireNum:
			if wireType != protowire.BytesType {
				return fmt.Errorf("protobuf: (UnixFSData) invalid wireType, field: Mtime, expected %d, got %d", protowire.BytesType, wireType)
			}
			mTimeBytes, n := protowire.ConsumeBytes(remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
			qp.MapEntry(ma, "Mtime", qp.Map(-1, func(ma ipld.MapAssembler) {
				err := consumeUnixTime(mTimeBytes, ma)
				if err != nil {
					panic(err)
				}
			}))
		default:
			n := protowire.ConsumeFieldValue(fieldNum, wireType, remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
		}
	}
	if !packedBlockSizes {
		if la == nil {
			qp.MapEntry(ma, "BlockSizes", qp.List(0, func(ipld.ListAssembler) {}))
		} else {
			err := la.Finish()
			if err != nil {
				return err
			}
			nd := bsa.Build()
			qp.MapEntry(ma, "BlockSizes", qp.Node(nd))
		}
	}
	return nil
}

func consumeBlockSizes(remaining []byte, count int64, la ipld.ListAssembler) error {
	for i := 0; i < int(count); i++ {
		blockSize, n := protowire.ConsumeVarint(remaining)
		if n < 0 {
			return protowire.ParseError(n)
		}
		remaining = remaining[n:]
		qp.ListEntry(la, qp.Int(int64(blockSize)))
	}
	if len(remaining) > 0 {
		return errors.New("did not consume all block sizes")
	}
	return nil
}

func consumeUnixTime(remaining []byte, ma ipld.MapAssembler) error {
	for {
		if len(remaining) == 0 {
			break
		}

		fieldNum, wireType, n := protowire.ConsumeTag(remaining)
		if n < 0 {
			return protowire.ParseError(n)
		}
		remaining = remaining[n:]

		switch fieldNum {
		case UnixTime_SecondsWireNum:
			if wireType != protowire.VarintType {
				return fmt.Errorf("protobuf: (UnixTime) invalid wireType, field: Seconds, expected %d, got %d", protowire.VarintType, wireType)
			}
			seconds, n := protowire.ConsumeVarint(remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
			qp.MapEntry(ma, "Seconds", qp.Int(int64(seconds)))
		case UnixTime_FractionalNanosecondsWireNum:
			if wireType != protowire.Fixed32Type {
				return fmt.Errorf("protobuf: (UnixTime) invalid wireType, field: FractionalNanoseconds, expected %d, got %d", protowire.Fixed32Type, wireType)
			}
			fractionalNanoseconds, n := protowire.ConsumeFixed32(remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
			qp.MapEntry(ma, "FractionalNanoseconds", qp.Int(int64(fractionalNanoseconds)))
		default:
			n := protowire.ConsumeFieldValue(fieldNum, wireType, remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
		}
	}
	return nil
}
func DecodeUnixTime(src []byte) (UnixTime, error) {
	nd, err := qp.BuildMap(Type.UnixTime, -1, func(ma ipld.MapAssembler) {
		err := consumeUnixTime(src, ma)
		if err != nil {
			panic(err)
		}
	})
	if err != nil {
		return nil, err
	}
	return nd.(UnixTime), err
}

func DecodeUnixFSMetadata(src []byte) (UnixFSMetadata, error) {
	nd, err := qp.BuildMap(Type.UnixFSMetadata, -1, func(ma ipld.MapAssembler) {
		err := consumeUnixFSMetadata(src, ma)
		if err != nil {
			panic(err)
		}
	})
	if err != nil {
		return nil, err
	}
	return nd.(UnixFSMetadata), nil
}

func consumeUnixFSMetadata(remaining []byte, ma ipld.MapAssembler) error {
	for {
		if len(remaining) == 0 {
			break
		}

		fieldNum, wireType, n := protowire.ConsumeTag(remaining)
		if n < 0 {
			return protowire.ParseError(n)
		}
		remaining = remaining[n:]

		switch fieldNum {
		case Metadata_MimeTypeWireNum:
			if wireType != protowire.BytesType {
				return fmt.Errorf("protobuf: (UnixFSMetadata) invalid wireType, field: MimeType, expected %d, got %d", protowire.VarintType, wireType)
			}
			mimeTypeBytes, n := protowire.ConsumeBytes(remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
			qp.MapEntry(ma, "MimeType", qp.String(string(mimeTypeBytes)))
		default:
			n := protowire.ConsumeFieldValue(fieldNum, wireType, remaining)
			if n < 0 {
				return protowire.ParseError(n)
			}
			remaining = remaining[n:]
		}
	}
	return nil
}
