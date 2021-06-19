package data_test

// adapted from https://github.com/ipfs/js-ipfs-unixfs/blob/master/packages/ipfs-unixfs/test/unixfs-format.spec.js

import (
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strconv"
	"testing"
	"time"

	. "github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/stretchr/testify/require"
)

func loadFixture(name string) []byte {
	_, filename, _, _ := runtime.Caller(0)

	f, err := os.Open(path.Join(path.Dir(filename), "fixtures", name))
	if err != nil {
		panic(err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	return data
}

var raw = loadFixture("raw.unixfs")
var directory = loadFixture("directory.unixfs")
var file = loadFixture("file.txt.unixfs")
var symlink = loadFixture("symlink.txt.unixfs")

func TestUnixfsFormat(t *testing.T) {
	t.Run("defaults to file", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(*builder.Builder) {})
		require.NoError(t, err)
		require.Equal(t, Data_File, data.FieldDataType().Int())
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)
		require.Equal(t, unmarshaled.FieldDataType(), data.FieldDataType())
		require.Equal(t, unmarshaled.FieldData(), data.FieldData())
		require.Equal(t, unmarshaled.FieldBlockSizes(), data.FieldBlockSizes())
		require.Equal(t, unmarshaled.FieldFileSize(), data.FieldFileSize())
	})

	t.Run("raw", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_Raw)
			builder.Data(b, []byte("bananas"))
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)
		require.Equal(t, unmarshaled.FieldDataType(), data.FieldDataType())
		require.Equal(t, unmarshaled.FieldData(), data.FieldData())
		require.Equal(t, unmarshaled.FieldBlockSizes(), data.FieldBlockSizes())
		require.Equal(t, unmarshaled.FieldFileSize(), data.FieldFileSize())
	})

	t.Run("directory", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_Directory)
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)
		require.Equal(t, unmarshaled.FieldDataType(), data.FieldDataType())
		require.Equal(t, unmarshaled.FieldData(), data.FieldData())
		require.Equal(t, unmarshaled.FieldBlockSizes(), data.FieldBlockSizes())
		require.Equal(t, unmarshaled.FieldFileSize(), data.FieldFileSize())
	})

	t.Run("HAMTShard", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_HAMTShard)
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)
		require.Equal(t, unmarshaled.FieldDataType(), data.FieldDataType())
		require.Equal(t, unmarshaled.FieldData(), data.FieldData())
		require.Equal(t, unmarshaled.FieldBlockSizes(), data.FieldBlockSizes())
		require.Equal(t, unmarshaled.FieldFileSize(), data.FieldFileSize())
	})

	t.Run("file", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_File)
			builder.Data(b, []byte("batata"))
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)
		require.Equal(t, unmarshaled.FieldDataType(), data.FieldDataType())
		require.Equal(t, unmarshaled.FieldData(), data.FieldData())
		require.Equal(t, unmarshaled.FieldBlockSizes(), data.FieldBlockSizes())
		require.Equal(t, unmarshaled.FieldFileSize(), data.FieldFileSize())
	})

	t.Run("file add blocksize", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_File)
			builder.BlockSizes(b, []uint64{256})
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)
		require.Equal(t, unmarshaled.FieldDataType(), data.FieldDataType())
		require.Equal(t, unmarshaled.FieldData(), data.FieldData())
		require.Equal(t, unmarshaled.FieldBlockSizes(), data.FieldBlockSizes())
		require.Equal(t, unmarshaled.FieldFileSize(), data.FieldFileSize())
	})

	t.Run("mode", func(t *testing.T) {
		mode, err := strconv.ParseInt("0555", 8, 32)
		require.NoError(t, err)
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_File)
			builder.Permissions(b, int(mode))
		})
		require.NoError(t, err)
		unmarshaled, err := DecodeUnixFSData(EncodeUnixFSData(data))
		require.NoError(t, err)
		require.True(t, unmarshaled.FieldMode().Exists())
		require.Equal(t, mode, unmarshaled.FieldMode().Must().Int())
	})

	t.Run("default mode for files", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_File)
		})
		require.NoError(t, err)
		unmarshaled, err := DecodeUnixFSData(EncodeUnixFSData(data))
		require.NoError(t, err)

		require.Equal(t, 0o0644, unmarshaled.Permissions())
	})

	t.Run("default mode for directories", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_Directory)
		})
		require.NoError(t, err)
		unmarshaled, err := DecodeUnixFSData(EncodeUnixFSData(data))
		require.NoError(t, err)

		require.Equal(t, 0o0755, unmarshaled.Permissions())
	})

	t.Run("default mode for hamt shards", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_HAMTShard)
		})
		require.NoError(t, err)
		unmarshaled, err := DecodeUnixFSData(EncodeUnixFSData(data))
		require.NoError(t, err)

		require.Equal(t, 0o0755, unmarshaled.Permissions())
	})

	t.Run("mode as string", func(t *testing.T) {
		mode, err := strconv.ParseInt("0555", 8, 32)
		require.NoError(t, err)
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_File)
			builder.PermissionsString(b, "0555")
		})
		require.NoError(t, err)
		unmarshaled, err := DecodeUnixFSData(EncodeUnixFSData(data))
		require.NoError(t, err)
		require.True(t, unmarshaled.FieldMode().Exists())
		require.Equal(t, mode, unmarshaled.FieldMode().Must().Int())
	})

	t.Run("mtime", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_File)
			builder.Mtime(b, func(tb builder.TimeBuilder) {
				builder.Seconds(tb, 5)
			})
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)
		require.Equal(t, unmarshaled.FieldMtime(), data.FieldMtime())
	})

	t.Run("mtime from time.Time", func(t *testing.T) {
		now := time.Now()
		seconds := now.Unix()
		nanosecond := now.Nanosecond()

		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_File)
			builder.Mtime(b, func(tb builder.TimeBuilder) {
				builder.Time(tb, now)
			})
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)
		require.True(t, unmarshaled.FieldMtime().Exists())
		mtime := unmarshaled.FieldMtime().Must()
		require.Equal(t, seconds, mtime.FieldSeconds().Int())
		require.True(t, mtime.FieldFractionalNanoseconds().Exists())
		require.Equal(t, int64(nanosecond), mtime.FieldFractionalNanoseconds().Must().Int())
	})

	t.Run("omits default file mode from protobuf", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_File)
			builder.Permissions(b, 0o0644)
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)

		require.False(t, unmarshaled.FieldMode().Exists())
		require.Equal(t, 0o644, unmarshaled.Permissions())

	})

	t.Run("omits default directory mode from protobuf", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_Directory)
			builder.Permissions(b, 0o0755)
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)

		require.False(t, unmarshaled.FieldMode().Exists())
		require.Equal(t, 0o0755, unmarshaled.Permissions())
	})

	t.Run("respects high bits in mode read from buffer", func(t *testing.T) {
		mode := 0o0100644 // similar to output from fs.stat
		nd, err := qp.BuildMap(Type.UnixFSData, -1, func(ma ipld.MapAssembler) {
			qp.MapEntry(ma, Field__DataType, qp.Int(Data_File))
			qp.MapEntry(ma, Field__BlockSizes, qp.List(0, func(ipld.ListAssembler) {}))
			qp.MapEntry(ma, Field__Mode, qp.Int(int64(mode)))
		})
		require.NoError(t, err)
		und, ok := nd.(UnixFSData)
		require.True(t, ok)

		marshaled := EncodeUnixFSData(und)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)
		require.Equal(t, 0o0644, unmarshaled.Permissions())
		require.True(t, unmarshaled.FieldMode().Exists())
		require.Equal(t, int64(mode), unmarshaled.FieldMode().Must().Int())

	})

	t.Run("ignores high bits in mode passed to constructor", func(t *testing.T) {
		mode := 0o0100644 // similar to output from fs.stat
		entry, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_File)
			builder.Permissions(b, mode)
		})
		require.NoError(t, err)
		require.True(t, entry.FieldMode().Exists())
		require.Equal(t, 0o644, entry.Permissions())
		// should have truncated mode to bits in the version of the spec this module supports

		marshaled := EncodeUnixFSData(entry)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)

		require.False(t, unmarshaled.FieldMode().Exists())
		require.Equal(t, 0o644, unmarshaled.Permissions())
	})

	// figuring out what is this metadata for https://github.com/ipfs/js-ipfs-data-importing/issues/3#issuecomment-182336526
	t.Run("metadata", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_Metadata)
		})
		require.NoError(t, err)

		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)

		require.Equal(t, Data_Metadata, unmarshaled.FieldDataType().Int())
	})

	t.Run("symlink", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_Symlink)
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)
		unmarshaled, err := DecodeUnixFSData(marshaled)
		require.NoError(t, err)
		require.Equal(t, unmarshaled.FieldDataType(), data.FieldDataType())
		require.Equal(t, unmarshaled.FieldData(), data.FieldData())
		require.Equal(t, unmarshaled.FieldBlockSizes(), data.FieldBlockSizes())
		require.Equal(t, unmarshaled.FieldFileSize(), data.FieldFileSize())
	})

	t.Run("invalid type", func(t *testing.T) {
		_, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, 9999)
		})
		require.EqualError(t, err, "type: 9999 is not valid")
	})
}

func TestInterop(t *testing.T) {
	t.Run("raw", func(t *testing.T) {
		unmarshaled, err := DecodeUnixFSData(raw)
		require.NoError(t, err)

		require.True(t, unmarshaled.FieldData().Exists())
		require.Equal(t, []byte("Hello UnixFS\n"), unmarshaled.FieldData().Must().Bytes())
		require.Equal(t, Data_File, unmarshaled.FieldDataType().Int())
		require.Equal(t, raw, EncodeUnixFSData(unmarshaled))
	})

	t.Run("directory", func(t *testing.T) {
		unmarshaled, err := DecodeUnixFSData(directory)
		require.NoError(t, err)

		require.False(t, unmarshaled.FieldData().Exists())
		require.Equal(t, Data_Directory, unmarshaled.FieldDataType().Int())
		require.Equal(t, directory, EncodeUnixFSData(unmarshaled))
	})

	t.Run("file", func(t *testing.T) {
		unmarshaled, err := DecodeUnixFSData(file)
		require.NoError(t, err)

		require.True(t, unmarshaled.FieldData().Exists())
		require.Equal(t, []byte("Hello UnixFS\n"), unmarshaled.FieldData().Must().Bytes())
		require.Equal(t, Data_File, unmarshaled.FieldDataType().Int())
		require.Equal(t, file, EncodeUnixFSData(unmarshaled))
	})

	t.Run("symlink", func(t *testing.T) {
		unmarshaled, err := DecodeUnixFSData(symlink)
		require.NoError(t, err)

		require.Equal(t, []byte("file.txt"), unmarshaled.FieldData().Must().Bytes())
		require.Equal(t, Data_Symlink, unmarshaled.FieldDataType().Int())
		require.Equal(t, symlink, EncodeUnixFSData(unmarshaled))
	})

	t.Run("empty", func(t *testing.T) {
		data, err := builder.BuildUnixFS(func(b *builder.Builder) {
			builder.DataType(b, Data_File)
		})
		require.NoError(t, err)
		marshaled := EncodeUnixFSData(data)

		require.Equal(t, []byte{0x08, 0x02}, marshaled)
	})
}
