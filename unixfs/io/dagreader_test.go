package io

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	mdag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"

	context "context"

	testu "github.com/ipfs/go-unixfs/test"
)

func TestBasicRead(t *testing.T) {
	dserv := testu.GetDAGServ()
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, testu.UseProtoBufLeaves)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	reader, err := NewDagReader(ctx, node, dserv)
	if err != nil {
		t.Fatal(err)
	}

	outbuf, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeekAndRead(t *testing.T) {
	dserv := testu.GetDAGServ()
	inbuf := make([]byte, 256)
	for i := 0; i <= 255; i++ {
		inbuf[i] = byte(i)
	}

	node := testu.GetNode(t, dserv, inbuf, testu.UseProtoBufLeaves)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	reader, err := NewDagReader(ctx, node, dserv)
	if err != nil {
		t.Fatal(err)
	}

	for i := 255; i >= 0; i-- {
		reader.Seek(int64(i), io.SeekStart)

		if getOffset(reader) != int64(i) {
			t.Fatal("expected offset to be increased by one after read")
		}

		out := readByte(t, reader)

		if int(out) != i {
			t.Fatalf("read %d at index %d, expected %d", out, i, i)
		}

		if getOffset(reader) != int64(i+1) {
			t.Fatal("expected offset to be increased by one after read")
		}
	}
}

func TestSeekWithoutBlocksizes(t *testing.T) {
	dserv := testu.GetDAGServ()
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	inbuf := make([]byte, 1024)

	for i := 0; i < 256; i++ {
		inbuf[i*4] = byte(i)
	}

	inbuf[1023] = 1 // force the reader to be 1024 bytes
	node := testu.GetNode(t, dserv, inbuf, testu.UseProtoBufLeaves)

	// remove the blocksizes
	pbnode := node.Copy().(*mdag.ProtoNode)
	fsnode, err := unixfs.FSNodeFromBytes(pbnode.Data())
	if err != nil {
		t.Fatal(err)
	}
	fsnode.RemoveAllBlockSizes()
	newData, err := fsnode.GetBytes()
	if err != nil {
		t.Fatal(err)
	}
	pbnode.SetData(newData)
	err = dserv.Add(ctx, pbnode)
	if err != nil {
		t.Fatal(err)
	}
	node = pbnode

	reader, err := NewDagReader(ctx, node, dserv)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.Seek(-4, io.SeekEnd)
	if err == nil {
		t.Fatal("seeking shouldn't work without blocksizes")
	}

	_, err = reader.Seek(4, io.SeekStart)
	if err == nil {
		t.Fatal("seeking shouldn't work without blocksizes")
	}

	_, err = reader.Seek(4, io.SeekCurrent)
	if err == nil {
		t.Fatal("seeking shouldn't work without blocksizes")
	}

	// Seeking to the current position or the end should still work.

	_, err = reader.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRelativeSeek(t *testing.T) {
	dserv := testu.GetDAGServ()
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	inbuf := make([]byte, 1024)

	for i := 0; i < 256; i++ {
		inbuf[i*4] = byte(i)
	}

	inbuf[1023] = 1 // force the reader to be 1024 bytes
	node := testu.GetNode(t, dserv, inbuf, testu.UseProtoBufLeaves)

	reader, err := NewDagReader(ctx, node, dserv)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 256; i++ {
		if getOffset(reader) != int64(i*4) {
			t.Fatalf("offset should be %d, was %d", i*4, getOffset(reader))
		}
		out := readByte(t, reader)
		if int(out) != i {
			t.Fatalf("expected to read: %d at %d, read %d", i, getOffset(reader)-1, out)
		}
		if i != 255 {
			_, err := reader.Seek(3, io.SeekCurrent)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	_, err = reader.Seek(-4, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 256; i++ {
		if getOffset(reader) != int64(1020-i*4) {
			t.Fatalf("offset should be %d, was %d", 1020-i*4, getOffset(reader))
		}
		out := readByte(t, reader)
		if int(out) != 255-i {
			t.Fatalf("expected to read: %d at %d, read %d", 255-i, getOffset(reader)-1, out)
		}
		reader.Seek(-5, io.SeekCurrent) // seek 4 bytes but we read one byte every time so 5 bytes
	}

}

func TestTypeFailures(t *testing.T) {
	dserv := testu.GetDAGServ()
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	node := unixfs.EmptyDirNode()
	if _, err := NewDagReader(ctx, node, dserv); err != ErrIsDir {
		t.Fatalf("excepted to get %v, got %v", ErrIsDir, err)
	}

	data, err := unixfs.SymlinkData("/somelink")
	if err != nil {
		t.Fatal(err)
	}
	node = mdag.NodeWithData(data)

	if _, err := NewDagReader(ctx, node, dserv); err != ErrCantReadSymlinks {
		t.Fatalf("excepted to get %v, got %v", ErrCantReadSymlinks, err)
	}
}

func TestBadPBData(t *testing.T) {
	dserv := testu.GetDAGServ()
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	node := mdag.NodeWithData([]byte{42})
	_, err := NewDagReader(ctx, node, dserv)
	if err == nil {
		t.Fatal("excepted error, got nil")
	}
}

func TestMetadataNode(t *testing.T) {
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	dserv := testu.GetDAGServ()
	rdata, rnode := testu.GetRandomNode(t, dserv, 512, testu.UseProtoBufLeaves)
	err := dserv.Add(ctx, rnode)
	if err != nil {
		t.Fatal(err)
	}

	data, err := unixfs.BytesForMetadata(&unixfs.Metadata{
		MimeType: "text",
		Size:     125,
	})
	if err != nil {
		t.Fatal(err)
	}
	node := mdag.NodeWithData(data)

	_, err = NewDagReader(ctx, node, dserv)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "incorrectly formatted") {
		t.Fatal("expected different error")
	}

	node.AddNodeLink("", rnode)

	reader, err := NewDagReader(ctx, node, dserv)
	if err != nil {
		t.Fatal(err)
	}
	readdata, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := testu.ArrComp(rdata, readdata); err != nil {
		t.Fatal(err)
	}
}

func TestWriteTo(t *testing.T) {
	dserv := testu.GetDAGServ()
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, testu.UseProtoBufLeaves)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	reader, err := NewDagReader(ctx, node, dserv)
	if err != nil {
		t.Fatal(err)
	}

	outbuf := new(bytes.Buffer)
	reader.WriteTo(outbuf)

	err = testu.ArrComp(inbuf, outbuf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

}

func TestReaderSzie(t *testing.T) {
	dserv := testu.GetDAGServ()
	size := int64(1024)
	_, node := testu.GetRandomNode(t, dserv, size, testu.UseProtoBufLeaves)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	reader, err := NewDagReader(ctx, node, dserv)
	if err != nil {
		t.Fatal(err)
	}

	if reader.Size() != uint64(size) {
		t.Fatal("wrong reader size")
	}
}

func readByte(t testing.TB, reader DagReader) byte {
	out := make([]byte, 1)
	c, err := reader.Read(out)

	if c != 1 {
		t.Fatal("reader should have read just one byte")
	}
	if err != nil {
		t.Fatal(err)
	}

	return out[0]
}

func getOffset(reader DagReader) int64 {
	offset, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to retrieve offset: " + err.Error())
	}
	return offset
}
