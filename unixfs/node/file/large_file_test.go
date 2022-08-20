//go:build !race
// +build !race

package file_test

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"testing"

	ipfsutil "github.com/ipfs/go-ipfs-util"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipfs/go-unixfsnode/file"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func TestLargeFileReader(t *testing.T) {
	if testing.Short() || strconv.IntSize == 32 {
		t.Skip()
	}
	buf := make([]byte, 512*1024*1024)
	ipfsutil.NewSeededRand(0xdeadbeef).Read(buf)
	r := bytes.NewReader(buf)

	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	f, _, err := builder.BuildUnixFSFile(r, "", &ls)
	if err != nil {
		t.Fatal(err)
	}

	// get back the root node substrate from the link at the top of the builder.
	fr, err := ls.Load(ipld.LinkContext{}, f, dagpb.Type.PBNode)
	if err != nil {
		t.Fatal(err)
	}

	ufn, err := file.NewUnixFSFile(context.Background(), fr, &ls)
	if err != nil {
		t.Fatal(err)
	}
	// read back out the file.
	for i := 0; i < len(buf); i += 100 * 1024 * 1024 {
		rs, err := ufn.AsLargeBytes()
		if err != nil {
			t.Fatal(err)
		}
		_, err = rs.Seek(int64(i), io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		ob, err := io.ReadAll(rs)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ob, buf[i:]) {
			t.Fatal("Not equal at offset", i, "expected", len(buf[i:]), "got", len(ob))
		}
	}
}
