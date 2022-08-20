package file_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"

	ipfsutil "github.com/ipfs/go-ipfs-util"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipfs/go-unixfsnode/directory"
	"github.com/ipfs/go-unixfsnode/file"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func TestRootV0File(t *testing.T) {
	baseFile := "./fixtures/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o.car"
	root, ls := open(baseFile, t)
	file, err := file.NewUnixFSFile(context.Background(), root, ls)
	if err != nil {
		t.Fatal(err)
	}
	fc, err := file.AsBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fc, []byte("hello world\n")) {
		t.Errorf("file content does not match: %s", string(fc))
	}
}

func TestNamedV0File(t *testing.T) {
	baseFile := "./fixtures/QmT8EC9sJq63SkDZ1mWLbWWyVV66PuqyHWpKkH4pcVyY4H.car"
	root, ls := open(baseFile, t)
	dir, err := unixfsnode.Reify(ipld.LinkContext{}, root, ls)
	if err != nil {
		t.Fatal(err)
	}
	dpbn := dir.(directory.UnixFSBasicDir)
	name, link := dpbn.Iterator().Next()
	if name.String() != "b.txt" {
		t.Fatal("unexpected filename")
	}
	fileNode, err := ls.Load(ipld.LinkContext{}, link.Link(), dagpb.Type.PBNode)
	if err != nil {
		t.Fatal(err)
	}
	file, err := file.NewUnixFSFile(context.Background(), fileNode, ls)
	if err != nil {
		t.Fatal(err)
	}
	fc, err := file.AsBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fc, []byte("hello world\n")) {
		t.Errorf("file content does not match: %s", string(fc))
	}
}

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

func open(car string, t *testing.T) (ipld.Node, *ipld.LinkSystem) {
	baseStore, err := blockstore.OpenReadOnly(car)
	if err != nil {
		t.Fatal(err)
	}
	ls := cidlink.DefaultLinkSystem()
	ls.StorageReadOpener = func(lctx ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("couldn't load link")
		}
		blk, err := baseStore.Get(lctx.Ctx, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk.RawData()), nil
	}
	carRoots, err := baseStore.Roots()
	if err != nil {
		t.Fatal(err)
	}
	root, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: carRoots[0]}, dagpb.Type.PBNode)
	if err != nil {
		t.Fatal(err)
	}
	return root, &ls
}
