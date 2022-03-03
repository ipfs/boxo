package builder

import (
	"bytes"
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/ipfs/go-unixfsnode/file"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func TestBuildUnixFSFile(t *testing.T) {
	buf := make([]byte, 10*1024*1024)
	u.NewSeededRand(0xdeadbeef).Read(buf)
	r := bytes.NewReader(buf)

	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	f, _, err := BuildUnixFSFile(r, "", &ls)
	if err != nil {
		t.Fatal(err)
	}

	// Note: this differs from the previous
	// go-unixfs version of this test (https://github.com/ipfs/go-unixfs/blob/master/importer/importer_test.go#L50)
	// because this library enforces CidV1 encoding.
	expected, err := cid.Decode("bafybeieyxejezqto5xwcxtvh5tskowwxrn3hmbk3hcgredji3g7abtnfkq")
	if err != nil {
		t.Fatal(err)
	}
	if !expected.Equals(f.(cidlink.Link).Cid) {
		t.Fatalf("expected CID %s, got CID %s", expected, f)
	}
	if _, err := storage.OpenRead(ipld.LinkContext{}, f); err != nil {
		t.Fatal("expected top of file to be in store.")
	}
}

func TestUnixFSFileRoundtrip(t *testing.T) {
	buf := make([]byte, 10*1024*1024)
	u.NewSeededRand(0xdeadbeef).Read(buf)
	r := bytes.NewReader(buf)

	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	f, _, err := BuildUnixFSFile(r, "", &ls)
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
	out, err := ufn.AsBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, buf) {
		t.Fatal("Not equal")
	}
}
