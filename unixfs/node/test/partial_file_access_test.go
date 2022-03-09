package test

import (
	"bytes"
	"context"
	"io"
	"testing"

	u "github.com/ipfs/go-ipfs-util"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipfs/go-unixfsnode/file"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	sb "github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

func TestPartialFileAccess(t *testing.T) {
	buf := make([]byte, 10*1024*1024)
	u.NewSeededRand(0xdeadbeef).Read(buf)
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

	openedLinks := []ipld.Link{}
	ls.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		openedLinks = append(openedLinks, l)
		return storage.OpenRead(lc, l)
	}

	// read back out the file.
	out, err := ufn.AsBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, buf) {
		t.Fatal("Not equal")
	}

	fullLen := len(openedLinks)

	openedLinks = []ipld.Link{}

	partial, err := ufn.(datamodel.LargeBytesNode).AsLargeBytes()
	if err != nil {
		t.Fatal(err)
	}
	half := make([]byte, len(buf)/2)
	if _, err := partial.Read(half); err != nil {
		t.Fatal(err)
	}
	if len(openedLinks) >= fullLen {
		t.Fatal("should not have accessed full file on a partial read.")
	}

	openedLinks = []ipld.Link{}

	prog := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem: ls,
		},
	}
	sb := sb.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	ss := sb.MatcherSubset(5*1024*1024, 6*1024*1024)
	sel, err := ss.Selector()
	if err != nil {
		t.Fatal(err)
	}

	if err := prog.WalkMatching(ufn, sel, func(_ traversal.Progress, n datamodel.Node) error {
		b, err := n.AsBytes()
		if err != nil {
			t.Fatal(err)
		}
		if len(b) != 1024*1024 {
			t.Fatalf("wrong length: %d", len(b))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(openedLinks) >= fullLen {
		t.Fatal("should not have accessed full file on a partial traversal.")
	}
}
