package car_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"
	sb "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"

	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
)

func TestPrepareTraversal(t *testing.T) {
	from, err := blockstore.OpenReadOnly("testdata/sample-unixfs-v2.car")
	require.NoError(t, err)
	ls := cidlink.DefaultLinkSystem()
	bsa := bsadapter.Adapter{Wrapped: from}
	ls.SetReadStorage(&bsa)

	rts, _ := from.Roots()
	writer, err := car.NewSelectiveWriter(context.Background(), &ls, rts[0], selectorparse.CommonSelector_ExploreAllRecursively)
	require.NoError(t, err)

	buf := bytes.Buffer{}
	n, err := writer.WriteTo(&buf)
	require.NoError(t, err)
	require.Equal(t, int64(len(buf.Bytes())), n)

	fi, _ := os.Stat("testdata/sample-unixfs-v2.car")
	require.Equal(t, fi.Size(), n)

	// Headers should be equal
	h1, _ := car.OpenReader("testdata/sample-unixfs-v2.car")
	h1h := bytes.Buffer{}
	h1h.Write(car.Pragma)
	h1.Header.WriteTo(&h1h)
	require.Equal(t, buf.Bytes()[:h1h.Len()], h1h.Bytes())
}

func TestFileTraversal(t *testing.T) {
	from, err := blockstore.OpenReadOnly("testdata/sample-unixfs-v2.car")
	require.NoError(t, err)
	ls := cidlink.DefaultLinkSystem()
	bsa := bsadapter.Adapter{Wrapped: from}
	ls.SetReadStorage(&bsa)

	rts, _ := from.Roots()
	outDir := t.TempDir()
	err = car.TraverseToFile(context.Background(), &ls, rts[0], selectorparse.CommonSelector_ExploreAllRecursively, path.Join(outDir, "out.car"))
	require.NoError(t, err)

	require.FileExists(t, path.Join(outDir, "out.car"))

	fa, _ := os.Stat("testdata/sample-unixfs-v2.car")
	fb, _ := os.Stat(path.Join(outDir, "out.car"))
	require.Equal(t, fa.Size(), fb.Size())
}

func TestV1Traversal(t *testing.T) {
	from, err := blockstore.OpenReadOnly("testdata/sample-v1.car")
	require.NoError(t, err)
	ls := cidlink.DefaultLinkSystem()
	bsa := bsadapter.Adapter{Wrapped: from}
	ls.SetReadStorage(&bsa)

	rts, _ := from.Roots()
	w := bytes.NewBuffer(nil)
	n, err := car.TraverseV1(context.Background(), &ls, rts[0], selectorparse.CommonSelector_ExploreAllRecursively, w)
	require.NoError(t, err)
	require.Equal(t, int64(len(w.Bytes())), int64(n))

	fa, _ := os.Stat("testdata/sample-v1.car")
	require.Equal(t, fa.Size(), int64(n))
}

func TestPartialTraversal(t *testing.T) {
	store := cidlink.Memory{Bag: make(map[string][]byte)}
	ls := cidlink.DefaultLinkSystem()
	ls.StorageReadOpener = store.OpenRead
	ls.StorageWriteOpener = store.OpenWrite
	unixfsnode.AddUnixFSReificationToLinkSystem(&ls)

	// write a unixfs file.
	initBuf := bytes.Buffer{}
	_, _ = initBuf.Write(make([]byte, 1000000))
	rt, _, err := builder.BuildUnixFSFile(&initBuf, "", &ls)
	require.NoError(t, err)

	// read a subset of the file.
	_, rts, err := cid.CidFromBytes([]byte(rt.Binary()))
	require.NoError(t, err)
	ssb := sb.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreInterpretAs("unixfs", ssb.MatcherSubset(0, 256*1000))
	buf := bytes.Buffer{}
	chooser := dagpb.AddSupportToChooser(func(l datamodel.Link, lc linking.LinkContext) (datamodel.NodePrototype, error) {
		return basicnode.Prototype.Any, nil
	})
	_, err = car.TraverseV1(context.Background(), &ls, rts, sel.Node(), &buf, car.WithTraversalPrototypeChooser(chooser))
	require.NoError(t, err)

	fb := len(buf.Bytes())
	require.Less(t, fb, 1000000)

	loaded, err := car.NewBlockReader(&buf)
	require.NoError(t, err)
	fnd := make(map[cid.Cid]struct{})
	var b blocks.Block
	for err == nil {
		b, err = loaded.Next()
		if err == io.EOF {
			break
		}
		if _, ok := fnd[b.Cid()]; ok {
			require.Fail(t, "duplicate block present", b.Cid())
		}
		fnd[b.Cid()] = struct{}{}
	}
	require.Equal(t, 2, len(fnd))
}
