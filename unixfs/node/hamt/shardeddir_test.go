package hamt_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"testing"
	"time"

	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	mdtest "github.com/ipfs/go-merkledag/test"
	ft "github.com/ipfs/go-unixfs"
	legacy "github.com/ipfs/go-unixfs/hamt"
	"github.com/ipfs/go-unixfsnode/hamt"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/stretchr/testify/require"
)

// For now these tests use legacy UnixFS HAMT builders until we finish a builder
// in go-ipld-prime
func shuffle(seed int64, arr []string) {
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < len(arr); i++ {
		a := r.Intn(len(arr))
		b := r.Intn(len(arr))
		arr[a], arr[b] = arr[b], arr[a]
	}
}

func makeDir(ds format.DAGService, size int) ([]string, *legacy.Shard, error) {
	return makeDirWidth(ds, size, 256)
}

func makeDirWidth(ds format.DAGService, size, width int) ([]string, *legacy.Shard, error) {
	ctx := context.Background()

	s, _ := legacy.NewShard(ds, width)

	var dirs []string
	for i := 0; i < size; i++ {
		dirs = append(dirs, fmt.Sprintf("DIRNAME%d", i))
	}

	shuffle(time.Now().UnixNano(), dirs)

	for i := 0; i < len(dirs); i++ {
		nd := ft.EmptyDirNode()
		ds.Add(ctx, nd)
		err := s.Set(ctx, dirs[i], nd)
		if err != nil {
			return nil, nil, err
		}
	}

	return dirs, s, nil
}

func assertLinksEqual(linksA []*format.Link, linksB []*format.Link) error {

	if len(linksA) != len(linksB) {
		return fmt.Errorf("links arrays are different sizes")
	}

	sort.Stable(dag.LinkSlice(linksA))
	sort.Stable(dag.LinkSlice(linksB))
	for i, a := range linksA {
		b := linksB[i]
		if a.Name != b.Name {
			return fmt.Errorf("links names mismatch")
		}

		if a.Cid.String() != b.Cid.String() {
			return fmt.Errorf("link hashes dont match")
		}
	}

	return nil
}

func mockDag() (format.DAGService, *ipld.LinkSystem) {
	bsrv := mdtest.Bserv()
	dsrv := dag.NewDAGService(bsrv)
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}

		blk, err := bsrv.GetBlock(lnkCtx.Ctx, cidLink.Cid)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(blk.RawData()), nil
	}
	lsys.TrustedStorage = true
	return dsrv, &lsys
}

func TestBasicSet(t *testing.T) {
	ds, lsys := mockDag()
	for _, w := range []int{128, 256, 512, 1024, 2048, 4096} {
		t.Run(fmt.Sprintf("BasicSet%d", w), func(t *testing.T) {
			names, s, err := makeDirWidth(ds, 1000, w)
			require.NoError(t, err)
			ctx := context.Background()
			legacyNode, err := s.Node()
			require.NoError(t, err)
			nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: legacyNode.Cid()}, dagpb.Type.PBNode)
			require.NoError(t, err)
			hamtShard, err := hamt.AttemptHAMTShardFromNode(ctx, nd, lsys)
			require.NoError(t, err)
			for _, d := range names {
				_, err := hamtShard.LookupByString(d)
				require.NoError(t, err)
			}
		})
	}
}

func TestIterator(t *testing.T) {
	ds, lsys := mockDag()
	_, s, err := makeDir(ds, 300)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	legacyNode, err := s.Node()
	require.NoError(t, err)
	nds, err := legacy.NewHamtFromDag(ds, legacyNode)
	require.NoError(t, err)
	nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: legacyNode.Cid()}, dagpb.Type.PBNode)
	require.NoError(t, err)
	hamtShard, err := hamt.AttemptHAMTShardFromNode(ctx, nd, lsys)
	require.NoError(t, err)

	linksA, err := nds.EnumLinks(ctx)
	require.NoError(t, err)

	require.Equal(t, int64(len(linksA)), hamtShard.Length())

	linksB := make([]*format.Link, 0, len(linksA))
	iter := hamtShard.Iterator()
	for !iter.Done() {
		name, link := iter.Next()
		linksB = append(linksB, &format.Link{
			Name: name.String(),
			Cid:  link.Link().(cidlink.Link).Cid,
		})
	}
	require.NoError(t, assertLinksEqual(linksA, linksB))
}

func TestLoadFailsFromNonShard(t *testing.T) {
	ds, lsys := mockDag()
	ctx := context.Background()
	legacyNode := ft.EmptyDirNode()
	ds.Add(ctx, legacyNode)
	nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: legacyNode.Cid()}, dagpb.Type.PBNode)
	require.NoError(t, err)
	_, err = hamt.AttemptHAMTShardFromNode(ctx, nd, lsys)
	require.Error(t, err)

	// empty protobuf w/o data
	nd, err = qp.BuildMap(dagpb.Type.PBNode, -1, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "Links", qp.List(-1, func(ipld.ListAssembler) {}))
	})
	require.NoError(t, err)

	_, err = hamt.AttemptHAMTShardFromNode(ctx, nd, lsys)
	require.Error(t, err)
}

func TestFindNonExisting(t *testing.T) {
	ds, lsys := mockDag()
	_, s, err := makeDir(ds, 100)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	legacyNode, err := s.Node()
	require.NoError(t, err)
	nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: legacyNode.Cid()}, dagpb.Type.PBNode)
	require.NoError(t, err)
	hamtShard, err := hamt.AttemptHAMTShardFromNode(ctx, nd, lsys)
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("notfound%d", i)
		_, err := hamtShard.LookupByString(key)
		require.EqualError(t, err, schema.ErrNoSuchField{Field: ipld.PathSegmentOfString(key)}.Error())
	}
}
