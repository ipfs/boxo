package resolver_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-multihash"

	merkledag "github.com/ipfs/go-merkledag"
	dagmock "github.com/ipfs/go-merkledag/test"
	path "github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	"github.com/ipfs/go-unixfsnode"
	dagcbor "github.com/ipld/go-ipld-prime/codec/dagcbor"
	dagjson "github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randNode() *merkledag.ProtoNode {
	node := new(merkledag.ProtoNode)
	node.SetData(make([]byte, 32))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Read(node.Data())
	return node
}

func TestRecurivePathResolution(t *testing.T) {
	ctx := context.Background()
	bsrv := dagmock.Bserv()

	a := randNode()
	b := randNode()
	c := randNode()

	err := b.AddNodeLink("grandchild", c)
	if err != nil {
		t.Fatal(err)
	}

	err = a.AddNodeLink("child", b)
	if err != nil {
		t.Fatal(err)
	}

	for _, n := range []*merkledag.ProtoNode{a, b, c} {
		err = bsrv.AddBlock(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
	}

	aKey := a.Cid()

	segments := []string{aKey.String(), "child", "grandchild"}
	p, err := path.FromSegments("/ipfs/", segments...)
	if err != nil {
		t.Fatal(err)
	}

	fetcherFactory := bsfetcher.NewFetcherConfig(bsrv)
	fetcherFactory.NodeReifier = unixfsnode.Reify
	fetcherFactory.PrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})
	resolver := resolver.NewBasicResolver(fetcherFactory)

	node, lnk, err := resolver.ResolvePath(ctx, p)
	if err != nil {
		t.Fatal(err)
	}

	uNode, ok := node.(unixfsnode.PathedPBNode)
	require.True(t, ok)
	fd := uNode.FieldData()
	byts, err := fd.Must().AsBytes()
	require.NoError(t, err)

	assert.Equal(t, cidlink.Link{Cid: c.Cid()}, lnk)

	assert.Equal(t, c.Data(), byts)
	cKey := c.Cid()

	rCid, rest, err := resolver.ResolveToLastNode(ctx, p)
	if err != nil {
		t.Fatal(err)
	}

	if len(rest) != 0 {
		t.Error("expected rest to be empty")
	}

	if rCid.String() != cKey.String() {
		t.Fatal(fmt.Errorf(
			"ResolveToLastNode failed for %s: %s != %s",
			p.String(), rCid.String(), cKey.String()))
	}

	p2, err := path.FromSegments("/ipfs/", aKey.String())
	if err != nil {
		t.Fatal(err)
	}

	rCid, rest, err = resolver.ResolveToLastNode(ctx, p2)
	if err != nil {
		t.Fatal(err)
	}

	if len(rest) != 0 {
		t.Error("expected rest to be empty")
	}

	if rCid.String() != aKey.String() {
		t.Fatal(fmt.Errorf(
			"ResolveToLastNode failed for %s: %s != %s",
			p.String(), rCid.String(), cKey.String()))
	}
}
func TestResolveToLastNode_ErrNoLink(t *testing.T) {
	ctx := context.Background()
	bsrv := dagmock.Bserv()

	a := randNode()
	b := randNode()
	c := randNode()

	err := b.AddNodeLink("grandchild", c)
	if err != nil {
		t.Fatal(err)
	}

	err = a.AddNodeLink("child", b)
	if err != nil {
		t.Fatal(err)
	}

	for _, n := range []*merkledag.ProtoNode{a, b, c} {
		err = bsrv.AddBlock(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
	}

	aKey := a.Cid()

	fetcherFactory := bsfetcher.NewFetcherConfig(bsrv)
	fetcherFactory.PrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})
	fetcherFactory.NodeReifier = unixfsnode.Reify
	r := resolver.NewBasicResolver(fetcherFactory)

	// test missing link intermediate segment
	segments := []string{aKey.String(), "cheese", "time"}
	p, err := path.FromSegments("/ipfs/", segments...)
	require.NoError(t, err)

	_, _, err = r.ResolveToLastNode(ctx, p)
	require.EqualError(t, err, resolver.ErrNoLink{Name: "cheese", Node: aKey}.Error())

	// test missing link at end
	bKey := b.Cid()
	segments = []string{aKey.String(), "child", "apples"}
	p, err = path.FromSegments("/ipfs/", segments...)
	require.NoError(t, err)

	_, _, err = r.ResolveToLastNode(ctx, p)
	require.EqualError(t, err, resolver.ErrNoLink{Name: "apples", Node: bKey}.Error())
}

func TestResolveToLastNode_NoUnnecessaryFetching(t *testing.T) {
	ctx := context.Background()
	bsrv := dagmock.Bserv()

	a := randNode()
	b := randNode()

	err := a.AddNodeLink("child", b)
	require.NoError(t, err)

	err = bsrv.AddBlock(ctx, a)
	require.NoError(t, err)

	aKey := a.Cid()

	segments := []string{aKey.String(), "child"}
	p, err := path.FromSegments("/ipfs/", segments...)
	require.NoError(t, err)

	fetcherFactory := bsfetcher.NewFetcherConfig(bsrv)
	fetcherFactory.PrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})
	fetcherFactory.NodeReifier = unixfsnode.Reify
	resolver := resolver.NewBasicResolver(fetcherFactory)

	resolvedCID, remainingPath, err := resolver.ResolveToLastNode(ctx, p)
	require.NoError(t, err)

	require.Equal(t, len(remainingPath), 0, "cannot have remaining path")
	require.Equal(t, b.Cid(), resolvedCID)
}

func TestPathRemainder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bsrv := dagmock.Bserv()

	nb := basicnode.Prototype.Any.NewBuilder()
	err := dagjson.Decode(nb, strings.NewReader(`{"foo": {"bar": "baz"}}`))
	require.NoError(t, err)
	out := new(bytes.Buffer)
	err = dagcbor.Encode(nb.Build(), out)
	require.NoError(t, err)
	lnk, err := cid.Prefix{
		Version:  1,
		Codec:    cid.DagCBOR,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	}.Sum(out.Bytes())
	require.NoError(t, err)
	blk, err := blocks.NewBlockWithCid(out.Bytes(), lnk)
	require.NoError(t, err)
	bsrv.AddBlock(ctx, blk)
	fetcherFactory := bsfetcher.NewFetcherConfig(bsrv)
	resolver := resolver.NewBasicResolver(fetcherFactory)

	rp1, remainder, err := resolver.ResolveToLastNode(ctx, path.FromString(lnk.String()+"/foo/bar"))
	require.NoError(t, err)

	assert.Equal(t, lnk, rp1)
	require.Equal(t, "foo/bar", path.Join(remainder))
}

func TestResolveToLastNode_MixedSegmentTypes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bsrv := dagmock.Bserv()
	a := randNode()
	err := bsrv.AddBlock(ctx, a)
	if err != nil {
		t.Fatal(err)
	}

	nb := basicnode.Prototype.Any.NewBuilder()
	json := `{"foo":{"bar":[0,{"boom":["baz",1,2,{"/":"CID"},"blop"]}]}}`
	json = strings.ReplaceAll(json, "CID", a.Cid().String())
	err = dagjson.Decode(nb, strings.NewReader(json))
	require.NoError(t, err)
	out := new(bytes.Buffer)
	err = dagcbor.Encode(nb.Build(), out)
	require.NoError(t, err)
	lnk, err := cid.Prefix{
		Version:  1,
		Codec:    cid.DagCBOR,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	}.Sum(out.Bytes())
	require.NoError(t, err)
	blk, err := blocks.NewBlockWithCid(out.Bytes(), lnk)
	require.NoError(t, err)
	bsrv.AddBlock(ctx, blk)
	fetcherFactory := bsfetcher.NewFetcherConfig(bsrv)
	resolver := resolver.NewBasicResolver(fetcherFactory)

	cid, remainder, err := resolver.ResolveToLastNode(ctx, path.FromString(lnk.String()+"/foo/bar/1/boom/3"))
	require.NoError(t, err)

	assert.Equal(t, 0, len(remainder))
	assert.True(t, cid.Equals(a.Cid()))
}
