package rapide_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/ipsl"
	"github.com/ipfs/go-libipfs/ipsl/helpers"
	. "github.com/ipfs/go-libipfs/rapide"
	mh "github.com/multiformats/go-multihash"
)

type mockBlockstore struct {
	t     *testing.T
	delay time.Duration

	m map[cid.Cid][]ipsl.CidTraversalPair
}

func (b *mockBlockstore) makeDag(width, depth uint, i *uint64) cid.Cid {
	if b.m == nil {
		b.m = make(map[cid.Cid][]ipsl.CidTraversalPair)
	}

	var bytes [8]byte
	binary.LittleEndian.PutUint64(bytes[:], *i)
	hash, err := mh.Encode(bytes[:], mh.IDENTITY)
	if err != nil {
		b.t.Fatal(err)
	}
	*i += 1

	var childs []ipsl.CidTraversalPair
	if depth == 0 {
		childs = []ipsl.CidTraversalPair{}
	} else {
		childs = make([]ipsl.CidTraversalPair, width)
		for idx := range childs {
			childs[idx] = ipsl.CidTraversalPair{
				Cid:       b.makeDag(width, depth-1, i),
				Traversal: b,
			}
		}
	}

	c := cid.NewCidV1(cid.Raw, hash)
	b.m[c] = childs

	return c
}

func (bs *mockBlockstore) Traverse(b blocks.Block) ([]ipsl.CidTraversalPair, error) {
	c := b.Cid()
	childrens, ok := bs.m[c]
	if !ok {
		bs.t.Fatalf("Traversed not existing cid: %q", c)
	}

	return childrens, nil
}

func (*mockBlockstore) Serialize() (ipsl.AstNode, []ipsl.BoundScope, error) {
	panic("MOCK!")
}

func (*mockBlockstore) SerializeForNetwork() (ipsl.AstNode, []ipsl.BoundScope, error) {
	panic("MOCK!")
}

func (bs *mockBlockstore) Download(ctx context.Context, root cid.Cid, traversal ipsl.Traversal) (ClosableBlockIterator, error) {
	ctx, cancel := context.WithCancel(ctx)
	r := make(chan blocks.BlockOrError)

	go func() {
		defer close(r)
		helpers.SyncDFS(ctx, root, traversal, bs, math.MaxUint, func(b blocks.Block) error {
			select {
			case r <- blocks.Is(b):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()

	return download{r, cancel, ctx}, nil
}

func (bs *mockBlockstore) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	time.Sleep(bs.delay)

	h := c.Hash()[1:] // skip 0x00 prefix
	_, n := binary.Uvarint(h)
	h = h[n:]
	return blocks.NewBlockWithCid(h, c)
}

func (bs *mockBlockstore) GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {
	r := make(chan blocks.Block, len(ks))
	for _, c := range ks {
		b, err := bs.GetBlock(ctx, c)
		if err != nil {
			break
		}

		r <- b
	}

	return r
}

func (*mockBlockstore) String() string {
	return "mock"
}

type download struct {
	c      <-chan blocks.BlockOrError
	cancel context.CancelFunc
	ctx    context.Context
}

func (d download) Next() (blocks.Block, error) {
	select {
	case <-d.ctx.Done():
		return nil, d.ctx.Err()
	case v := <-d.c:
		return v.Get()
	}
}

func (d download) Close() error {
	d.cancel()
	return nil
}

func TestServerDrivenDownloader(t *testing.T) {
	for _, tc := range [...]struct {
		delay   time.Duration
		runners uint
		width   uint
		depth   uint
	}{
		{0, 1, 2, 2},
		{0, 10, 5, 5},
		{0, 100, 3, 10},
		{time.Nanosecond, 1, 2, 2},
		{time.Nanosecond, 10, 5, 5},
		{time.Nanosecond, 100, 3, 10},
		{time.Microsecond, 1, 2, 2},
		{time.Microsecond, 10, 5, 5},
		{time.Microsecond, 100, 3, 10},
		{time.Millisecond, 1, 2, 2},
		{time.Millisecond, 10, 5, 5},
		{time.Millisecond, 100, 3, 10},
	} {
		t.Run(fmt.Sprintf("%v %v %v %v", tc.delay, tc.runners, tc.width, tc.depth), func(t *testing.T) {
			bs := &mockBlockstore{
				t:     t,
				delay: tc.delay,
			}
			var i uint64
			root := bs.makeDag(tc.width, tc.depth, &i)

			clients := make([]ServerDrivenDownloader, tc.runners)
			for i := tc.runners; i != 0; {
				i--
				clients[i] = bs
			}

			seen := make(map[cid.Cid]struct{})
			for b := range (&Client{ServerDrivenDownloaders: clients}).Get(context.Background(), root, bs) {
				block, err := b.Get()
				if err != nil {
					t.Fatalf("got error from rapide: %s", err)
				}
				c := block.Cid()
				if _, ok := bs.m[c]; !ok {
					t.Fatalf("got cid not in blockstore %s", c)
				}
				seen[c] = struct{}{}
			}

			if len(seen) != len(bs.m) {
				t.Fatalf("seen less blocks than in blockstore: expected %d; got %d", len(bs.m), len(seen))
			}
		})
	}
}
