package bitswap

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
	"time"

	tn "github.com/ipfs/go-bitswap/testnet"

	"github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
)

type fetchFunc func(t *testing.T, bs *Bitswap, ks []cid.Cid)

type distFunc func(t *testing.T, provs []Instance, blocks []blocks.Block)

type runStats struct {
	Dups    uint64
	MsgSent uint64
	MsgRecd uint64
	Time    time.Duration
	Name    string
}

var benchmarkLog []runStats

func TestDups2Nodes(t *testing.T) {
	t.Run("AllToAll-OneAtATime", func(t *testing.T) {
		subtestDistributeAndFetch(t, 3, 100, allToAll, oneAtATime)
	})
	t.Run("AllToAll-BigBatch", func(t *testing.T) {
		subtestDistributeAndFetch(t, 3, 100, allToAll, batchFetchAll)
	})

	t.Run("Overlap1-OneAtATime", func(t *testing.T) {
		subtestDistributeAndFetch(t, 3, 100, overlap1, oneAtATime)
	})

	t.Run("Overlap2-BatchBy10", func(t *testing.T) {
		subtestDistributeAndFetch(t, 3, 100, overlap2, batchFetchBy10)
	})

	t.Run("Overlap3-OneAtATime", func(t *testing.T) {
		subtestDistributeAndFetch(t, 3, 100, overlap3, oneAtATime)
	})
	t.Run("Overlap3-BatchBy10", func(t *testing.T) {
		subtestDistributeAndFetch(t, 3, 100, overlap3, batchFetchBy10)
	})
	t.Run("Overlap3-AllConcurrent", func(t *testing.T) {
		subtestDistributeAndFetch(t, 3, 100, overlap3, fetchAllConcurrent)
	})
	t.Run("Overlap3-BigBatch", func(t *testing.T) {
		subtestDistributeAndFetch(t, 3, 100, overlap3, batchFetchAll)
	})
	t.Run("Overlap3-UnixfsFetch", func(t *testing.T) {
		subtestDistributeAndFetch(t, 3, 100, overlap3, unixfsFileFetch)
	})
	t.Run("10Nodes-AllToAll-OneAtATime", func(t *testing.T) {
		subtestDistributeAndFetch(t, 10, 100, allToAll, oneAtATime)
	})
	t.Run("10Nodes-AllToAll-BatchFetchBy10", func(t *testing.T) {
		subtestDistributeAndFetch(t, 10, 100, allToAll, batchFetchBy10)
	})
	t.Run("10Nodes-AllToAll-BigBatch", func(t *testing.T) {
		subtestDistributeAndFetch(t, 10, 100, allToAll, batchFetchAll)
	})
	t.Run("10Nodes-AllToAll-AllConcurrent", func(t *testing.T) {
		subtestDistributeAndFetch(t, 10, 100, allToAll, fetchAllConcurrent)
	})
	t.Run("10Nodes-AllToAll-UnixfsFetch", func(t *testing.T) {
		subtestDistributeAndFetch(t, 10, 100, allToAll, unixfsFileFetch)
	})
	t.Run("10Nodes-OnePeerPerBlock-OneAtATime", func(t *testing.T) {
		subtestDistributeAndFetch(t, 10, 100, onePeerPerBlock, oneAtATime)
	})
	t.Run("10Nodes-OnePeerPerBlock-BigBatch", func(t *testing.T) {
		subtestDistributeAndFetch(t, 10, 100, onePeerPerBlock, batchFetchAll)
	})
	t.Run("10Nodes-OnePeerPerBlock-UnixfsFetch", func(t *testing.T) {
		subtestDistributeAndFetch(t, 10, 100, onePeerPerBlock, unixfsFileFetch)
	})
	t.Run("200Nodes-AllToAll-BigBatch", func(t *testing.T) {
		subtestDistributeAndFetch(t, 200, 20, allToAll, batchFetchAll)
	})

	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	ioutil.WriteFile("benchmark.json", out, 0666)
}

func subtestDistributeAndFetch(t *testing.T, numnodes, numblks int, df distFunc, ff fetchFunc) {
	start := time.Now()
	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(10*time.Millisecond))
	sg := NewTestSessionGenerator(net)
	defer sg.Close()

	bg := blocksutil.NewBlockGenerator()

	instances := sg.Instances(numnodes)
	blocks := bg.Blocks(numblks)

	fetcher := instances[numnodes-1]

	df(t, instances[:numnodes-1], blocks)

	var ks []cid.Cid
	for _, blk := range blocks {
		ks = append(ks, blk.Cid())
	}

	ff(t, fetcher.Exchange, ks)

	st, err := fetcher.Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}

	nst := fetcher.Exchange.network.Stats()
	stats := runStats{
		Time:    time.Now().Sub(start),
		MsgRecd: nst.MessagesRecvd,
		MsgSent: nst.MessagesSent,
		Dups:    st.DupBlksReceived,
		Name:    t.Name(),
	}
	benchmarkLog = append(benchmarkLog, stats)
	t.Logf("send/recv: %d / %d", nst.MessagesSent, nst.MessagesRecvd)
	if st.DupBlksReceived != 0 {
		t.Fatalf("got %d duplicate blocks!", st.DupBlksReceived)
	}
}

func allToAll(t *testing.T, provs []Instance, blocks []blocks.Block) {
	for _, p := range provs {
		if err := p.Blockstore().PutMany(blocks); err != nil {
			t.Fatal(err)
		}
	}
}

// overlap1 gives the first 75 blocks to the first peer, and the last 75 blocks
// to the second peer. This means both peers have the middle 50 blocks
func overlap1(t *testing.T, provs []Instance, blks []blocks.Block) {
	if len(provs) != 2 {
		t.Fatal("overlap1 only works with 2 provs")
	}
	bill := provs[0]
	jeff := provs[1]

	if err := bill.Blockstore().PutMany(blks[:75]); err != nil {
		t.Fatal(err)
	}
	if err := jeff.Blockstore().PutMany(blks[25:]); err != nil {
		t.Fatal(err)
	}
}

// overlap2 gives every even numbered block to the first peer, odd numbered
// blocks to the second.  it also gives every third block to both peers
func overlap2(t *testing.T, provs []Instance, blks []blocks.Block) {
	if len(provs) != 2 {
		t.Fatal("overlap2 only works with 2 provs")
	}
	bill := provs[0]
	jeff := provs[1]

	bill.Blockstore().Put(blks[0])
	jeff.Blockstore().Put(blks[0])
	for i, blk := range blks {
		if i%3 == 0 {
			bill.Blockstore().Put(blk)
			jeff.Blockstore().Put(blk)
		} else if i%2 == 1 {
			bill.Blockstore().Put(blk)
		} else {
			jeff.Blockstore().Put(blk)
		}
	}
}

func overlap3(t *testing.T, provs []Instance, blks []blocks.Block) {
	if len(provs) != 2 {
		t.Fatal("overlap3 only works with 2 provs")
	}

	bill := provs[0]
	jeff := provs[1]

	bill.Blockstore().Put(blks[0])
	jeff.Blockstore().Put(blks[0])
	for i, blk := range blks {
		if i%3 == 0 {
			bill.Blockstore().Put(blk)
			jeff.Blockstore().Put(blk)
		} else if i%2 == 1 {
			bill.Blockstore().Put(blk)
		} else {
			jeff.Blockstore().Put(blk)
		}
	}
}

// onePeerPerBlock picks a random peer to hold each block
// with this layout, we shouldnt actually ever see any duplicate blocks
// but we're mostly just testing performance of the sync algorithm
func onePeerPerBlock(t *testing.T, provs []Instance, blks []blocks.Block) {
	for _, blk := range blks {
		provs[rand.Intn(len(provs))].Blockstore().Put(blk)
	}
}

func oneAtATime(t *testing.T, bs *Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background()).(*Session)
	for _, c := range ks {
		_, err := ses.GetBlock(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("Session fetch latency: %s", ses.latTotal/time.Duration(ses.fetchcnt))
}

// fetch data in batches, 10 at a time
func batchFetchBy10(t *testing.T, bs *Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())
	for i := 0; i < len(ks); i += 10 {
		out, err := ses.GetBlocks(context.Background(), ks[i:i+10])
		if err != nil {
			t.Fatal(err)
		}
		for range out {
		}
	}
}

// fetch each block at the same time concurrently
func fetchAllConcurrent(t *testing.T, bs *Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())

	var wg sync.WaitGroup
	for _, c := range ks {
		wg.Add(1)
		go func(c cid.Cid) {
			defer wg.Done()
			_, err := ses.GetBlock(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
		}(c)
	}
	wg.Wait()
}

func batchFetchAll(t *testing.T, bs *Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())
	out, err := ses.GetBlocks(context.Background(), ks)
	if err != nil {
		t.Fatal(err)
	}
	for range out {
	}
}

// simulates the fetch pattern of trying to sync a unixfs file graph as fast as possible
func unixfsFileFetch(t *testing.T, bs *Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())
	_, err := ses.GetBlock(context.Background(), ks[0])
	if err != nil {
		t.Fatal(err)
	}

	out, err := ses.GetBlocks(context.Background(), ks[1:11])
	if err != nil {
		t.Fatal(err)
	}
	for range out {
	}

	out, err = ses.GetBlocks(context.Background(), ks[11:])
	if err != nil {
		t.Fatal(err)
	}
	for range out {
	}
}
