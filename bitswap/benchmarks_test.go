package bitswap

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"

	bssession "github.com/ipfs/go-bitswap/session"
	tn "github.com/ipfs/go-bitswap/testnet"
	"github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
)

type fetchFunc func(b *testing.B, bs *Bitswap, ks []cid.Cid)

type distFunc func(b *testing.B, provs []Instance, blocks []blocks.Block)

type runStats struct {
	Dups    uint64
	MsgSent uint64
	MsgRecd uint64
	Time    time.Duration
	Name    string
}

var benchmarkLog []runStats

func BenchmarkDups2Nodes(b *testing.B) {
	benchmarkLog = nil
	fixedDelay := delay.Fixed(10 * time.Millisecond)
	b.Run("AllToAll-OneAtATime", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, allToAll, oneAtATime)
	})
	b.Run("AllToAll-BigBatch", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, allToAll, batchFetchAll)
	})

	b.Run("Overlap1-OneAtATime", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap1, oneAtATime)
	})

	b.Run("Overlap2-BatchBy10", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap2, batchFetchBy10)
	})

	b.Run("Overlap3-OneAtATime", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap3, oneAtATime)
	})
	b.Run("Overlap3-BatchBy10", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap3, batchFetchBy10)
	})
	b.Run("Overlap3-AllConcurrent", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap3, fetchAllConcurrent)
	})
	b.Run("Overlap3-BigBatch", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap3, batchFetchAll)
	})
	b.Run("Overlap3-UnixfsFetch", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap3, unixfsFileFetch)
	})
	b.Run("10Nodes-AllToAll-OneAtATime", func(b *testing.B) {
		subtestDistributeAndFetch(b, 10, 100, fixedDelay, allToAll, oneAtATime)
	})
	b.Run("10Nodes-AllToAll-BatchFetchBy10", func(b *testing.B) {
		subtestDistributeAndFetch(b, 10, 100, fixedDelay, allToAll, batchFetchBy10)
	})
	b.Run("10Nodes-AllToAll-BigBatch", func(b *testing.B) {
		subtestDistributeAndFetch(b, 10, 100, fixedDelay, allToAll, batchFetchAll)
	})
	b.Run("10Nodes-AllToAll-AllConcurrent", func(b *testing.B) {
		subtestDistributeAndFetch(b, 10, 100, fixedDelay, allToAll, fetchAllConcurrent)
	})
	b.Run("10Nodes-AllToAll-UnixfsFetch", func(b *testing.B) {
		subtestDistributeAndFetch(b, 10, 100, fixedDelay, allToAll, unixfsFileFetch)
	})
	b.Run("10Nodes-OnePeerPerBlock-OneAtATime", func(b *testing.B) {
		subtestDistributeAndFetch(b, 10, 100, fixedDelay, onePeerPerBlock, oneAtATime)
	})
	b.Run("10Nodes-OnePeerPerBlock-BigBatch", func(b *testing.B) {
		subtestDistributeAndFetch(b, 10, 100, fixedDelay, onePeerPerBlock, batchFetchAll)
	})
	b.Run("10Nodes-OnePeerPerBlock-UnixfsFetch", func(b *testing.B) {
		subtestDistributeAndFetch(b, 10, 100, fixedDelay, onePeerPerBlock, unixfsFileFetch)
	})
	b.Run("200Nodes-AllToAll-BigBatch", func(b *testing.B) {
		subtestDistributeAndFetch(b, 200, 20, fixedDelay, allToAll, batchFetchAll)
	})
	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	ioutil.WriteFile("tmp/benchmark.json", out, 0666)
}

const fastSpeed = 60 * time.Millisecond
const mediumSpeed = 200 * time.Millisecond
const slowSpeed = 800 * time.Millisecond
const superSlowSpeed = 4000 * time.Millisecond
const distribution = 20 * time.Millisecond
const fastBandwidth = 1250000.0
const fastBandwidthDeviation = 300000.0
const mediumBandwidth = 500000.0
const mediumBandwidthDeviation = 80000.0
const slowBandwidth = 100000.0
const slowBandwidthDeviation = 16500.0
const stdBlockSize = 8000

func BenchmarkDupsManyNodesRealWorldNetwork(b *testing.B) {
	benchmarkLog = nil
	fastNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		mediumSpeed-fastSpeed, slowSpeed-fastSpeed,
		0.0, 0.0, distribution, nil)
	fastNetworkDelay := delay.Delay(fastSpeed, fastNetworkDelayGenerator)
	fastBandwidthGenerator := tn.VariableRateLimitGenerator(fastBandwidth, fastBandwidthDeviation, nil)
	averageNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		mediumSpeed-fastSpeed, slowSpeed-fastSpeed,
		0.3, 0.3, distribution, nil)
	averageNetworkDelay := delay.Delay(fastSpeed, averageNetworkDelayGenerator)
	averageBandwidthGenerator := tn.VariableRateLimitGenerator(mediumBandwidth, mediumBandwidthDeviation, nil)
	slowNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		mediumSpeed-fastSpeed, superSlowSpeed-fastSpeed,
		0.3, 0.3, distribution, nil)
	slowNetworkDelay := delay.Delay(fastSpeed, slowNetworkDelayGenerator)
	slowBandwidthGenerator := tn.VariableRateLimitGenerator(slowBandwidth, slowBandwidthDeviation, nil)

	b.Run("200Nodes-AllToAll-BigBatch-FastNetwork", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 300, 200, fastNetworkDelay, fastBandwidthGenerator, stdBlockSize, allToAll, batchFetchAll)
	})
	b.Run("200Nodes-AllToAll-BigBatch-AverageVariableSpeedNetwork", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 300, 200, averageNetworkDelay, averageBandwidthGenerator, stdBlockSize, allToAll, batchFetchAll)
	})
	b.Run("200Nodes-AllToAll-BigBatch-SlowVariableSpeedNetwork", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 300, 200, slowNetworkDelay, slowBandwidthGenerator, stdBlockSize, allToAll, batchFetchAll)
	})
	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	ioutil.WriteFile("tmp/rw-benchmark.json", out, 0666)
}

func subtestDistributeAndFetch(b *testing.B, numnodes, numblks int, d delay.D, df distFunc, ff fetchFunc) {
	start := time.Now()
	net := tn.VirtualNetwork(mockrouting.NewServer(), d)

	sg := NewTestSessionGenerator(net)
	defer sg.Close()

	bg := blocksutil.NewBlockGenerator()

	instances := sg.Instances(numnodes)
	blocks := bg.Blocks(numblks)
	runDistribution(b, instances, blocks, df, ff, start)
}

func subtestDistributeAndFetchRateLimited(b *testing.B, numnodes, numblks int, d delay.D, rateLimitGenerator tn.RateLimitGenerator, blockSize int64, df distFunc, ff fetchFunc) {
	start := time.Now()
	net := tn.RateLimitedVirtualNetwork(mockrouting.NewServer(), d, rateLimitGenerator)

	sg := NewTestSessionGenerator(net)
	defer sg.Close()

	instances := sg.Instances(numnodes)
	blocks := testutil.GenerateBlocksOfSize(numblks, blockSize)

	runDistribution(b, instances, blocks, df, ff, start)
}

func runDistribution(b *testing.B, instances []Instance, blocks []blocks.Block, df distFunc, ff fetchFunc, start time.Time) {

	numnodes := len(instances)

	fetcher := instances[numnodes-1]

	df(b, instances[:numnodes-1], blocks)

	var ks []cid.Cid
	for _, blk := range blocks {
		ks = append(ks, blk.Cid())
	}

	ff(b, fetcher.Exchange, ks)

	st, err := fetcher.Exchange.Stat()
	if err != nil {
		b.Fatal(err)
	}

	nst := fetcher.Exchange.network.Stats()
	stats := runStats{
		Time:    time.Now().Sub(start),
		MsgRecd: nst.MessagesRecvd,
		MsgSent: nst.MessagesSent,
		Dups:    st.DupBlksReceived,
		Name:    b.Name(),
	}
	benchmarkLog = append(benchmarkLog, stats)
	b.Logf("send/recv: %d / %d", nst.MessagesSent, nst.MessagesRecvd)
	if st.DupBlksReceived != 0 {
		b.Fatalf("got %d duplicate blocks!", st.DupBlksReceived)
	}
}

func allToAll(b *testing.B, provs []Instance, blocks []blocks.Block) {
	for _, p := range provs {
		if err := p.Blockstore().PutMany(blocks); err != nil {
			b.Fatal(err)
		}
	}
}

// overlap1 gives the first 75 blocks to the first peer, and the last 75 blocks
// to the second peer. This means both peers have the middle 50 blocks
func overlap1(b *testing.B, provs []Instance, blks []blocks.Block) {
	if len(provs) != 2 {
		b.Fatal("overlap1 only works with 2 provs")
	}
	bill := provs[0]
	jeff := provs[1]

	if err := bill.Blockstore().PutMany(blks[:75]); err != nil {
		b.Fatal(err)
	}
	if err := jeff.Blockstore().PutMany(blks[25:]); err != nil {
		b.Fatal(err)
	}
}

// overlap2 gives every even numbered block to the first peer, odd numbered
// blocks to the second.  it also gives every third block to both peers
func overlap2(b *testing.B, provs []Instance, blks []blocks.Block) {
	if len(provs) != 2 {
		b.Fatal("overlap2 only works with 2 provs")
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

func overlap3(b *testing.B, provs []Instance, blks []blocks.Block) {
	if len(provs) != 2 {
		b.Fatal("overlap3 only works with 2 provs")
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
func onePeerPerBlock(b *testing.B, provs []Instance, blks []blocks.Block) {
	for _, blk := range blks {
		provs[rand.Intn(len(provs))].Blockstore().Put(blk)
	}
}

func oneAtATime(b *testing.B, bs *Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background()).(*bssession.Session)
	for _, c := range ks {
		_, err := ses.GetBlock(context.Background(), c)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.Logf("Session fetch latency: %s", ses.GetAverageLatency())
}

// fetch data in batches, 10 at a time
func batchFetchBy10(b *testing.B, bs *Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())
	for i := 0; i < len(ks); i += 10 {
		out, err := ses.GetBlocks(context.Background(), ks[i:i+10])
		if err != nil {
			b.Fatal(err)
		}
		for range out {
		}
	}
}

// fetch each block at the same time concurrently
func fetchAllConcurrent(b *testing.B, bs *Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())

	var wg sync.WaitGroup
	for _, c := range ks {
		wg.Add(1)
		go func(c cid.Cid) {
			defer wg.Done()
			_, err := ses.GetBlock(context.Background(), c)
			if err != nil {
				b.Fatal(err)
			}
		}(c)
	}
	wg.Wait()
}

func batchFetchAll(b *testing.B, bs *Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())
	out, err := ses.GetBlocks(context.Background(), ks)
	if err != nil {
		b.Fatal(err)
	}
	for range out {
	}
}

// simulates the fetch pattern of trying to sync a unixfs file graph as fast as possible
func unixfsFileFetch(b *testing.B, bs *Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())
	_, err := ses.GetBlock(context.Background(), ks[0])
	if err != nil {
		b.Fatal(err)
	}

	out, err := ses.GetBlocks(context.Background(), ks[1:11])
	if err != nil {
		b.Fatal(err)
	}
	for range out {
	}

	out, err = ses.GetBlocks(context.Background(), ks[11:])
	if err != nil {
		b.Fatal(err)
	}
	for range out {
	}
}
