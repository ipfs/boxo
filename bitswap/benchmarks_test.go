package bitswap_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"
	blocks "github.com/ipfs/go-block-format"

	bitswap "github.com/ipfs/go-bitswap"
	bssession "github.com/ipfs/go-bitswap/session"
	testinstance "github.com/ipfs/go-bitswap/testinstance"
	tn "github.com/ipfs/go-bitswap/testnet"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
)

type fetchFunc func(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid)

type distFunc func(b *testing.B, provs []testinstance.Instance, blocks []blocks.Block)

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

	b.Run("Overlap3-OneAtATime", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap2, oneAtATime)
	})
	b.Run("Overlap3-BatchBy10", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap2, batchFetchBy10)
	})
	b.Run("Overlap3-AllConcurrent", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap2, fetchAllConcurrent)
	})
	b.Run("Overlap3-BigBatch", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap2, batchFetchAll)
	})
	b.Run("Overlap3-UnixfsFetch", func(b *testing.B) {
		subtestDistributeAndFetch(b, 3, 100, fixedDelay, overlap2, unixfsFileFetch)
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
	_ = ioutil.WriteFile("tmp/benchmark.json", out, 0666)
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
	benchmarkSeed, err := strconv.ParseInt(os.Getenv("BENCHMARK_SEED"), 10, 64)
	var randomGen *rand.Rand = nil
	if err == nil {
		randomGen = rand.New(rand.NewSource(benchmarkSeed))
	}

	fastNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		mediumSpeed-fastSpeed, slowSpeed-fastSpeed,
		0.0, 0.0, distribution, randomGen)
	fastNetworkDelay := delay.Delay(fastSpeed, fastNetworkDelayGenerator)
	fastBandwidthGenerator := tn.VariableRateLimitGenerator(fastBandwidth, fastBandwidthDeviation, randomGen)
	averageNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		mediumSpeed-fastSpeed, slowSpeed-fastSpeed,
		0.3, 0.3, distribution, randomGen)
	averageNetworkDelay := delay.Delay(fastSpeed, averageNetworkDelayGenerator)
	averageBandwidthGenerator := tn.VariableRateLimitGenerator(mediumBandwidth, mediumBandwidthDeviation, randomGen)
	slowNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		mediumSpeed-fastSpeed, superSlowSpeed-fastSpeed,
		0.3, 0.3, distribution, randomGen)
	slowNetworkDelay := delay.Delay(fastSpeed, slowNetworkDelayGenerator)
	slowBandwidthGenerator := tn.VariableRateLimitGenerator(slowBandwidth, slowBandwidthDeviation, randomGen)

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
	_ = ioutil.WriteFile("tmp/rw-benchmark.json", out, 0666)
}

func subtestDistributeAndFetch(b *testing.B, numnodes, numblks int, d delay.D, df distFunc, ff fetchFunc) {
	for i := 0; i < b.N; i++ {
		start := time.Now()
		net := tn.VirtualNetwork(mockrouting.NewServer(), d)

		ig := testinstance.NewTestInstanceGenerator(net)
		defer ig.Close()

		bg := blocksutil.NewBlockGenerator()

		instances := ig.Instances(numnodes)
		blocks := bg.Blocks(numblks)
		runDistribution(b, instances, blocks, df, ff, start)
	}
}

func subtestDistributeAndFetchRateLimited(b *testing.B, numnodes, numblks int, d delay.D, rateLimitGenerator tn.RateLimitGenerator, blockSize int64, df distFunc, ff fetchFunc) {
	for i := 0; i < b.N; i++ {

		start := time.Now()
		net := tn.RateLimitedVirtualNetwork(mockrouting.NewServer(), d, rateLimitGenerator)

		ig := testinstance.NewTestInstanceGenerator(net)
		defer ig.Close()

		instances := ig.Instances(numnodes)
		blocks := testutil.GenerateBlocksOfSize(numblks, blockSize)

		runDistribution(b, instances, blocks, df, ff, start)
	}
}

func runDistribution(b *testing.B, instances []testinstance.Instance, blocks []blocks.Block, df distFunc, ff fetchFunc, start time.Time) {

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

	nst := fetcher.Adapter.Stats()
	stats := runStats{
		Time:    time.Since(start),
		MsgRecd: nst.MessagesRecvd,
		MsgSent: nst.MessagesSent,
		Dups:    st.DupBlksReceived,
		Name:    b.Name(),
	}
	benchmarkLog = append(benchmarkLog, stats)
	b.Logf("send/recv: %d / %d", nst.MessagesSent, nst.MessagesRecvd)
}

func allToAll(b *testing.B, provs []testinstance.Instance, blocks []blocks.Block) {
	for _, p := range provs {
		if err := p.Blockstore().PutMany(blocks); err != nil {
			b.Fatal(err)
		}
	}
}

// overlap1 gives the first 75 blocks to the first peer, and the last 75 blocks
// to the second peer. This means both peers have the middle 50 blocks
func overlap1(b *testing.B, provs []testinstance.Instance, blks []blocks.Block) {
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
func overlap2(b *testing.B, provs []testinstance.Instance, blks []blocks.Block) {
	if len(provs) != 2 {
		b.Fatal("overlap2 only works with 2 provs")
	}
	bill := provs[0]
	jeff := provs[1]

	for i, blk := range blks {
		even := i%2 == 0
		third := i%3 == 0
		if third || even {
			if err := bill.Blockstore().Put(blk); err != nil {
				b.Fatal(err)
			}
		}
		if third || !even {
			if err := jeff.Blockstore().Put(blk); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// onePeerPerBlock picks a random peer to hold each block
// with this layout, we shouldnt actually ever see any duplicate blocks
// but we're mostly just testing performance of the sync algorithm
func onePeerPerBlock(b *testing.B, provs []testinstance.Instance, blks []blocks.Block) {
	for _, blk := range blks {
		err := provs[rand.Intn(len(provs))].Blockstore().Put(blk)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func oneAtATime(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
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
func batchFetchBy10(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
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
func fetchAllConcurrent(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())

	var wg sync.WaitGroup
	for _, c := range ks {
		wg.Add(1)
		go func(c cid.Cid) {
			defer wg.Done()
			_, err := ses.GetBlock(context.Background(), c)
			if err != nil {
				b.Error(err)
			}
		}(c)
	}
	wg.Wait()
}

func batchFetchAll(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
	ses := bs.NewSession(context.Background())
	out, err := ses.GetBlocks(context.Background(), ks)
	if err != nil {
		b.Fatal(err)
	}
	for range out {
	}
}

// simulates the fetch pattern of trying to sync a unixfs file graph as fast as possible
func unixfsFileFetch(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
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
