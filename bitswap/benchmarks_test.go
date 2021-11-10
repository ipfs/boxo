package bitswap_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"
	blocks "github.com/ipfs/go-block-format"
	protocol "github.com/libp2p/go-libp2p-core/protocol"

	bitswap "github.com/ipfs/go-bitswap"
	bssession "github.com/ipfs/go-bitswap/internal/session"
	bsnet "github.com/ipfs/go-bitswap/network"
	testinstance "github.com/ipfs/go-bitswap/testinstance"
	tn "github.com/ipfs/go-bitswap/testnet"
	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
)

type fetchFunc func(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid)

type distFunc func(b *testing.B, provs []testinstance.Instance, blocks []blocks.Block)

type runStats struct {
	DupsRcvd uint64
	BlksRcvd uint64
	MsgSent  uint64
	MsgRecd  uint64
	Time     time.Duration
	Name     string
}

var benchmarkLog []runStats

type bench struct {
	name       string
	nodeCount  int
	blockCount int
	distFn     distFunc
	fetchFn    fetchFunc
}

var benches = []bench{
	// Fetch from two seed nodes that both have all 100 blocks
	// - request one at a time, in series
	{"3Nodes-AllToAll-OneAtATime", 3, 100, allToAll, oneAtATime},
	// - request all 100 with a single GetBlocks() call
	{"3Nodes-AllToAll-BigBatch", 3, 100, allToAll, batchFetchAll},

	// Fetch from two seed nodes, one at a time, where:
	// - node A has blocks 0 - 74
	// - node B has blocks 25 - 99
	{"3Nodes-Overlap1-OneAtATime", 3, 100, overlap1, oneAtATime},

	// Fetch from two seed nodes, where:
	// - node A has even blocks
	// - node B has odd blocks
	// - both nodes have every third block

	// - request one at a time, in series
	{"3Nodes-Overlap3-OneAtATime", 3, 100, overlap2, oneAtATime},
	// - request 10 at a time, in series
	{"3Nodes-Overlap3-BatchBy10", 3, 100, overlap2, batchFetchBy10},
	// - request all 100 in parallel as individual GetBlock() calls
	{"3Nodes-Overlap3-AllConcurrent", 3, 100, overlap2, fetchAllConcurrent},
	// - request all 100 with a single GetBlocks() call
	{"3Nodes-Overlap3-BigBatch", 3, 100, overlap2, batchFetchAll},
	// - request 1, then 10, then 89 blocks (similar to how IPFS would fetch a file)
	{"3Nodes-Overlap3-UnixfsFetch", 3, 100, overlap2, unixfsFileFetch},

	// Fetch from nine seed nodes, all nodes have all blocks
	// - request one at a time, in series
	{"10Nodes-AllToAll-OneAtATime", 10, 100, allToAll, oneAtATime},
	// - request 10 at a time, in series
	{"10Nodes-AllToAll-BatchFetchBy10", 10, 100, allToAll, batchFetchBy10},
	// - request all 100 with a single GetBlocks() call
	{"10Nodes-AllToAll-BigBatch", 10, 100, allToAll, batchFetchAll},
	// - request all 100 in parallel as individual GetBlock() calls
	{"10Nodes-AllToAll-AllConcurrent", 10, 100, allToAll, fetchAllConcurrent},
	// - request 1, then 10, then 89 blocks (similar to how IPFS would fetch a file)
	{"10Nodes-AllToAll-UnixfsFetch", 10, 100, allToAll, unixfsFileFetch},
	// - follow a typical IPFS request pattern for 1000 blocks
	{"10Nodes-AllToAll-UnixfsFetchLarge", 10, 1000, allToAll, unixfsFileFetchLarge},

	// Fetch from nine seed nodes, blocks are distributed randomly across all nodes (no dups)
	// - request one at a time, in series
	{"10Nodes-OnePeerPerBlock-OneAtATime", 10, 100, onePeerPerBlock, oneAtATime},
	// - request all 100 with a single GetBlocks() call
	{"10Nodes-OnePeerPerBlock-BigBatch", 10, 100, onePeerPerBlock, batchFetchAll},
	// - request 1, then 10, then 89 blocks (similar to how IPFS would fetch a file)
	{"10Nodes-OnePeerPerBlock-UnixfsFetch", 10, 100, onePeerPerBlock, unixfsFileFetch},

	// Fetch from 199 seed nodes, all nodes have all blocks, fetch all 20 blocks with a single GetBlocks() call
	{"200Nodes-AllToAll-BigBatch", 200, 20, allToAll, batchFetchAll},
}

func BenchmarkFixedDelay(b *testing.B) {
	benchmarkLog = nil
	fixedDelay := delay.Fixed(10 * time.Millisecond)
	bstoreLatency := time.Duration(0)

	for _, bch := range benches {
		b.Run(bch.name, func(b *testing.B) {
			subtestDistributeAndFetch(b, bch.nodeCount, bch.blockCount, fixedDelay, bstoreLatency, bch.distFn, bch.fetchFn)
		})
	}

	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	_ = ioutil.WriteFile("tmp/benchmark.json", out, 0666)
	printResults(benchmarkLog)
}

type mixedBench struct {
	bench
	fetcherCount int // number of nodes that fetch data
	oldSeedCount int // number of seed nodes running old version of Bitswap
}

var mixedBenches = []mixedBench{
	{bench{"3Nodes-Overlap3-OneAtATime", 3, 10, overlap2, oneAtATime}, 1, 2},
	{bench{"3Nodes-AllToAll-OneAtATime", 3, 10, allToAll, oneAtATime}, 1, 2},
	{bench{"3Nodes-Overlap3-AllConcurrent", 3, 10, overlap2, fetchAllConcurrent}, 1, 2},
	// mixedBench{bench{"3Nodes-Overlap3-UnixfsFetch", 3, 100, overlap2, unixfsFileFetch}, 1, 2},
}

func BenchmarkFetchFromOldBitswap(b *testing.B) {
	benchmarkLog = nil
	fixedDelay := delay.Fixed(10 * time.Millisecond)
	bstoreLatency := time.Duration(0)

	for _, bch := range mixedBenches {
		b.Run(bch.name, func(b *testing.B) {
			fetcherCount := bch.fetcherCount
			oldSeedCount := bch.oldSeedCount
			newSeedCount := bch.nodeCount - (fetcherCount + oldSeedCount)

			net := tn.VirtualNetwork(mockrouting.NewServer(), fixedDelay)

			// Simulate an older Bitswap node (old protocol ID) that doesn't
			// send DONT_HAVE responses
			oldProtocol := []protocol.ID{bsnet.ProtocolBitswapOneOne}
			oldNetOpts := []bsnet.NetOpt{bsnet.SupportedProtocols(oldProtocol)}
			oldBsOpts := []bitswap.Option{bitswap.SetSendDontHaves(false)}
			oldNodeGenerator := testinstance.NewTestInstanceGenerator(net, oldNetOpts, oldBsOpts)

			// Regular new Bitswap node
			newNodeGenerator := testinstance.NewTestInstanceGenerator(net, nil, nil)
			var instances []testinstance.Instance

			// Create new nodes (fetchers + seeds)
			for i := 0; i < fetcherCount+newSeedCount; i++ {
				inst := newNodeGenerator.Next()
				instances = append(instances, inst)
			}
			// Create old nodes (just seeds)
			for i := 0; i < oldSeedCount; i++ {
				inst := oldNodeGenerator.Next()
				instances = append(instances, inst)
			}
			// Connect all the nodes together
			testinstance.ConnectInstances(instances)

			// Generate blocks, with a smaller root block
			rootBlock := testutil.GenerateBlocksOfSize(1, rootBlockSize)
			blocks := testutil.GenerateBlocksOfSize(bch.blockCount, stdBlockSize)
			blocks[0] = rootBlock[0]

			// Run the distribution
			runDistributionMulti(b, instances[:fetcherCount], instances[fetcherCount:], blocks, bstoreLatency, bch.distFn, bch.fetchFn)

			newNodeGenerator.Close()
			oldNodeGenerator.Close()
		})
	}

	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	_ = ioutil.WriteFile("tmp/benchmark.json", out, 0666)
	printResults(benchmarkLog)
}

const datacenterSpeed = 5 * time.Millisecond
const fastSpeed = 60 * time.Millisecond
const mediumSpeed = 200 * time.Millisecond
const slowSpeed = 800 * time.Millisecond
const superSlowSpeed = 4000 * time.Millisecond
const datacenterDistribution = 3 * time.Millisecond
const distribution = 20 * time.Millisecond
const datacenterBandwidth = 125000000.0
const datacenterBandwidthDeviation = 3000000.0
const fastBandwidth = 1250000.0
const fastBandwidthDeviation = 300000.0
const mediumBandwidth = 500000.0
const mediumBandwidthDeviation = 80000.0
const slowBandwidth = 100000.0
const slowBandwidthDeviation = 16500.0
const rootBlockSize = 800
const stdBlockSize = 8000
const largeBlockSize = int64(256 * 1024)

func BenchmarkRealWorld(b *testing.B) {
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
	bstoreLatency := time.Duration(0)

	b.Run("200Nodes-AllToAll-BigBatch-FastNetwork", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 300, 200, fastNetworkDelay, fastBandwidthGenerator, stdBlockSize, bstoreLatency, allToAll, batchFetchAll)
	})
	b.Run("200Nodes-AllToAll-BigBatch-AverageVariableSpeedNetwork", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 300, 200, averageNetworkDelay, averageBandwidthGenerator, stdBlockSize, bstoreLatency, allToAll, batchFetchAll)
	})
	b.Run("200Nodes-AllToAll-BigBatch-SlowVariableSpeedNetwork", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 300, 200, slowNetworkDelay, slowBandwidthGenerator, stdBlockSize, bstoreLatency, allToAll, batchFetchAll)
	})
	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	_ = ioutil.WriteFile("tmp/rw-benchmark.json", out, 0666)
	printResults(benchmarkLog)
}

func BenchmarkDatacenter(b *testing.B) {
	benchmarkLog = nil
	benchmarkSeed, err := strconv.ParseInt(os.Getenv("BENCHMARK_SEED"), 10, 64)
	var randomGen *rand.Rand = nil
	if err == nil {
		randomGen = rand.New(rand.NewSource(benchmarkSeed))
	}

	datacenterNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		fastSpeed-datacenterSpeed, (fastSpeed-datacenterSpeed)/2,
		0.0, 0.0, datacenterDistribution, randomGen)
	datacenterNetworkDelay := delay.Delay(datacenterSpeed, datacenterNetworkDelayGenerator)
	datacenterBandwidthGenerator := tn.VariableRateLimitGenerator(datacenterBandwidth, datacenterBandwidthDeviation, randomGen)
	bstoreLatency := time.Millisecond * 25

	b.Run("3Nodes-Overlap3-UnixfsFetch", func(b *testing.B) {
		subtestDistributeAndFetchRateLimited(b, 3, 100, datacenterNetworkDelay, datacenterBandwidthGenerator, largeBlockSize, bstoreLatency, allToAll, unixfsFileFetch)
	})
	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	_ = ioutil.WriteFile("tmp/rb-benchmark.json", out, 0666)
	printResults(benchmarkLog)
}

func BenchmarkDatacenterMultiLeechMultiSeed(b *testing.B) {
	benchmarkLog = nil
	benchmarkSeed, err := strconv.ParseInt(os.Getenv("BENCHMARK_SEED"), 10, 64)
	var randomGen *rand.Rand = nil
	if err == nil {
		randomGen = rand.New(rand.NewSource(benchmarkSeed))
	}

	datacenterNetworkDelayGenerator := tn.InternetLatencyDelayGenerator(
		fastSpeed-datacenterSpeed, (fastSpeed-datacenterSpeed)/2,
		0.0, 0.0, datacenterDistribution, randomGen)
	datacenterNetworkDelay := delay.Delay(datacenterSpeed, datacenterNetworkDelayGenerator)
	datacenterBandwidthGenerator := tn.VariableRateLimitGenerator(datacenterBandwidth, datacenterBandwidthDeviation, randomGen)
	bstoreLatency := time.Millisecond * 25

	b.Run("3Leech3Seed-AllToAll-UnixfsFetch", func(b *testing.B) {
		d := datacenterNetworkDelay
		rateLimitGenerator := datacenterBandwidthGenerator
		blockSize := largeBlockSize
		df := allToAll
		ff := unixfsFileFetchLarge
		numnodes := 6
		numblks := 1000

		for i := 0; i < b.N; i++ {
			net := tn.RateLimitedVirtualNetwork(mockrouting.NewServer(), d, rateLimitGenerator)

			ig := testinstance.NewTestInstanceGenerator(net, nil, nil)
			defer ig.Close()

			instances := ig.Instances(numnodes)
			blocks := testutil.GenerateBlocksOfSize(numblks, blockSize)
			runDistributionMulti(b, instances[:3], instances[3:], blocks, bstoreLatency, df, ff)
		}
	})

	out, _ := json.MarshalIndent(benchmarkLog, "", "  ")
	_ = ioutil.WriteFile("tmp/rb-benchmark.json", out, 0666)
	printResults(benchmarkLog)
}

func subtestDistributeAndFetch(b *testing.B, numnodes, numblks int, d delay.D, bstoreLatency time.Duration, df distFunc, ff fetchFunc) {
	for i := 0; i < b.N; i++ {
		net := tn.VirtualNetwork(mockrouting.NewServer(), d)

		ig := testinstance.NewTestInstanceGenerator(net, nil, nil)

		instances := ig.Instances(numnodes)
		rootBlock := testutil.GenerateBlocksOfSize(1, rootBlockSize)
		blocks := testutil.GenerateBlocksOfSize(numblks, stdBlockSize)
		blocks[0] = rootBlock[0]
		runDistribution(b, instances, blocks, bstoreLatency, df, ff)
		ig.Close()
	}
}

func subtestDistributeAndFetchRateLimited(b *testing.B, numnodes, numblks int, d delay.D, rateLimitGenerator tn.RateLimitGenerator, blockSize int64, bstoreLatency time.Duration, df distFunc, ff fetchFunc) {
	for i := 0; i < b.N; i++ {
		net := tn.RateLimitedVirtualNetwork(mockrouting.NewServer(), d, rateLimitGenerator)

		ig := testinstance.NewTestInstanceGenerator(net, nil, nil)
		defer ig.Close()

		instances := ig.Instances(numnodes)
		rootBlock := testutil.GenerateBlocksOfSize(1, rootBlockSize)
		blocks := testutil.GenerateBlocksOfSize(numblks, blockSize)
		blocks[0] = rootBlock[0]
		runDistribution(b, instances, blocks, bstoreLatency, df, ff)
	}
}

func runDistributionMulti(b *testing.B, fetchers []testinstance.Instance, seeds []testinstance.Instance, blocks []blocks.Block, bstoreLatency time.Duration, df distFunc, ff fetchFunc) {
	// Distribute blocks to seed nodes
	df(b, seeds, blocks)

	// Set the blockstore latency on seed nodes
	if bstoreLatency > 0 {
		for _, i := range seeds {
			i.SetBlockstoreLatency(bstoreLatency)
		}
	}

	// Fetch blocks (from seed nodes to leech nodes)
	var ks []cid.Cid
	for _, blk := range blocks {
		ks = append(ks, blk.Cid())
	}

	start := time.Now()
	var wg sync.WaitGroup
	for _, fetcher := range fetchers {
		wg.Add(1)

		go func(ftchr testinstance.Instance) {
			defer wg.Done()

			ff(b, ftchr.Exchange, ks)
		}(fetcher)
	}
	wg.Wait()

	// Collect statistics
	fetcher := fetchers[0]
	st, err := fetcher.Exchange.Stat()
	if err != nil {
		b.Fatal(err)
	}

	for _, fetcher := range fetchers {
		nst := fetcher.Adapter.Stats()
		stats := runStats{
			Time:     time.Since(start),
			MsgRecd:  nst.MessagesRecvd,
			MsgSent:  nst.MessagesSent,
			DupsRcvd: st.DupBlksReceived,
			BlksRcvd: st.BlocksReceived,
			Name:     b.Name(),
		}
		benchmarkLog = append(benchmarkLog, stats)
	}
	// b.Logf("send/recv: %d / %d (dups: %d)", nst.MessagesSent, nst.MessagesRecvd, st.DupBlksReceived)
}

func runDistribution(b *testing.B, instances []testinstance.Instance, blocks []blocks.Block, bstoreLatency time.Duration, df distFunc, ff fetchFunc) {
	numnodes := len(instances)
	fetcher := instances[numnodes-1]

	// Distribute blocks to seed nodes
	seeds := instances[:numnodes-1]
	df(b, seeds, blocks)

	// Set the blockstore latency on seed nodes
	if bstoreLatency > 0 {
		for _, i := range seeds {
			i.SetBlockstoreLatency(bstoreLatency)
		}
	}

	// Fetch blocks (from seed nodes to leech nodes)
	var ks []cid.Cid
	for _, blk := range blocks {
		ks = append(ks, blk.Cid())
	}

	start := time.Now()
	ff(b, fetcher.Exchange, ks)

	// Collect statistics
	st, err := fetcher.Exchange.Stat()
	if err != nil {
		b.Fatal(err)
	}

	nst := fetcher.Adapter.Stats()
	stats := runStats{
		Time:     time.Since(start),
		MsgRecd:  nst.MessagesRecvd,
		MsgSent:  nst.MessagesSent,
		DupsRcvd: st.DupBlksReceived,
		BlksRcvd: st.BlocksReceived,
		Name:     b.Name(),
	}
	benchmarkLog = append(benchmarkLog, stats)
	// b.Logf("send/recv: %d / %d (dups: %d)", nst.MessagesSent, nst.MessagesRecvd, st.DupBlksReceived)
}

func allToAll(b *testing.B, provs []testinstance.Instance, blocks []blocks.Block) {
	for _, p := range provs {
		if err := p.Blockstore().PutMany(context.Background(), blocks); err != nil {
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

	if err := bill.Blockstore().PutMany(context.Background(), blks[:75]); err != nil {
		b.Fatal(err)
	}
	if err := jeff.Blockstore().PutMany(context.Background(), blks[25:]); err != nil {
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
			if err := bill.Blockstore().Put(context.Background(), blk); err != nil {
				b.Fatal(err)
			}
		}
		if third || !even {
			if err := jeff.Blockstore().Put(context.Background(), blk); err != nil {
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
		err := provs[rand.Intn(len(provs))].Blockstore().Put(context.Background(), blk)
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
	// b.Logf("Session fetch latency: %s", ses.GetAverageLatency())
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

func unixfsFileFetchLarge(b *testing.B, bs *bitswap.Bitswap, ks []cid.Cid) {
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

	out, err = ses.GetBlocks(context.Background(), ks[11:100])
	if err != nil {
		b.Fatal(err)
	}
	for range out {
	}

	rest := ks[100:]
	for len(rest) > 0 {
		var batch [][]cid.Cid
		for i := 0; i < 5 && len(rest) > 0; i++ {
			cnt := 10
			if len(rest) < 10 {
				cnt = len(rest)
			}
			group := rest[:cnt]
			rest = rest[cnt:]
			batch = append(batch, group)
		}

		var anyErr error
		var wg sync.WaitGroup
		for _, group := range batch {
			wg.Add(1)
			go func(grp []cid.Cid) {
				defer wg.Done()

				out, err = ses.GetBlocks(context.Background(), grp)
				if err != nil {
					anyErr = err
				}
				for range out {
				}
			}(group)
		}
		wg.Wait()

		// Note: b.Fatal() cannot be called from within a go-routine
		if anyErr != nil {
			b.Fatal(anyErr)
		}
	}
}

func printResults(rs []runStats) {
	nameOrder := make([]string, 0)
	names := make(map[string]struct{})
	for i := 0; i < len(rs); i++ {
		if _, ok := names[rs[i].Name]; !ok {
			nameOrder = append(nameOrder, rs[i].Name)
			names[rs[i].Name] = struct{}{}
		}
	}

	for i := 0; i < len(names); i++ {
		name := nameOrder[i]
		count := 0
		sent := 0.0
		rcvd := 0.0
		dups := 0.0
		blks := 0.0
		elpd := 0.0
		for i := 0; i < len(rs); i++ {
			if rs[i].Name == name {
				count++
				sent += float64(rs[i].MsgSent)
				rcvd += float64(rs[i].MsgRecd)
				dups += float64(rs[i].DupsRcvd)
				blks += float64(rs[i].BlksRcvd)
				elpd += float64(rs[i].Time)
			}
		}
		sent /= float64(count)
		rcvd /= float64(count)
		dups /= float64(count)
		blks /= float64(count)

		label := fmt.Sprintf("%s (%d runs / %.2fs):", name, count, elpd/1000000000.0)
		fmt.Printf("%-75s %s: sent %d, recv %d, dups %d / %d\n",
			label,
			fmtDuration(time.Duration(int64(math.Round(elpd/float64(count))))),
			int64(math.Round(sent)), int64(math.Round(rcvd)),
			int64(math.Round(dups)), int64(math.Round(blks)))
	}
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Millisecond)
	s := d / time.Second
	d -= s * time.Second
	ms := d / time.Millisecond
	return fmt.Sprintf("%d.%03ds", s, ms)
}
