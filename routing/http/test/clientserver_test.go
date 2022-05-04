package test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"
	proto "github.com/ipfs/go-delegated-routing/gen/proto"
	"github.com/ipfs/go-delegated-routing/server"
	ipns "github.com/ipfs/go-ipns"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

func testClientServer(t *testing.T, numIter int) (avgLatency time.Duration, deltaGo int, deltaMem uint64) {
	// start a server
	s := httptest.NewServer(server.DelegatedRoutingAsyncHandler(testDelegatedRoutingService{}))
	defer s.Close()

	// start a client
	q, err := proto.New_DelegatedRouting_Client(s.URL, proto.DelegatedRouting_Client_WithHTTPClient(s.Client()))
	if err != nil {
		t.Fatal(err)
	}
	c := client.NewClient(q)

	// verify result
	h, err := multihash.Sum([]byte("TEST"), multihash.SHA3, 4)
	if err != nil {
		t.Fatal(err)
	}

	ngoStart, allocStart := snapUtilizations()
	fmt.Printf("start: goroutines=%d allocated=%d\n", ngoStart, allocStart)

	timeStart := time.Now()

	for i := 0; i < numIter; i++ {
		// exercise FindProviders
		infos, err := c.FindProviders(context.Background(), cid.NewCidV1(cid.Raw, h))
		if err != nil {
			t.Fatal(err)
		}
		if len(infos) != 1 {
			t.Fatalf("expecting 1 result, got %d", len(infos))
		}
		if infos[0].ID != testAddrInfo.ID {
			t.Errorf("expecting %#v, got %#v", testAddrInfo.ID, infos[0].ID)
		}
		if len(infos[0].Addrs) != 1 {
			t.Fatalf("expecting 1 address, got %d", len(infos[0].Addrs))
		}
		if !infos[0].Addrs[0].Equal(testAddrInfo.Addrs[0]) {
			t.Errorf("expecting %#v, got %#v", testAddrInfo.Addrs[0], infos[0].Addrs[0])
		}

		// exercise GetIPNS
		record, err := c.GetIPNS(context.Background(), testIPNSID)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(record, testIPNSRecord) {
			t.Errorf("expecting %#v, got %#v", testIPNSRecord, record[0])
		}

		// exercise PutIPNS
		err = c.PutIPNS(context.Background(), testIPNSID, testIPNSRecord)
		if err != nil {
			t.Fatal(err)
		}
	}

	timeEnd := time.Now()
	avgLatency = timeEnd.Sub(timeStart) / time.Duration(numIter)
	fmt.Printf("average roundtrip latency: %v\n", avgLatency)

	ngoEnd, allocEnd := snapUtilizations()
	fmt.Printf("end: goroutines=%d allocated=%d\n", ngoEnd, allocEnd)
	deltaGo, deltaMem = ngoEnd-ngoStart, allocEnd-allocStart
	fmt.Printf("diff: goroutines=%d allocated=%d\n", deltaGo, deltaMem)

	return
}

type testStatistic struct {
	total        float64
	totalSquared float64
	count        int64
	max          float64
	min          float64
}

func (s *testStatistic) Add(sample float64) {
	s.total += sample
	s.totalSquared += sample * sample
	if s.count == 0 {
		s.max = sample
		s.min = sample
	} else {
		s.max = max64(s.max, sample)
		s.min = min64(s.min, sample)
	}
	s.count++
}

func max64(x, y float64) float64 {
	if x > y {
		return x
	}
	return y
}

func min64(x, y float64) float64 {
	if x < y {
		return x
	}
	return y
}

func (s testStatistic) Mean() float64 {
	return s.total / float64(s.count)
}

func (s testStatistic) Variance() float64 {
	mean := s.Mean()
	return s.totalSquared/float64(s.count) - mean*mean
}

func (s testStatistic) Stddev() float64 {
	return math.Sqrt(s.Variance())
}

func (s testStatistic) MaxDeviation() float64 {
	mean := s.Mean()
	return max64(math.Abs(s.max-mean), math.Abs(s.min-mean))
}

func (s testStatistic) DeviatesBy(numStddev float64) bool {
	return s.MaxDeviation()/s.Stddev() > numStddev
}

func TestClientServer(t *testing.T) {

	var numIter []int = []int{1e2, 1e3, 1e4}
	avgLatency := make([]time.Duration, len(numIter))
	deltaGo := make([]int, len(numIter))
	deltaMem := make([]uint64, len(numIter))
	for i, n := range numIter {
		avgLatency[i], deltaGo[i], deltaMem[i] = testClientServer(t, n)
	}

	// compute means and standard deviations of all statistics
	var avgLatencyStat, deltaGoStat, deltaMemStat testStatistic
	for i := range numIter {
		avgLatencyStat.Add(float64(avgLatency[i]))
		deltaGoStat.Add(float64(deltaGo[i]))
		deltaMemStat.Add(float64(deltaMem[i]))
	}

	// each test instance executes with a different number of iterations (1e3, 1e4, 1e5).
	// for each iteration, we measure three statistics:
	//	- latency of average iteration (i.e. an rpc network call)
	//	- excess/remaining number of goroutines after the test instance runs
	//	- excess/remaining allocated memory after the test instance runs
	// we then verify that no statistic regresses beyond 2 standard deviations across the different test runs.
	// this ensures that none of the statistics grow with the increase in test iterations.
	// in turn, this implies that there are no leakages of memory or goroutines.
	const deviationFactor = 2
	if avgLatencyStat.DeviatesBy(deviationFactor) {
		t.Errorf("average latency is not stable")
	}
	if deltaGoStat.DeviatesBy(deviationFactor) {
		t.Errorf("allocated goroutines count is not stable")
	}
	if deltaMemStat.DeviatesBy(deviationFactor) {
		t.Errorf("allocated memory is not stable")
	}
}

func snapUtilizations() (numGoroutines int, alloc uint64) {
	runtime.GC()
	time.Sleep(time.Second)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return runtime.NumGoroutine(), ms.Alloc
}

const (
	testPeerID   = "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"
	testPeerAddr = "/ip4/7.7.7.7/tcp/4242"
)

var (
	// provider record
	testMultiaddr = multiaddr.StringCast(testPeerAddr)
	testAddrInfo  = &peer.AddrInfo{
		ID:    peer.ID(testPeerID),
		Addrs: []multiaddr.Multiaddr{testMultiaddr},
	}
	// IPNS
	testIPNSID     []byte
	testIPNSRecord []byte
)

// TestMain generates a valid IPNS key and record for testing purposes.
func TestMain(m *testing.M) {
	privateKey, publicKey, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		panic(err)
	}
	peerID, err := peer.IDFromPublicKey(publicKey)
	if err != nil {
		panic(err)
	}
	testIPNSID = []byte(ipns.RecordKey(peerID))
	entry, err := ipns.Create(privateKey, testIPNSID, 0, time.Now().Add(time.Hour), time.Hour)
	if err != nil {
		panic(err)
	}
	if err = ipns.EmbedPublicKey(publicKey, entry); err != nil {
		panic(err)
	}
	testIPNSRecord, err = entry.Marshal()
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

type testDelegatedRoutingService struct{}

func (testDelegatedRoutingService) GetIPNS(id []byte) (<-chan client.GetIPNSAsyncResult, error) {
	ch := make(chan client.GetIPNSAsyncResult)
	go func() {
		ch <- client.GetIPNSAsyncResult{Record: testIPNSRecord}
		close(ch)
	}()
	return ch, nil
}

func (testDelegatedRoutingService) PutIPNS(id []byte, record []byte) (<-chan client.PutIPNSAsyncResult, error) {
	ch := make(chan client.PutIPNSAsyncResult)
	go func() {
		ch <- client.PutIPNSAsyncResult{}
		close(ch)
	}()
	return ch, nil
}

func (testDelegatedRoutingService) FindProviders(key cid.Cid) (<-chan client.FindProvidersAsyncResult, error) {
	ch := make(chan client.FindProvidersAsyncResult)
	go func() {
		ch <- client.FindProvidersAsyncResult{AddrInfo: []peer.AddrInfo{*testAddrInfo}}
		close(ch)
	}()
	return ch, nil
}
