package httpnet

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	"github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var (
	errorCid   = cid.MustParse("bafkreiachshsblgr5kv3mzbgfgmvuhllwe2f6fasm6mykzwsi4l7odq464") // "errorcid"
	slowCid    = cid.MustParse("bafkreidhph5i4jevaun4eqjxolqgn3rfpoknj35ocyos3on57iriwpaujm") // "slowcid"
	backoffCid = cid.MustParse("bafkreid6g5qrufgqj46djic7ntjnppaj5bg4urppjoyywrxwegvltrmqbu") // "backoff"
)

var _ network.Receiver = (*mockRecv)(nil)

type mockRecv struct {
	// mu guards the maps: coalesced requests deliver responses to
	// several peers' collector goroutines concurrently.
	mu                 sync.Mutex
	blocks             map[cid.Cid]struct{}
	haves              map[cid.Cid]struct{}
	donthaves          map[cid.Cid]struct{}
	waitCh             chan struct{}
	waitConnectedCh    chan struct{}
	waitDisconnectedCh chan struct{}
}

func (recv *mockRecv) ReceiveMessage(ctx context.Context, sender peer.ID, incoming bsmsg.BitSwapMessage) {
	recv.mu.Lock()
	for _, b := range incoming.Blocks() {
		recv.blocks[b.Cid()] = struct{}{}
	}

	for _, c := range incoming.Haves() {
		recv.haves[c] = struct{}{}
	}

	for _, c := range incoming.DontHaves() {
		recv.donthaves[c] = struct{}{}
	}
	recv.mu.Unlock()

	recv.waitCh <- struct{}{}
}

func (recv *mockRecv) wait(seconds int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		return errors.New("receiver waited too long without receiving message")
	case <-recv.waitCh:
		return nil
	}
}

func (recv *mockRecv) waitConnected(seconds int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		return errors.New("receiver waited too long without receiving a connect event")
	case <-recv.waitConnectedCh:
		return nil
	}
}

func (recv *mockRecv) waitDisconnected(seconds int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		return errors.New("receiver waited too long without receiving a disconnect event")
	case <-recv.waitDisconnectedCh:
		return nil
	}
}

func (recv *mockRecv) ReceiveError(err error) {
}

func (recv *mockRecv) PeerConnected(p peer.ID) {
	recv.waitConnectedCh <- struct{}{}
}

func (recv *mockRecv) PeerDisconnected(p peer.ID) {
	recv.waitDisconnectedCh <- struct{}{}
}

func mockReceiver(t *testing.T) *mockRecv {
	t.Helper()
	return &mockRecv{
		blocks:             make(map[cid.Cid]struct{}),
		haves:              make(map[cid.Cid]struct{}),
		donthaves:          make(map[cid.Cid]struct{}),
		waitCh:             make(chan struct{}, 1),
		waitConnectedCh:    make(chan struct{}, 1),
		waitDisconnectedCh: make(chan struct{}, 1),
	}
}

func mockNet(t *testing.T) mocknet.Mocknet {
	t.Helper()

	return mocknet.New()
}

func mockNetwork(t *testing.T, recv network.Receiver, opts ...Option) (*Network, mocknet.Mocknet) {
	t.Helper()

	mn := mockNet(t)

	h, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	// allow ovewrite of default options by prepending them
	opts = append([]Option{WithInsecureSkipVerify(true)}, opts...)
	htnet := New(h, opts...)
	htnet.Start(recv)
	return htnet.(*Network), mn
}

func makeBlocks(t *testing.T, start, end int) []blocks.Block {
	t.Helper()

	var blks []blocks.Block
	for i := start; i < end; i++ {
		blks = append(blks, blocks.NewBlock(fmt.Appendf(nil, "%d", i)))
	}
	return blks
}

func makeCids(t *testing.T, start, end int) []cid.Cid {
	t.Helper()

	var cids []cid.Cid
	blks := makeBlocks(t, start, end)
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}
	return cids
}

func makeMessage(wantlist []cid.Cid, wantType pb.Message_Wantlist_WantType, sendDontHave bool) bsmsg.BitSwapMessage {
	msg := bsmsg.New(true)
	for _, c := range wantlist {
		msg.AddEntry(
			c,
			0,
			wantType,
			sendDontHave,
		)
	}
	return msg
}

func makeWantsMessage(wantlist []cid.Cid) bsmsg.BitSwapMessage {
	return makeMessage(wantlist, pb.Message_Wantlist_Block, true)
}

func makeHavesMessage(wantlist []cid.Cid) bsmsg.BitSwapMessage {
	return makeMessage(wantlist, pb.Message_Wantlist_Have, true)
}

func makeBlockstore(t *testing.T, start, end int) blockstore.Blockstore {
	t.Helper()

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))

	blks := makeBlocks(t, start, end)

	ctx := context.Background()
	for _, b := range blks {
		err := bs.Put(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
	}
	return bs
}

type Handler struct {
	bstore blockstore.Blockstore

	// Probe instrumentation. probes counts requests for pingCid.
	// probeStatus, when non-zero, overrides the 200 a probe answers by
	// default; probeHeadStatus, when non-zero, overrides it for HEAD
	// probes only. probeRetryAfter, when set, is sent as a Retry-After
	// header on non-200 probe responses.
	probes          atomic.Int64
	probeStatus     atomic.Int64
	probeHeadStatus atomic.Int64
	probeRetryAfter atomic.Value // string
}

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	_, cidstr, ok := strings.Cut(path, "/ipfs/")
	if !ok {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	c, err := cid.Parse(cidstr)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	if cidstr == pingCid {
		h.probes.Add(1)
		status := int(h.probeStatus.Load())
		if r.Method == http.MethodHead {
			if s := int(h.probeHeadStatus.Load()); s != 0 {
				status = s
			}
		}
		if status == 0 {
			status = http.StatusOK
		}
		if status != http.StatusOK {
			if ra, _ := h.probeRetryAfter.Load().(string); ra != "" {
				rw.Header().Set("Retry-After", ra)
			}
		}
		rw.WriteHeader(status)
		return
	}

	if c.Equals(errorCid) {
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	if c.Equals(backoffCid) {
		rw.Header().Set("Retry-After", "5")
		rw.WriteHeader(http.StatusTooManyRequests)
		return
	}

	if c.Equals(slowCid) {
		time.Sleep(2 * time.Second)
	}

	b, err := h.bstore.Get(r.Context(), c)
	if errors.Is(err, ipld.ErrNotFound{}) {
		rw.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}

	rw.Write(b.RawData())
}

func makeServerAndHandler(t *testing.T, bstart, bend int) (*httptest.Server, *Handler) {
	t.Helper()

	handler := &Handler{
		bstore: makeBlockstore(t, bstart, bend),
	}

	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	return srv, handler
}

func makeServer(t *testing.T, bstart, bend int) *httptest.Server {
	t.Helper()

	srv, _ := makeServerAndHandler(t, bstart, bend)
	return srv
}

func srvMultiaddr(t *testing.T, srv *httptest.Server) multiaddr.Multiaddr {
	t.Helper()

	maddr, err := manet.FromNetAddr(srv.Listener.Addr())
	if err != nil {
		t.Fatal(err)
	}

	httpma, err := multiaddr.NewMultiaddr("/https")
	if err != nil {
		t.Fatal(err)
	}

	return maddr.Encapsulate(httpma)
}

func connectToPeer(t *testing.T, ctx context.Context, htnet *Network, remote host.Host, srvs ...*httptest.Server) error {
	var addrs []multiaddr.Multiaddr
	for _, srv := range srvs {
		addrs = append(addrs, srvMultiaddr(t, srv))
	}

	return htnet.Connect(
		ctx,
		peer.AddrInfo{
			ID:    remote.ID(),
			Addrs: addrs,
		},
	)
}

func mustConnectToPeer(t *testing.T, ctx context.Context, htnet *Network, remote host.Host, srvs ...*httptest.Server) {
	t.Helper()

	if err := connectToPeer(t, ctx, htnet, remote, srvs...); err != nil {
		t.Fatal(err)
	}
}

func TestBestURL(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	msrv := makeServer(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peer, msrv)

	nms, err := htnet.NewMessageSender(
		ctx,
		peer.ID(),
		&network.MessageSenderOpts{
			MaxRetries: 5,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	ms := nms.(*httpMsgSender)

	baseurl, err := url.Parse("http://127.0.0.1/ipfs")
	if err != nil {
		t.Fatal(err)
	}
	var urls []*url.URL
	for i := range 4 {
		baseurl.Host = fmt.Sprintf("127.0.0.1:%d", 1000+i)
		u, _ := url.Parse(baseurl.String())
		urls = append(urls, u)
	}
	// add some bogus urls to test the sorting
	now := time.Now()
	surls := []*senderURL{
		{
			ParsedURL: network.ParsedURL{
				URL: urls[0],
			},
		},
		{
			ParsedURL: network.ParsedURL{
				URL: urls[1],
			},
		},
		{
			ParsedURL: network.ParsedURL{
				URL: urls[2],
			},
		},
		{
			ParsedURL: network.ParsedURL{
				URL: urls[3],
			},
		},
	}

	surls[0].cooldown.Store(now.Add(time.Second))
	surls[0].serverErrors.Store(6)
	surls[1].cooldown.Store(now.Add(time.Second))
	surls[1].serverErrors.Store(1)
	surls[2].cooldown.Store(time.Time{})
	surls[2].serverErrors.Store(3)
	surls[3].cooldown.Store(time.Time{})
	surls[3].serverErrors.Store(2)

	ms.urls = surls

	sortedUrls := ms.sortURLS()

	expected := []string{
		urls[3].String(),
		urls[2].String(),
		urls[1].String(),
		urls[0].String(),
	}

	for i, u := range sortedUrls {
		if u.URL.String() != expected[i] {
			t.Error("wrong url order", i, u.URL)
		}
	}

	ms.urls = sortedUrls[3:]

	_, err = ms.bestURL(nil)
	if err == nil {
		t.Fatal("expected error since only urls failed too many times")
	}
}

func TestConnectErrors(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	msrv := makeServer(t, 0, 0)

	htnet, mn := mockNetwork(t, recv,
		WithInsecureSkipVerify(false),
	)
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	err = connectToPeer(t, ctx, htnet, peer, msrv)
	if err == nil {
		t.Fatal("expected error")
	}
	t.Log(err)
	if !strings.Contains(err.Error(), "failed to verify") {
		t.Error("wrong error")
	}

	htnet2, mn2 := mockNetwork(t, recv,
		WithDenylist([]string{"127.0.0.1"}),
	)

	peer2, err := mn2.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	err = connectToPeer(t, ctx, htnet2, peer2, msrv)
	if err == nil {
		t.Fatal("expected error")
	}
	t.Log(err)
	if !strings.Contains(err.Error(), "denylist") {
		t.Error("wrong error")
	}
}

func TestSendMessage(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	msrv := makeServer(t, 0, 10)
	mustConnectToPeer(t, ctx, htnet, peer, msrv)

	wl := makeCids(t, 0, 10)
	msg := makeWantsMessage(wl)

	err = htnet.SendMessage(ctx, peer.ID(), msg)
	if err != nil {
		t.Fatal(err)
	}

	recv.wait(5)

	for _, c := range wl {
		if _, ok := recv.blocks[c]; !ok {
			t.Error("block was not received")
		}
	}
}

func TestSendMessageWithFailingServer(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	msrv := makeServer(t, 0, 0)
	msrv2 := makeServer(t, 0, 10)
	mustConnectToPeer(t, ctx, htnet, peer, msrv, msrv2)

	wl := makeCids(t, 0, 10)
	msg := makeWantsMessage(wl)

	err = htnet.SendMessage(ctx, peer.ID(), msg)
	if err != nil {
		t.Fatal(err)
	}

	err = recv.wait(5)
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range wl {
		if _, ok := recv.blocks[c]; !ok {
			t.Error("block was not received")
		}
	}
}

func TestSendMessageWithPartialResponse(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	msrv := makeServer(t, 5, 10)
	mustConnectToPeer(t, ctx, htnet, peer, msrv)

	wl := makeCids(t, 0, 10)
	msg := makeWantsMessage(wl)

	err = htnet.SendMessage(ctx, peer.ID(), msg)
	if err != nil {
		t.Fatal(err)
	}

	recv.wait(5)

	for _, c := range wl[5:10] {
		if _, ok := recv.blocks[c]; !ok {
			t.Error("block was not received")
		}
	}

	for _, c := range wl[0:5] {
		if _, ok := recv.blocks[c]; ok {
			t.Error("block should not have been received")
		}
	}
}

func TestSendMessageSendHavesAndDontHaves(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	msrv := makeServer(t, 0, 5)
	mustConnectToPeer(t, ctx, htnet, peer, msrv)

	wl := makeCids(t, 0, 10)
	msg := makeHavesMessage(wl)

	err = htnet.SendMessage(ctx, peer.ID(), msg)
	if err != nil {
		t.Fatal(err)
	}

	recv.wait(5)

	for _, c := range wl[0:5] {
		if _, ok := recv.haves[c]; !ok {
			t.Error("have was not received")
		}
	}

	for _, c := range wl[5:10] {
		if _, ok := recv.donthaves[c]; !ok {
			t.Error("dont_have was not received")
		}
	}
}

func TestBackOff(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)

	// 1 server associated to two peers.
	// so that it has the same url.
	// We trigger backoff using peer1
	// and the backoff should happen when making a
	// request on peer2.
	// The backoff means the blocks are recorded as "don't have".

	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	peer2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	msrv := makeServer(t, 0, 1)
	mustConnectToPeer(t, ctx, htnet, peer, msrv)
	mustConnectToPeer(t, ctx, htnet, peer2, msrv)

	nms, err := htnet.NewMessageSender(ctx, peer.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}

	wl := makeCids(t, 0, 1)
	msg := makeWantsMessage([]cid.Cid{backoffCid})
	msg2 := makeWantsMessage(wl)

	err = nms.SendMsg(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}

	recv.wait(1)
	if len(recv.donthaves) == 0 {
		t.Fatal("back off should have counted as DONT_HAVE")
	}

	// should produce a dont_have as well even though we have this cid.
	// (because we are in backoff for the url-host).
	nms2, err := htnet.NewMessageSender(ctx, peer2.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}

	err = nms2.SendMsg(ctx, msg2)
	if err != nil {
		t.Fatal(err)
	}

	recv.wait(1)

	if len(recv.donthaves) != 2 || (len(recv.blocks)+len(recv.haves)) > 0 {
		t.Error("no blocks should have been received while on backoff")
	}
}

// Write a TestErrorTracking function which tests that a peer is disconnected when the treshold is crossed.
func TestErrorTracking(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv, WithMaxDontHaveErrors(1))

	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	msrv := makeServer(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peer, msrv)

	err = recv.waitConnected(1)
	if err != nil {
		t.Fatal(err)
	}

	wl := makeCids(t, 0, 1)
	msg := makeWantsMessage(wl)

	err = htnet.SendMessage(ctx, peer.ID(), msg)
	if err != nil {
		t.Fatal(err)
	}

	recv.wait(1)
	err = recv.waitDisconnected(1)
	if err == nil { // we received a disconnect event
		t.Fatal("disconnect event not expected")
	}

	// Threshold was 1. This will trigger a disconnection.
	err = htnet.SendMessage(ctx, peer.ID(), msg)
	if err != nil {
		t.Fatal(err)
	}

	recv.wait(1)
	err = recv.waitDisconnected(1)
	if err != nil {
		t.Fatal(err)
	}
}

// TestNoBackgroundProbes is the regression guard for the removed periodic
// ping loop: an idle connected peer must not generate any request. The sleep
// is deliberately longer than the old 5s ping cadence.
func TestNoBackgroundProbes(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv, handler := makeServerAndHandler(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peer, srv)

	probes := handler.probes.Load()
	if probes == 0 {
		t.Fatal("Connect should have probed the endpoint")
	}

	time.Sleep(6 * time.Second)

	if got := handler.probes.Load(); got != probes {
		t.Errorf("idle connected peer generated background probes: %d -> %d", probes, got)
	}
}

func TestConnectSeedsLatency(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv := makeServer(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peer, srv)

	if htnet.Latency(peer.ID()) <= 0 {
		t.Error("latency should be seeded by the Connect probe")
	}
}

func TestPassiveLatencyUpdate(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv := makeServer(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peer, srv)

	seed := htnet.Latency(peer.ID())

	// slowCid answers with a ~2s delay, far above the probe seed, so the
	// EWMA must move up once the response is recorded.
	msg := makeWantsMessage([]cid.Cid{slowCid})
	if err := htnet.SendMessage(ctx, peer.ID(), msg); err != nil {
		t.Fatal(err)
	}
	if err := recv.wait(5); err != nil {
		t.Fatal(err)
	}

	if got := htnet.Latency(peer.ID()); got <= seed {
		t.Errorf("latency should grow after a slow response: seed %s, got %s", seed, got)
	}
}

func TestPassiveLatencySkipsThrottleStatuses(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv := makeServer(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peer, srv)

	seed := htnet.Latency(peer.ID())

	// backoffCid answers 429: a throttled response must not move the
	// latency estimate, or short rejections would shrink DONT_HAVE
	// timeouts.
	msg := makeWantsMessage([]cid.Cid{backoffCid})
	if err := htnet.SendMessage(ctx, peer.ID(), msg); err != nil {
		t.Fatal(err)
	}
	if err := recv.wait(5); err != nil {
		t.Fatal(err)
	}

	if got := htnet.Latency(peer.ID()); got != seed {
		t.Errorf("latency should not move on 429: seed %s, got %s", seed, got)
	}
}

func TestConnectCooldownRetryAfter(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv, handler := makeServerAndHandler(t, 0, 0)
	handler.probeStatus.Store(http.StatusTooManyRequests)
	handler.probeRetryAfter.Store("1")

	if err := connectToPeer(t, ctx, htnet, peer, srv); err == nil {
		t.Fatal("expected connect to fail while throttled")
	}
	if got := handler.probes.Load(); got != 1 {
		t.Fatalf("429 on HEAD should not be followed by GET: %d probes", got)
	}

	// A second connect within the Retry-After window makes no request.
	err = connectToPeer(t, ctx, htnet, peer, srv)
	if err == nil {
		t.Fatal("expected connect to fail during cooldown")
	}
	if !strings.Contains(err.Error(), "cooldown") {
		t.Errorf("expected a cooldown error, got: %s", err)
	}
	if got := handler.probes.Load(); got != 1 {
		t.Errorf("connect during cooldown should not probe: %d probes", got)
	}

	// After the Retry-After deadline, connect probes again.
	handler.probeStatus.Store(0)
	time.Sleep(1100 * time.Millisecond)
	mustConnectToPeer(t, ctx, htnet, peer, srv)
	if got := handler.probes.Load(); got != 2 {
		t.Errorf("connect after cooldown should probe again: %d probes", got)
	}
}

func TestConnectCooldownDefaultBackoff(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv, handler := makeServerAndHandler(t, 0, 0)
	handler.probeStatus.Store(http.StatusInternalServerError)

	if err := connectToPeer(t, ctx, htnet, peer, srv); err == nil {
		t.Fatal("expected connect to fail")
	}
	if got := handler.probes.Load(); got != 2 {
		t.Fatalf("expected HEAD and GET probes: %d", got)
	}

	// The host is cooling down for DefaultConnectFailureBackoff: a second
	// connect makes no request.
	if err := connectToPeer(t, ctx, htnet, peer, srv); err == nil {
		t.Fatal("expected connect to fail during cooldown")
	}
	if got := handler.probes.Load(); got != 2 {
		t.Errorf("connect during cooldown should not probe: %d probes", got)
	}

	host := srv.Listener.Addr().String()
	dl, cooling := htnet.cooldownTracker.inCooldown(host)
	if !cooling {
		t.Fatal("expected an active cooldown for the host")
	}
	if until := time.Until(dl); until < DefaultConnectFailureBackoff-10*time.Second || until > DefaultConnectFailureBackoff {
		t.Errorf("cooldown should be about DefaultConnectFailureBackoff away: %s", until)
	}
}

// A Retry-After date in the past (cached response, clock skew) must not
// disable the backoff: the default applies instead.
func TestConnectCooldownPastRetryAfterDate(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv, handler := makeServerAndHandler(t, 0, 0)
	handler.probeStatus.Store(http.StatusTooManyRequests)
	handler.probeRetryAfter.Store(time.Now().Add(-time.Hour).UTC().Format(time.RFC1123))

	if err := connectToPeer(t, ctx, htnet, peer, srv); err == nil {
		t.Fatal("expected connect to fail while throttled")
	}

	dl, cooling := htnet.cooldownTracker.inCooldown(srv.Listener.Addr().String())
	if !cooling {
		t.Fatal("expected an active cooldown despite the past Retry-After date")
	}
	if until := time.Until(dl); until < DefaultConnectFailureBackoff-10*time.Second || until > DefaultConnectFailureBackoff {
		t.Errorf("expected the default backoff, got a deadline %s away", until)
	}
}

// An endpoint skipped only because its host was cooling stays in the
// peerstore as a failover target for the connection's lifetime; since it was
// never probed, HEAD support is not assumed for the peer.
func TestConnectKeepsCooledEndpoints(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	cooled := makeServer(t, 0, 0)
	healthy := makeServer(t, 0, 0)

	htnet.cooldownTracker.setByDuration(cooled.Listener.Addr().String(), time.Minute)

	mustConnectToPeer(t, ctx, htnet, peer, cooled, healthy)

	if addrs := htnet.host.Peerstore().Addrs(peer.ID()); len(addrs) != 2 {
		t.Errorf("cooled endpoint should stay in the peerstore: %d addrs", len(addrs))
	}
	if supportsHave(htnet.host.Peerstore(), peer.ID()) {
		t.Error("HEAD support must not be assumed for an unprobed endpoint")
	}
}

// A 410 on the probe is an endpoint that does not serve the probe path, not a
// working gateway.
func TestConnectProbe410Fails(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv, handler := makeServerAndHandler(t, 0, 0)
	handler.probeStatus.Store(http.StatusGone)

	if err := connectToPeer(t, ctx, htnet, peer, srv); err == nil {
		t.Fatal("expected connect to fail on a 410 probe")
	}
	if got := handler.probes.Load(); got != 2 {
		t.Errorf("expected HEAD and GET probes: %d", got)
	}
	if _, cooling := htnet.cooldownTracker.inCooldown(srv.Listener.Addr().String()); !cooling {
		t.Error("failed probe should have started a cooldown")
	}
}

func TestConnectHeadFallbackNotSelfGated(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv, handler := makeServerAndHandler(t, 0, 0)
	handler.probeHeadStatus.Store(http.StatusMethodNotAllowed)

	// HEAD fails, the GET fallback within the same Connect must still run
	// and succeed, and no cooldown may be written for the host.
	mustConnectToPeer(t, ctx, htnet, peer, srv)

	if got := handler.probes.Load(); got != 2 {
		t.Errorf("expected HEAD and GET probes: %d", got)
	}
	if _, cooling := htnet.cooldownTracker.inCooldown(srv.Listener.Addr().String()); cooling {
		t.Error("no cooldown should be set when the GET fallback succeeded")
	}
	if supportsHave(htnet.host.Peerstore(), peer.ID()) {
		t.Error("HEAD support should have been recorded as false")
	}
}

func TestDisconnectFreezesProbes(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv, handler := makeServerAndHandler(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peer, srv)

	probes := handler.probes.Load()

	if err := htnet.DisconnectFrom(ctx, peer.ID()); err != nil {
		t.Fatal(err)
	}
	if htnet.IsConnectedToPeer(ctx, peer.ID()) {
		t.Error("peer should not be connected after DisconnectFrom")
	}
	if htnet.Latency(peer.ID()) != 0 {
		t.Error("latency should be wiped on disconnect")
	}
	if got := handler.probes.Load(); got != probes {
		t.Errorf("disconnect should not generate probes: %d -> %d", probes, got)
	}

	// Reconnecting probes again and re-seeds latency.
	mustConnectToPeer(t, ctx, htnet, peer, srv)
	if got := handler.probes.Load(); got != probes+1 {
		t.Errorf("reconnect should probe again: %d -> %d", probes, got)
	}
	if htnet.Latency(peer.ID()) <= 0 {
		t.Error("latency should be re-seeded on reconnect")
	}
}

func TestPingAllHostsCooling(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv, handler := makeServerAndHandler(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peer, srv)

	probes := handler.probes.Load()

	htnet.cooldownTracker.setByDuration(srv.Listener.Addr().String(), time.Minute)

	res := htnet.Ping(ctx, peer.ID())
	if !errors.Is(res.Error, errProbesInCooldown) {
		t.Errorf("expected errProbesInCooldown, got: %v", res.Error)
	}
	if got := handler.probes.Load(); got != probes {
		t.Errorf("ping during cooldown should not probe: %d -> %d", probes, got)
	}
}

// TestCooldownSharedAcrossNetworks covers the reason the cooldown registry is
// process-wide: an application that builds a Network per retrieval must not
// re-probe a host that just failed, even from a brand-new instance, and
// Network.Stop must not tear the shared registry down.
func TestCooldownSharedAcrossNetworks(t *testing.T) {
	ctx := context.Background()
	srv, handler := makeServerAndHandler(t, 0, 0)
	handler.probeStatus.Store(http.StatusInternalServerError)

	htnet1, mn1 := mockNetwork(t, mockReceiver(t))
	peer1, err := mn1.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := connectToPeer(t, ctx, htnet1, peer1, srv); err == nil {
		t.Fatal("expected connect to fail")
	}
	if got := handler.probes.Load(); got != 2 {
		t.Fatalf("expected HEAD and GET probes: %d", got)
	}

	// Stopping the first Network must leave the shared registry running.
	htnet1.Stop()

	// A different Network in the same process inherits the cooldown: an
	// ephemeral node must not re-probe a host that just failed.
	htnet2, mn2 := mockNetwork(t, mockReceiver(t))
	peer2, err := mn2.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	err = connectToPeer(t, ctx, htnet2, peer2, srv)
	if err == nil {
		t.Fatal("expected connect to fail during shared cooldown")
	}
	if !strings.Contains(err.Error(), "cooldown") {
		t.Errorf("expected a cooldown error, got: %s", err)
	}
	if got := handler.probes.Load(); got != 2 {
		t.Errorf("second Network should not probe during shared cooldown: %d probes", got)
	}

	// A Network with a private registry is isolated and probes again.
	htnet3, mn3 := mockNetwork(t, mockReceiver(t), WithCooldownTracker(NewCooldownTracker()))
	peer3, err := mn3.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := connectToPeer(t, ctx, htnet3, peer3, srv); err == nil {
		t.Fatal("expected connect to fail")
	}
	if got := handler.probes.Load(); got != 4 {
		t.Errorf("a private registry should probe independently: %d probes", got)
	}
}

func TestSenderCooldownExpires(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv := makeServer(t, 0, 1)
	mustConnectToPeer(t, ctx, htnet, peer, srv)

	nms, err := htnet.NewMessageSender(ctx, peer.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ms := nms.(*httpMsgSender)
	u := ms.urls[0]

	entry := makeWantsMessage(makeCids(t, 0, 1)).Wantlist()[0]

	// A pending cooldown short-circuits without a request.
	u.cooldown.Store(time.Now().Add(time.Minute))
	_, serr := ms.tryURL(ctx, u, entry)
	if serr == nil || serr.Type != typeRetryLater {
		t.Fatal("pending cooldown should return retry-later")
	}

	// An expired cooldown is cleared and the request proceeds. This is
	// what keeps a sender created during a cooldown from treating it as
	// permanent.
	u.cooldown.Store(time.Now().Add(-time.Second))
	b, serr := ms.tryURL(ctx, u, entry)
	if serr != nil {
		t.Fatalf("expired cooldown should not block the request: %s", serr)
	}
	if b == nil {
		t.Fatal("expected a block")
	}
	if !u.cooldown.Load().(time.Time).IsZero() {
		t.Error("expired cooldown snapshot should have been cleared")
	}
}

// TestCoalescedRequestsShareOneRoundTrip verifies end to end that
// concurrent identical wants from two peer IDs resolving to the same
// HTTP endpoint produce a single wire request, while each peer still
// receives its own bitswap response.
func TestCoalescedRequestsShareOneRoundTrip(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)

	peerA, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	peerB, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	var hits atomic.Int32
	handler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ipfs/"+pingCid) {
			rw.WriteHeader(http.StatusOK)
			return
		}
		hits.Add(1)
		// Hold the response so the second peer's want overlaps the
		// first peer's in-flight request.
		time.Sleep(400 * time.Millisecond)
		rw.WriteHeader(http.StatusNotFound)
	})
	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	mustConnectToPeer(t, ctx, htnet, peerA, srv)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}
	mustConnectToPeer(t, ctx, htnet, peerB, srv)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}

	msg := makeWantsMessage(makeCids(t, 0, 1))
	if err := htnet.SendMessage(ctx, peerA.ID(), msg); err != nil {
		t.Fatal(err)
	}
	if err := htnet.SendMessage(ctx, peerB.ID(), msg); err != nil {
		t.Fatal(err)
	}

	// Both peers must deliver their (DONT_HAVE) response.
	if err := recv.wait(3); err != nil {
		t.Fatal(err)
	}
	if err := recv.wait(3); err != nil {
		t.Fatal(err)
	}
	if len(recv.donthaves) != 1 {
		t.Fatalf("want the shared cid as DONT_HAVE, got %d entries", len(recv.donthaves))
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("concurrent identical wants made %d wire requests, want 1", got)
	}
}
