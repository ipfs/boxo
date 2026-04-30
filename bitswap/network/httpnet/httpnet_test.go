package httpnet

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
	blocks             map[cid.Cid]struct{}
	haves              map[cid.Cid]struct{}
	donthaves          map[cid.Cid]struct{}
	waitCh             chan struct{}
	waitConnectedCh    chan struct{}
	waitDisconnectedCh chan struct{}
}

func (recv *mockRecv) ReceiveMessage(ctx context.Context, sender peer.ID, incoming bsmsg.BitSwapMessage) {
	for _, b := range incoming.Blocks() {
		recv.blocks[b.Cid()] = struct{}{}
	}

	for _, c := range incoming.Haves() {
		recv.haves[c] = struct{}{}
	}

	for _, c := range incoming.DontHaves() {
		recv.donthaves[c] = struct{}{}
	}

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
		rw.WriteHeader(http.StatusOK)
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

func makeServer(t *testing.T, bstart, bend int) *httptest.Server {
	t.Helper()

	handler := &Handler{
		bstore: makeBlockstore(t, bstart, bend),
	}

	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.StartTLS()
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
	surls := make([]*senderURL, len(urls))
	for i := range urls {
		surls[i] = &senderURL{
			ParsedURL:    network.ParsedURL{URL: urls[i]},
			serverErrors: new(atomic.Int64),
		}
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

	// Retry-After is "I am busy", not "I am broken". The shared per-host
	// breaker must not have been incremented; otherwise peer2 would have
	// inherited a non-zero count and bestURL would have tripped the
	// fatal disconnect path instead of returning DONT_HAVE.
	urls := network.ExtractURLsFromPeer(htnet.host.Peerstore().PeerInfo(peer.ID()))
	if len(urls) == 0 {
		t.Fatal("expected at least one URL on peer")
	}
	hostKey := endpointKey(urls[0].URL.Scheme, urls[0].URL.Host, urls[0].SNI)
	htnet.breaker.mu.Lock()
	c := htnet.breaker.counters[hostKey]
	htnet.breaker.mu.Unlock()
	if c != nil && c.Load() != 0 {
		t.Errorf("breaker counter for %s = %d after Retry-After cycle, want 0", hostKey, c.Load())
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

// TestSharedBreakerDisconnectsAcrossPeers verifies that when one peer
// triggers a real server error against a host shared with other peers,
// the per-host breaker is high enough that the next SendMsg from any
// peer using the same host disconnects too. Without sharing, each peer
// would burn its own MaxRetries quota before disconnecting.
func TestSharedBreakerDisconnectsAcrossPeers(t *testing.T) {
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

	srv := makeServer(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peerA, srv)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}
	mustConnectToPeer(t, ctx, htnet, peerB, srv)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}

	// Drive a real server error from peerA against the shared host. The
	// server returns 500 with empty body which lands in the default
	// (typeServer) arm of handleResponse and increments the breaker.
	msg := makeWantsMessage([]cid.Cid{errorCid})
	if err := htnet.SendMessage(ctx, peerA.ID(), msg); err != nil {
		t.Fatal(err)
	}
	if err := recv.waitDisconnected(2); err != nil {
		t.Fatalf("peerA should disconnect after typeServer trips MaxRetries: %v", err)
	}

	// Counter is shared. peerB's senderURL inherits the same pointer at
	// MessageSender construction time; bestURL trips fatal before any
	// HTTP request even goes out.
	wl := makeCids(t, 0, 1)
	if err := htnet.SendMessage(ctx, peerB.ID(), makeWantsMessage(wl)); err != nil {
		t.Fatal(err)
	}
	if err := recv.waitDisconnected(2); err != nil {
		t.Fatalf("peerB should disconnect via the per-host shared breaker: %v", err)
	}
}

// TestTryURL_CooldownAutoExpiry verifies that a senderURL with a stale
// (already-elapsed) cooldown deadline does not block tryURL forever.
// fillSenderURLs only checks the deadline at construction time, so a
// long-lived senderURL needs tryURL itself to ignore expired deadlines.
func TestTryURL_CooldownAutoExpiry(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)

	p, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	srv := makeServer(t, 0, 1)
	mustConnectToPeer(t, ctx, htnet, p, srv)

	nms, err := htnet.NewMessageSender(ctx, p.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ms := nms.(*httpMsgSender)

	// Stamp an already-elapsed cooldown on the senderURL.
	ms.urls[0].cooldown.Store(time.Now().Add(-1 * time.Hour))

	wantBlocks := makeCids(t, 0, 1)
	msg := makeWantsMessage(wantBlocks)
	if err := nms.SendMsg(ctx, msg); err != nil {
		t.Fatal(err)
	}
	recv.wait(1)
	if len(recv.blocks) != 1 {
		t.Errorf("expected the block through despite stale cooldown; got blocks=%d donthaves=%d",
			len(recv.blocks), len(recv.donthaves))
	}
}
