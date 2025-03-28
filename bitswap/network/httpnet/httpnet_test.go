package httpnet

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

var errorCid = cid.MustParse("bafkreiachshsblgr5kv3mzbgfgmvuhllwe2f6fasm6mykzwsi4l7odq464")   // "errorcid"
var slowCid = cid.MustParse("bafkreidhph5i4jevaun4eqjxolqgn3rfpoknj35ocyos3on57iriwpaujm")    // "slowcid"
var backoffCid = cid.MustParse("bafkreid6g5qrufgqj46djic7ntjnppaj5bg4urppjoyywrxwegvltrmqbu") // "backoff"

var _ network.Receiver = (*mockRecv)(nil)

type mockRecv struct {
	blocks    map[cid.Cid]struct{}
	haves     map[cid.Cid]struct{}
	donthaves map[cid.Cid]struct{}
	waitCh    chan struct{}
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

func (recv *mockRecv) wait(seconds time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), seconds*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		return errors.New("receiver waited too long without receiving message")
	case <-recv.waitCh:
		return nil
	}
}

func (recv *mockRecv) ReceiveError(err error) {

}

func (recv *mockRecv) PeerConnected(p peer.ID) {

}

func (recv *mockRecv) PeerDisconnected(p peer.ID) {

}

func mockReceiver(t *testing.T) *mockRecv {
	t.Helper()
	return &mockRecv{
		blocks:    make(map[cid.Cid]struct{}),
		haves:     make(map[cid.Cid]struct{}),
		donthaves: make(map[cid.Cid]struct{}),
		waitCh:    make(chan struct{}, 1),
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
	opts = append(opts, WithInsecureSkipVerify(true))
	htnet := New(h, opts...)
	htnet.Start(recv)
	return htnet.(*Network), mn
}

func makeBlocks(t *testing.T, start, end int) []blocks.Block {
	t.Helper()

	var blks []blocks.Block
	for i := start; i < end; i++ {
		blks = append(blks, blocks.NewBlock([]byte(fmt.Sprintf("%d", i))))
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
	if r.Method == "HEAD" {
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

func connectToPeer(t *testing.T, ctx context.Context, htnet *Network, remote host.Host, srvs ...*httptest.Server) {
	var addrs []multiaddr.Multiaddr
	for _, srv := range srvs {
		addrs = append(addrs, srvMultiaddr(t, srv))
	}

	err := htnet.Connect(
		ctx,
		peer.AddrInfo{
			ID:    remote.ID(),
			Addrs: addrs,
		},
	)
	if err != nil {
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
	connectToPeer(t, ctx, htnet, peer, msrv)

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
	for i := 0; i < 4; i++ {
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

func TestSendMessage(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	msrv := makeServer(t, 0, 10)
	connectToPeer(t, ctx, htnet, peer, msrv)

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
	connectToPeer(t, ctx, htnet, peer, msrv, msrv2)

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
	connectToPeer(t, ctx, htnet, peer, msrv)

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
	connectToPeer(t, ctx, htnet, peer, msrv)

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
	connectToPeer(t, ctx, htnet, peer, msrv)
	connectToPeer(t, ctx, htnet, peer2, msrv)

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
