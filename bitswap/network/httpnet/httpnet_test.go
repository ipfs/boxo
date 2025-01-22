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
	"github.com/libp2p/go-libp2p/core/peerstore"
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

func (recv *mockRecv) wait() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	if c.Equals(errorCid) {
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	if c.Equals(backoffCid) {
		rw.Header().Set("Retry-After", "5")
		rw.WriteHeader(http.StatusTooManyRequests)
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

func associateServerToPeer(t *testing.T, srv *httptest.Server, h, remote host.Host) {
	h.Peerstore().AddAddr(
		remote.ID(),
		srvMultiaddr(t, srv),
		peerstore.PermanentAddrTTL,
	)
}

func TestBestURL(t *testing.T) {
	ctx := context.Background()
	htnet, mn := mockNetwork(t, mockReceiver(t))
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	msrv := makeServer(t, 0, 0)
	associateServerToPeer(t, msrv, htnet.host, peer)

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
	for i := 0; i < 6; i++ {
		baseurl.Host = fmt.Sprintf("127.0.0.1:%d", 1000+i)
		u, _ := url.Parse(baseurl.String())
		urls = append(urls, u)
	}
	// add some bogus urls to test the sorting
	now := time.Now()
	surls := []*senderURL{
		{
			url:          urls[0],
			cooldown:     now.Add(time.Second),
			clientErrors: 0,
			serverErrors: 2,
		},
		{
			url:          urls[1],
			cooldown:     now.Add(time.Second),
			clientErrors: 0,
			serverErrors: 1,
		},
		{
			url:          urls[2],
			cooldown:     time.Time{},
			clientErrors: 2,
			serverErrors: 2,
		},
		{
			url:          urls[3],
			cooldown:     time.Time{},
			clientErrors: 2,
			serverErrors: 1,
		},
		{
			url:          urls[4],
			cooldown:     time.Time{},
			clientErrors: 1,
			serverErrors: 2,
		},
		{
			url:          urls[5],
			cooldown:     time.Time{},
			clientErrors: 0,
			serverErrors: 20,
		},
	}
	ms.urls = surls

	ms.sortURLS()

	expected := []string{
		urls[4].String(), // no cooldown, min client errors
		urls[3].String(), // min server errors
		urls[2].String(), // no timeout
		urls[1].String(), // min server
		urls[0].String(),
		urls[5].String(), // maxed errors
	}

	for i, u := range ms.urls {
		if u.url.String() != expected[i] {
			t.Error("wrong url order", i, u.url)
		}
	}

	ms.urls = ms.urls[5:]

	_, err = ms.bestURL()
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
	associateServerToPeer(t, msrv, htnet.host, peer)

	wl := makeCids(t, 0, 10)
	msg := makeWantsMessage(wl)

	err = htnet.SendMessage(ctx, peer.ID(), msg)
	if err != nil {
		t.Fatal(err)
	}

	recv.wait()

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
	associateServerToPeer(t, msrv, htnet.host, peer)
	associateServerToPeer(t, msrv2, htnet.host, peer)

	wl := makeCids(t, 0, 10)
	msg := makeWantsMessage(wl)

	err = htnet.SendMessage(ctx, peer.ID(), msg)
	if err != nil {
		t.Fatal(err)
	}

	err = recv.wait()
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
	associateServerToPeer(t, msrv, htnet.host, peer)

	wl := makeCids(t, 0, 10)
	msg := makeWantsMessage(wl)

	err = htnet.SendMessage(ctx, peer.ID(), msg)
	if err != nil {
		t.Fatal(err)
	}

	recv.wait()

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
	htnet, mn := mockNetwork(t, recv,
		WithSupportsHave(true),
	)
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	msrv := makeServer(t, 0, 5)
	associateServerToPeer(t, msrv, htnet.host, peer)

	wl := makeCids(t, 0, 10)
	msg := makeHavesMessage(wl)

	err = htnet.SendMessage(ctx, peer.ID(), msg)
	if err != nil {
		t.Fatal(err)
	}

	recv.wait()

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

func TestSendCancels(t *testing.T) {
	ctx := context.Background()
	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv,
		WithSupportsHave(true),
	)
	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	msrv := makeServer(t, 0, 1)
	associateServerToPeer(t, msrv, htnet.host, peer)

	peer2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	msrv2 := makeServer(t, 0, 1)
	associateServerToPeer(t, msrv2, htnet.host, peer2)

	wl := makeCids(t, 0, 1)
	msg := makeWantsMessage(append(wl, slowCid))
	msg2 := msg.Clone()
	msg2.Reset(true)
	msg2.Cancel(slowCid)

	go func() {
		// send message to peer1
		err := htnet.SendMessage(ctx, peer.ID(), msg)
		if err != nil {
			t.Error(err)
		}
	}()
	time.Sleep(1 * time.Second)

	// send cancel to peer2, should abort all ongoing requests for that
	// cid.
	err = htnet.SendMessage(ctx, peer2.ID(), msg2)
	if err != nil {
		t.Fatal(err)
	}

	err = recv.wait()
	if err != nil {
		t.Fatal(err)
	}
	if len(recv.donthaves) > 0 {
		t.Fatal("request aborted, don't haves should not have been sent")
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

	peer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	peer2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	msrv := makeServer(t, 0, 1)
	associateServerToPeer(t, msrv, htnet.host, peer)
	associateServerToPeer(t, msrv, htnet.host, peer2)

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

	nms2, err := htnet.NewMessageSender(ctx, peer2.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	err = nms2.SendMsg(ctx, msg2)
	if err != nil {
		t.Fatal(err)
	}
	recv.wait()
	timeSince := time.Since(now)

	if timeSince < 5*time.Second {
		t.Fatal("backoff should have made request wait for more than 5 seconds")
	}
	t.Logf("waited for %s seconds", timeSince)

}
