package decision

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	message "github.com/ipfs/go-bitswap/message"

	blocks "github.com/ipfs/go-block-format"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	process "github.com/jbenet/goprocess"
	peer "github.com/libp2p/go-libp2p-core/peer"
	testutil "github.com/libp2p/go-libp2p-core/test"
)

type peerTag struct {
	done  chan struct{}
	peers map[peer.ID]int
}

type fakePeerTagger struct {
	lk   sync.Mutex
	tags map[string]*peerTag
}

func (fpt *fakePeerTagger) TagPeer(p peer.ID, tag string, n int) {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	if fpt.tags == nil {
		fpt.tags = make(map[string]*peerTag, 1)
	}
	pt, ok := fpt.tags[tag]
	if !ok {
		pt = &peerTag{peers: make(map[peer.ID]int, 1), done: make(chan struct{})}
		fpt.tags[tag] = pt
	}
	pt.peers[p] = n
}

func (fpt *fakePeerTagger) UntagPeer(p peer.ID, tag string) {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	pt := fpt.tags[tag]
	if pt == nil {
		return
	}
	delete(pt.peers, p)
	if len(pt.peers) == 0 {
		close(pt.done)
		delete(fpt.tags, tag)
	}
}

func (fpt *fakePeerTagger) count(tag string) int {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	if pt, ok := fpt.tags[tag]; ok {
		return len(pt.peers)
	}
	return 0
}

func (fpt *fakePeerTagger) wait(tag string) {
	fpt.lk.Lock()
	pt := fpt.tags[tag]
	if pt == nil {
		fpt.lk.Unlock()
		return
	}
	doneCh := pt.done
	fpt.lk.Unlock()
	<-doneCh
}

type engineSet struct {
	PeerTagger *fakePeerTagger
	Peer       peer.ID
	Engine     *Engine
	Blockstore blockstore.Blockstore
}

func newEngine(ctx context.Context, idStr string) engineSet {
	fpt := &fakePeerTagger{}
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	e := NewEngine(ctx, bs, fpt)
	e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))
	return engineSet{
		Peer: peer.ID(idStr),
		//Strategy: New(true),
		PeerTagger: fpt,
		Blockstore: bs,
		Engine:     e,
	}
}

func TestConsistentAccounting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := newEngine(ctx, "Ernie")
	receiver := newEngine(ctx, "Bert")

	// Send messages from Ernie to Bert
	for i := 0; i < 1000; i++ {

		m := message.New(false)
		content := []string{"this", "is", "message", "i"}
		m.AddBlock(blocks.NewBlock([]byte(strings.Join(content, " "))))

		sender.Engine.MessageSent(receiver.Peer, m)
		receiver.Engine.MessageReceived(ctx, sender.Peer, m)
	}

	// Ensure sender records the change
	if sender.Engine.numBytesSentTo(receiver.Peer) == 0 {
		t.Fatal("Sent bytes were not recorded")
	}

	// Ensure sender and receiver have the same values
	if sender.Engine.numBytesSentTo(receiver.Peer) != receiver.Engine.numBytesReceivedFrom(sender.Peer) {
		t.Fatal("Inconsistent book-keeping. Strategies don't agree")
	}

	// Ensure sender didn't record receving anything. And that the receiver
	// didn't record sending anything
	if receiver.Engine.numBytesSentTo(sender.Peer) != 0 || sender.Engine.numBytesReceivedFrom(receiver.Peer) != 0 {
		t.Fatal("Bert didn't send bytes to Ernie")
	}
}

func TestPeerIsAddedToPeersWhenMessageReceivedOrSent(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sanfrancisco := newEngine(ctx, "sf")
	seattle := newEngine(ctx, "sea")

	m := message.New(true)

	sanfrancisco.Engine.MessageSent(seattle.Peer, m)
	seattle.Engine.MessageReceived(ctx, sanfrancisco.Peer, m)

	if seattle.Peer == sanfrancisco.Peer {
		t.Fatal("Sanity Check: Peers have same Key!")
	}

	if !peerIsPartner(seattle.Peer, sanfrancisco.Engine) {
		t.Fatal("Peer wasn't added as a Partner")
	}

	if !peerIsPartner(sanfrancisco.Peer, seattle.Engine) {
		t.Fatal("Peer wasn't added as a Partner")
	}

	seattle.Engine.PeerDisconnected(sanfrancisco.Peer)
	if peerIsPartner(sanfrancisco.Peer, seattle.Engine) {
		t.Fatal("expected peer to be removed")
	}
}

func peerIsPartner(p peer.ID, e *Engine) bool {
	for _, partner := range e.Peers() {
		if partner == p {
			return true
		}
	}
	return false
}

func TestOutboxClosedWhenEngineClosed(t *testing.T) {
	ctx := context.Background()
	t.SkipNow() // TODO implement *Engine.Close
	e := NewEngine(ctx, blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())), &fakePeerTagger{})
	e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for nextEnvelope := range e.Outbox() {
			<-nextEnvelope
		}
		wg.Done()
	}()
	// e.Close()
	wg.Wait()
	if _, ok := <-e.Outbox(); ok {
		t.Fatal("channel should be closed")
	}
}

func TestPartnerWantsThenCancels(t *testing.T) {
	numRounds := 10
	if testing.Short() {
		numRounds = 1
	}
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")
	vowels := strings.Split("aeiou", "")

	type testCase [][]string
	testcases := []testCase{
		{
			alphabet, vowels,
		},
		{
			alphabet, stringsComplement(alphabet, vowels),
			alphabet[1:25], stringsComplement(alphabet[1:25], vowels), alphabet[2:25], stringsComplement(alphabet[2:25], vowels),
			alphabet[3:25], stringsComplement(alphabet[3:25], vowels), alphabet[4:25], stringsComplement(alphabet[4:25], vowels),
			alphabet[5:25], stringsComplement(alphabet[5:25], vowels), alphabet[6:25], stringsComplement(alphabet[6:25], vowels),
			alphabet[7:25], stringsComplement(alphabet[7:25], vowels), alphabet[8:25], stringsComplement(alphabet[8:25], vowels),
			alphabet[9:25], stringsComplement(alphabet[9:25], vowels), alphabet[10:25], stringsComplement(alphabet[10:25], vowels),
			alphabet[11:25], stringsComplement(alphabet[11:25], vowels), alphabet[12:25], stringsComplement(alphabet[12:25], vowels),
			alphabet[13:25], stringsComplement(alphabet[13:25], vowels), alphabet[14:25], stringsComplement(alphabet[14:25], vowels),
			alphabet[15:25], stringsComplement(alphabet[15:25], vowels), alphabet[16:25], stringsComplement(alphabet[16:25], vowels),
			alphabet[17:25], stringsComplement(alphabet[17:25], vowels), alphabet[18:25], stringsComplement(alphabet[18:25], vowels),
			alphabet[19:25], stringsComplement(alphabet[19:25], vowels), alphabet[20:25], stringsComplement(alphabet[20:25], vowels),
			alphabet[21:25], stringsComplement(alphabet[21:25], vowels), alphabet[22:25], stringsComplement(alphabet[22:25], vowels),
			alphabet[23:25], stringsComplement(alphabet[23:25], vowels), alphabet[24:25], stringsComplement(alphabet[24:25], vowels),
			alphabet[25:25], stringsComplement(alphabet[25:25], vowels),
		},
	}

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	for _, letter := range alphabet {
		block := blocks.NewBlock([]byte(letter))
		if err := bs.Put(block); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()
	for i := 0; i < numRounds; i++ {
		expected := make([][]string, 0, len(testcases))
		e := NewEngine(ctx, bs, &fakePeerTagger{})
		e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))
		for _, testcase := range testcases {
			set := testcase[0]
			cancels := testcase[1]
			keeps := stringsComplement(set, cancels)
			expected = append(expected, keeps)

			partner := testutil.RandPeerIDFatal(t)

			partnerWants(e, set, partner)
			partnerCancels(e, cancels, partner)
		}
		if err := checkHandledInOrder(t, e, expected); err != nil {
			t.Logf("run #%d of %d", i, numRounds)
			t.Fatal(err)
		}
	}
}

func TestTaggingPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	sanfrancisco := newEngine(ctx, "sf")
	seattle := newEngine(ctx, "sea")

	keys := []string{"a", "b", "c", "d", "e"}
	for _, letter := range keys {
		block := blocks.NewBlock([]byte(letter))
		if err := sanfrancisco.Blockstore.Put(block); err != nil {
			t.Fatal(err)
		}
	}
	partnerWants(sanfrancisco.Engine, keys, seattle.Peer)
	next := <-sanfrancisco.Engine.Outbox()
	envelope := <-next

	if sanfrancisco.PeerTagger.count(sanfrancisco.Engine.tagQueued) != 1 {
		t.Fatal("Incorrect number of peers tagged")
	}
	envelope.Sent()
	<-sanfrancisco.Engine.Outbox()
	sanfrancisco.PeerTagger.wait(sanfrancisco.Engine.tagQueued)
	if sanfrancisco.PeerTagger.count(sanfrancisco.Engine.tagQueued) != 0 {
		t.Fatal("Peers should be untagged but weren't")
	}
}

func TestTaggingUseful(t *testing.T) {
	oldShortTerm := shortTerm
	shortTerm = 1 * time.Millisecond
	defer func() { shortTerm = oldShortTerm }()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	me := newEngine(ctx, "engine")
	friend := peer.ID("friend")

	block := blocks.NewBlock([]byte("foobar"))
	msg := message.New(false)
	msg.AddBlock(block)

	for i := 0; i < 3; i++ {
		if me.PeerTagger.count(me.Engine.tagUseful) != 0 {
			t.Fatal("Peers should be untagged but weren't")
		}
		me.Engine.MessageSent(friend, msg)
		time.Sleep(shortTerm * 2)
		if me.PeerTagger.count(me.Engine.tagUseful) != 1 {
			t.Fatal("Peers should be tagged but weren't")
		}
		time.Sleep(shortTerm * 8)
	}

	if me.PeerTagger.count(me.Engine.tagUseful) == 0 {
		t.Fatal("peers should still be tagged due to long-term usefulness")
	}
	time.Sleep(shortTerm * 2)
	if me.PeerTagger.count(me.Engine.tagUseful) == 0 {
		t.Fatal("peers should still be tagged due to long-term usefulness")
	}
	time.Sleep(shortTerm * 20)
	if me.PeerTagger.count(me.Engine.tagUseful) != 0 {
		t.Fatal("peers should finally be untagged")
	}
}

func partnerWants(e *Engine, keys []string, partner peer.ID) {
	add := message.New(false)
	for i, letter := range keys {
		block := blocks.NewBlock([]byte(letter))
		add.AddEntry(block.Cid(), len(keys)-i)
	}
	e.MessageReceived(context.Background(), partner, add)
}

func partnerCancels(e *Engine, keys []string, partner peer.ID) {
	cancels := message.New(false)
	for _, k := range keys {
		block := blocks.NewBlock([]byte(k))
		cancels.Cancel(block.Cid())
	}
	e.MessageReceived(context.Background(), partner, cancels)
}

func checkHandledInOrder(t *testing.T, e *Engine, expected [][]string) error {
	for _, keys := range expected {
		next := <-e.Outbox()
		envelope := <-next
		received := envelope.Message.Blocks()
		// Verify payload message length
		if len(received) != len(keys) {
			return errors.New(fmt.Sprintln("# blocks received", len(received), "# blocks expected", len(keys)))
		}
		// Verify payload message contents
		for _, k := range keys {
			found := false
			expected := blocks.NewBlock([]byte(k))
			for _, block := range received {
				if block.Cid().Equals(expected.Cid()) {
					found = true
					break
				}
			}
			if !found {
				return errors.New(fmt.Sprintln("received", received, "expected", string(expected.RawData())))
			}
		}
	}
	return nil
}

func stringsComplement(set, subset []string) []string {
	m := make(map[string]struct{})
	for _, letter := range subset {
		m[letter] = struct{}{}
	}
	var complement []string
	for _, letter := range set {
		if _, exists := m[letter]; !exists {
			complement = append(complement, letter)
		}
	}
	return complement
}
