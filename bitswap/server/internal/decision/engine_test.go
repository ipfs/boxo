package decision

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-libipfs/bitswap/internal/testutil"
	message "github.com/ipfs/go-libipfs/bitswap/message"
	pb "github.com/ipfs/go-libipfs/bitswap/message/pb"
	blocks "github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/internal/test"
	process "github.com/jbenet/goprocess"
	peer "github.com/libp2p/go-libp2p/core/peer"
	libp2ptest "github.com/libp2p/go-libp2p/core/test"
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

func newTestEngine(ctx context.Context, idStr string, opts ...Option) engineSet {
	return newTestEngineWithSampling(ctx, idStr, shortTerm, nil, clock.New(), opts...)
}

func newTestEngineWithSampling(ctx context.Context, idStr string, peerSampleInterval time.Duration, sampleCh chan struct{}, clock clock.Clock, opts ...Option) engineSet {
	fpt := &fakePeerTagger{}
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	e := newEngineForTesting(ctx, bs, fpt, "localhost", 0, append(opts[:len(opts):len(opts)], WithScoreLedger(NewTestScoreLedger(peerSampleInterval, sampleCh, clock)), WithBlockstoreWorkerCount(4))...)
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
	test.Flaky(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := newTestEngine(ctx, "Ernie")
	receiver := newTestEngine(ctx, "Bert")

	// Send messages from Ernie to Bert
	for i := 0; i < 1000; i++ {

		m := message.New(false)
		content := []string{"this", "is", "message", "i"}
		m.AddBlock(blocks.NewBlock([]byte(strings.Join(content, " "))))

		sender.Engine.MessageSent(receiver.Peer, m)
		receiver.Engine.MessageReceived(ctx, sender.Peer, m)
		receiver.Engine.ReceivedBlocks(sender.Peer, m.Blocks())
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
	test.Flaky(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sanfrancisco := newTestEngine(ctx, "sf")
	seattle := newTestEngine(ctx, "sea")

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

func newEngineForTesting(
	ctx context.Context,
	bs blockstore.Blockstore,
	peerTagger PeerTagger,
	self peer.ID,
	maxReplaceSize int,
	opts ...Option,
) *Engine {
	return newEngine(
		ctx,
		bs,
		peerTagger,
		self,
		maxReplaceSize,
		opts...,
	)
}

func TestOutboxClosedWhenEngineClosed(t *testing.T) {
	test.Flaky(t)

	t.SkipNow() // TODO implement *Engine.Close
	ctx := context.Background()
	e := newEngineForTesting(ctx, blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())), &fakePeerTagger{}, "localhost", 0, WithScoreLedger(NewTestScoreLedger(shortTerm, nil, clock.New())), WithBlockstoreWorkerCount(4))
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

func TestPartnerWantHaveWantBlockNonActive(t *testing.T) {
	test.Flaky(t)

	alphabet := "abcdefghijklmnopqrstuvwxyz"
	vowels := "aeiou"

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	for _, letter := range strings.Split(alphabet, "") {
		block := blocks.NewBlock([]byte(letter))
		if err := bs.Put(context.Background(), block); err != nil {
			t.Fatal(err)
		}
	}

	partner := libp2ptest.RandPeerIDFatal(t)
	// partnerWantBlocks(e, vowels, partner)

	type testCaseEntry struct {
		wantBlks     string
		wantHaves    string
		sendDontHave bool
	}

	type testCaseExp struct {
		blks      string
		haves     string
		dontHaves string
	}

	type testCase struct {
		only bool
		wls  []testCaseEntry
		exp  []testCaseExp
	}

	testCases := []testCase{
		// Just send want-blocks
		{
			wls: []testCaseEntry{
				{
					wantBlks:     vowels,
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				{
					blks: vowels,
				},
			},
		},

		// Send want-blocks and want-haves
		{
			wls: []testCaseEntry{
				{
					wantBlks:     vowels,
					wantHaves:    "fgh",
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				{
					blks:  vowels,
					haves: "fgh",
				},
			},
		},

		// Send want-blocks and want-haves, with some want-haves that are not
		// present, but without requesting DONT_HAVES
		{
			wls: []testCaseEntry{
				{
					wantBlks:     vowels,
					wantHaves:    "fgh123",
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				{
					blks:  vowels,
					haves: "fgh",
				},
			},
		},

		// Send want-blocks and want-haves, with some want-haves that are not
		// present, and request DONT_HAVES
		{
			wls: []testCaseEntry{
				{
					wantBlks:     vowels,
					wantHaves:    "fgh123",
					sendDontHave: true,
				},
			},
			exp: []testCaseExp{
				{
					blks:      vowels,
					haves:     "fgh",
					dontHaves: "123",
				},
			},
		},

		// Send want-blocks and want-haves, with some want-blocks and want-haves that are not
		// present, but without requesting DONT_HAVES
		{
			wls: []testCaseEntry{
				{
					wantBlks:     "aeiou123",
					wantHaves:    "fgh456",
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				{
					blks:      "aeiou",
					haves:     "fgh",
					dontHaves: "",
				},
			},
		},

		// Send want-blocks and want-haves, with some want-blocks and want-haves that are not
		// present, and request DONT_HAVES
		{
			wls: []testCaseEntry{
				{
					wantBlks:     "aeiou123",
					wantHaves:    "fgh456",
					sendDontHave: true,
				},
			},
			exp: []testCaseExp{
				{
					blks:      "aeiou",
					haves:     "fgh",
					dontHaves: "123456",
				},
			},
		},

		// Send repeated want-blocks
		{
			wls: []testCaseEntry{
				{
					wantBlks:     "ae",
					sendDontHave: false,
				},
				{
					wantBlks:     "io",
					sendDontHave: false,
				},
				{
					wantBlks:     "u",
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				{
					blks: "aeiou",
				},
			},
		},

		// Send repeated want-blocks and want-haves
		{
			wls: []testCaseEntry{
				{
					wantBlks:     "ae",
					wantHaves:    "jk",
					sendDontHave: false,
				},
				{
					wantBlks:     "io",
					wantHaves:    "lm",
					sendDontHave: false,
				},
				{
					wantBlks:     "u",
					sendDontHave: false,
				},
			},
			exp: []testCaseExp{
				{
					blks:  "aeiou",
					haves: "jklm",
				},
			},
		},

		// Send repeated want-blocks and want-haves, with some want-blocks and want-haves that are not
		// present, and request DONT_HAVES
		{
			wls: []testCaseEntry{
				{
					wantBlks:     "ae12",
					wantHaves:    "jk5",
					sendDontHave: true,
				},
				{
					wantBlks:     "io34",
					wantHaves:    "lm",
					sendDontHave: true,
				},
				{
					wantBlks:     "u",
					wantHaves:    "6",
					sendDontHave: true,
				},
			},
			exp: []testCaseExp{
				{
					blks:      "aeiou",
					haves:     "jklm",
					dontHaves: "123456",
				},
			},
		},

		// Send want-block then want-have for same CID
		{
			wls: []testCaseEntry{
				{
					wantBlks:     "a",
					sendDontHave: true,
				},
				{
					wantHaves:    "a",
					sendDontHave: true,
				},
			},
			// want-have should be ignored because there was already a
			// want-block for the same CID in the queue
			exp: []testCaseExp{
				{
					blks: "a",
				},
			},
		},

		// Send want-have then want-block for same CID
		{
			wls: []testCaseEntry{
				{
					wantHaves:    "b",
					sendDontHave: true,
				},
				{
					wantBlks:     "b",
					sendDontHave: true,
				},
			},
			// want-block should overwrite existing want-have
			exp: []testCaseExp{
				{
					blks: "b",
				},
			},
		},

		// Send want-block then want-block for same CID
		{
			wls: []testCaseEntry{
				{
					wantBlks:     "a",
					sendDontHave: true,
				},
				{
					wantBlks:     "a",
					sendDontHave: true,
				},
			},
			// second want-block should be ignored
			exp: []testCaseExp{
				{
					blks: "a",
				},
			},
		},

		// Send want-have then want-have for same CID
		{
			wls: []testCaseEntry{
				{
					wantHaves:    "a",
					sendDontHave: true,
				},
				{
					wantHaves:    "a",
					sendDontHave: true,
				},
			},
			// second want-have should be ignored
			exp: []testCaseExp{
				{
					haves: "a",
				},
			},
		},
	}

	var onlyTestCases []testCase
	for _, testCase := range testCases {
		if testCase.only {
			onlyTestCases = append(onlyTestCases, testCase)
		}
	}
	if len(onlyTestCases) > 0 {
		testCases = onlyTestCases
	}

	ctx := context.Background()
	e := newEngineForTesting(ctx, bs, &fakePeerTagger{}, "localhost", 0, WithScoreLedger(NewTestScoreLedger(shortTerm, nil, clock.New())), WithBlockstoreWorkerCount(4))
	e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))
	for i, testCase := range testCases {
		t.Logf("Test case %d:", i)
		for _, wl := range testCase.wls {
			t.Logf("  want-blocks '%s' / want-haves '%s' / sendDontHave %t",
				wl.wantBlks, wl.wantHaves, wl.sendDontHave)
			wantBlks := strings.Split(wl.wantBlks, "")
			wantHaves := strings.Split(wl.wantHaves, "")
			partnerWantBlocksHaves(e, wantBlks, wantHaves, wl.sendDontHave, partner)
		}

		for _, exp := range testCase.exp {
			expBlks := strings.Split(exp.blks, "")
			expHaves := strings.Split(exp.haves, "")
			expDontHaves := strings.Split(exp.dontHaves, "")

			next := <-e.Outbox()
			env := <-next
			err := checkOutput(t, e, env, expBlks, expHaves, expDontHaves)
			if err != nil {
				t.Fatal(err)
			}
			env.Sent()
		}
	}
}

func TestPartnerWantHaveWantBlockActive(t *testing.T) {
	test.Flaky(t)

	alphabet := "abcdefghijklmnopqrstuvwxyz"

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	for _, letter := range strings.Split(alphabet, "") {
		block := blocks.NewBlock([]byte(letter))
		if err := bs.Put(context.Background(), block); err != nil {
			t.Fatal(err)
		}
	}

	partner := libp2ptest.RandPeerIDFatal(t)

	type testCaseEntry struct {
		wantBlks     string
		wantHaves    string
		sendDontHave bool
	}

	type testCaseExp struct {
		blks      string
		haves     string
		dontHaves string
	}

	type testCase struct {
		only bool
		wls  []testCaseEntry
		exp  []testCaseExp
	}

	testCases := []testCase{
		// Send want-block then want-have for same CID
		{
			wls: []testCaseEntry{
				{
					wantBlks:     "a",
					sendDontHave: true,
				},
				{
					wantHaves:    "a",
					sendDontHave: true,
				},
			},
			// want-have should be ignored because there was already a
			// want-block for the same CID in the queue
			exp: []testCaseExp{
				{
					blks: "a",
				},
			},
		},

		// Send want-have then want-block for same CID
		{
			wls: []testCaseEntry{
				{
					wantHaves:    "b",
					sendDontHave: true,
				},
				{
					wantBlks:     "b",
					sendDontHave: true,
				},
			},
			// want-have is active when want-block is added, so want-have
			// should get sent, then want-block
			exp: []testCaseExp{
				{
					haves: "b",
				},
				{
					blks: "b",
				},
			},
		},

		// Send want-block then want-block for same CID
		{
			wls: []testCaseEntry{
				{
					wantBlks:     "a",
					sendDontHave: true,
				},
				{
					wantBlks:     "a",
					sendDontHave: true,
				},
			},
			// second want-block should be ignored
			exp: []testCaseExp{
				{
					blks: "a",
				},
			},
		},

		// Send want-have then want-have for same CID
		{
			wls: []testCaseEntry{
				{
					wantHaves:    "a",
					sendDontHave: true,
				},
				{
					wantHaves:    "a",
					sendDontHave: true,
				},
			},
			// second want-have should be ignored
			exp: []testCaseExp{
				{
					haves: "a",
				},
			},
		},
	}

	var onlyTestCases []testCase
	for _, testCase := range testCases {
		if testCase.only {
			onlyTestCases = append(onlyTestCases, testCase)
		}
	}
	if len(onlyTestCases) > 0 {
		testCases = onlyTestCases
	}

	ctx := context.Background()
	e := newEngineForTesting(ctx, bs, &fakePeerTagger{}, "localhost", 0, WithScoreLedger(NewTestScoreLedger(shortTerm, nil, clock.New())), WithBlockstoreWorkerCount(4))
	e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))

	var next envChan
	for i, testCase := range testCases {
		envs := make([]*Envelope, 0)

		t.Logf("Test case %d:", i)
		for _, wl := range testCase.wls {
			t.Logf("  want-blocks '%s' / want-haves '%s' / sendDontHave %t",
				wl.wantBlks, wl.wantHaves, wl.sendDontHave)
			wantBlks := strings.Split(wl.wantBlks, "")
			wantHaves := strings.Split(wl.wantHaves, "")
			partnerWantBlocksHaves(e, wantBlks, wantHaves, wl.sendDontHave, partner)

			var env *Envelope
			next, env = getNextEnvelope(e, next, 5*time.Millisecond)
			if env != nil {
				envs = append(envs, env)
			}
		}

		if len(envs) != len(testCase.exp) {
			t.Fatalf("Expected %d envelopes but received %d", len(testCase.exp), len(envs))
		}

		for i, exp := range testCase.exp {
			expBlks := strings.Split(exp.blks, "")
			expHaves := strings.Split(exp.haves, "")
			expDontHaves := strings.Split(exp.dontHaves, "")

			err := checkOutput(t, e, envs[i], expBlks, expHaves, expDontHaves)
			if err != nil {
				t.Fatal(err)
			}
			envs[i].Sent()
		}
	}
}

func checkOutput(t *testing.T, e *Engine, envelope *Envelope, expBlks []string, expHaves []string, expDontHaves []string) error {
	blks := envelope.Message.Blocks()
	presences := envelope.Message.BlockPresences()

	// Verify payload message length
	if len(blks) != len(expBlks) {
		blkDiff := formatBlocksDiff(blks, expBlks)
		msg := fmt.Sprintf("Received %d blocks. Expected %d blocks:\n%s", len(blks), len(expBlks), blkDiff)
		return errors.New(msg)
	}

	// Verify block presences message length
	expPresencesCount := len(expHaves) + len(expDontHaves)
	if len(presences) != expPresencesCount {
		presenceDiff := formatPresencesDiff(presences, expHaves, expDontHaves)
		return fmt.Errorf("Received %d BlockPresences. Expected %d BlockPresences:\n%s",
			len(presences), expPresencesCount, presenceDiff)
	}

	// Verify payload message contents
	for _, k := range expBlks {
		found := false
		expected := blocks.NewBlock([]byte(k))
		for _, block := range blks {
			if block.Cid().Equals(expected.Cid()) {
				found = true
				break
			}
		}
		if !found {
			return errors.New(formatBlocksDiff(blks, expBlks))
		}
	}

	// Verify HAVEs
	if err := checkPresence(presences, expHaves, pb.Message_Have); err != nil {
		return errors.New(formatPresencesDiff(presences, expHaves, expDontHaves))
	}

	// Verify DONT_HAVEs
	if err := checkPresence(presences, expDontHaves, pb.Message_DontHave); err != nil {
		return errors.New(formatPresencesDiff(presences, expHaves, expDontHaves))
	}

	return nil
}

func checkPresence(presences []message.BlockPresence, expPresence []string, presenceType pb.Message_BlockPresenceType) error {
	for _, k := range expPresence {
		found := false
		expected := blocks.NewBlock([]byte(k))
		for _, p := range presences {
			if p.Cid.Equals(expected.Cid()) {
				found = true
				if p.Type != presenceType {
					return errors.New("type mismatch")
				}
				break
			}
		}
		if !found {
			return errors.New("not found")
		}
	}
	return nil
}

func formatBlocksDiff(blks []blocks.Block, expBlks []string) string {
	var out bytes.Buffer
	out.WriteString(fmt.Sprintf("Blocks (%d):\n", len(blks)))
	for _, b := range blks {
		out.WriteString(fmt.Sprintf("  %s: %s\n", b.Cid(), b.RawData()))
	}
	out.WriteString(fmt.Sprintf("Expected (%d):\n", len(expBlks)))
	for _, k := range expBlks {
		expected := blocks.NewBlock([]byte(k))
		out.WriteString(fmt.Sprintf("  %s: %s\n", expected.Cid(), k))
	}
	return out.String()
}

func formatPresencesDiff(presences []message.BlockPresence, expHaves []string, expDontHaves []string) string {
	var out bytes.Buffer
	out.WriteString(fmt.Sprintf("BlockPresences (%d):\n", len(presences)))
	for _, p := range presences {
		t := "HAVE"
		if p.Type == pb.Message_DontHave {
			t = "DONT_HAVE"
		}
		out.WriteString(fmt.Sprintf("  %s - %s\n", p.Cid, t))
	}
	out.WriteString(fmt.Sprintf("Expected (%d):\n", len(expHaves)+len(expDontHaves)))
	for _, k := range expHaves {
		expected := blocks.NewBlock([]byte(k))
		out.WriteString(fmt.Sprintf("  %s: %s - HAVE\n", expected.Cid(), k))
	}
	for _, k := range expDontHaves {
		expected := blocks.NewBlock([]byte(k))
		out.WriteString(fmt.Sprintf("  %s: %s - DONT_HAVE\n", expected.Cid(), k))
	}
	return out.String()
}

func TestPartnerWantsThenCancels(t *testing.T) {
	test.Flaky(t)

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
		if err := bs.Put(context.Background(), block); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()
	for i := 0; i < numRounds; i++ {
		expected := make([][]string, 0, len(testcases))
		e := newEngineForTesting(ctx, bs, &fakePeerTagger{}, "localhost", 0, WithScoreLedger(NewTestScoreLedger(shortTerm, nil, clock.New())), WithBlockstoreWorkerCount(4))
		e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))
		for _, testcase := range testcases {
			set := testcase[0]
			cancels := testcase[1]
			keeps := stringsComplement(set, cancels)
			expected = append(expected, keeps)

			partner := libp2ptest.RandPeerIDFatal(t)

			partnerWantBlocks(e, set, partner)
			partnerCancels(e, cancels, partner)
		}
		if err := checkHandledInOrder(t, e, expected); err != nil {
			t.Logf("run #%d of %d", i, numRounds)
			t.Fatal(err)
		}
	}
}

func TestSendReceivedBlocksToPeersThatWantThem(t *testing.T) {
	test.Flaky(t)

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	partner := libp2ptest.RandPeerIDFatal(t)
	otherPeer := libp2ptest.RandPeerIDFatal(t)

	ctx := context.Background()
	e := newEngineForTesting(ctx, bs, &fakePeerTagger{}, "localhost", 0, WithScoreLedger(NewTestScoreLedger(shortTerm, nil, clock.New())), WithBlockstoreWorkerCount(4))
	e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))

	blks := testutil.GenerateBlocksOfSize(4, 8*1024)
	msg := message.New(false)
	msg.AddEntry(blks[0].Cid(), 4, pb.Message_Wantlist_Have, false)
	msg.AddEntry(blks[1].Cid(), 3, pb.Message_Wantlist_Have, false)
	msg.AddEntry(blks[2].Cid(), 2, pb.Message_Wantlist_Block, false)
	msg.AddEntry(blks[3].Cid(), 1, pb.Message_Wantlist_Block, false)
	e.MessageReceived(context.Background(), partner, msg)

	// Nothing in blockstore, so shouldn't get any envelope
	var next envChan
	next, env := getNextEnvelope(e, next, 5*time.Millisecond)
	if env != nil {
		t.Fatal("expected no envelope yet")
	}

	e.ReceivedBlocks(otherPeer, []blocks.Block{blks[0], blks[2]})
	if err := bs.PutMany(context.Background(), []blocks.Block{blks[0], blks[2]}); err != nil {
		t.Fatal(err)
	}
	e.NotifyNewBlocks([]blocks.Block{blks[0], blks[2]})
	_, env = getNextEnvelope(e, next, 5*time.Millisecond)
	if env == nil {
		t.Fatal("expected envelope")
	}
	if env.Peer != partner {
		t.Fatal("expected message to peer")
	}
	sentBlk := env.Message.Blocks()
	if len(sentBlk) != 1 || !sentBlk[0].Cid().Equals(blks[2].Cid()) {
		t.Fatal("expected 1 block")
	}
	sentHave := env.Message.BlockPresences()
	if len(sentHave) != 1 || !sentHave[0].Cid.Equals(blks[0].Cid()) || sentHave[0].Type != pb.Message_Have {
		t.Fatal("expected 1 HAVE")
	}
}

func TestSendDontHave(t *testing.T) {
	test.Flaky(t)

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	partner := libp2ptest.RandPeerIDFatal(t)
	otherPeer := libp2ptest.RandPeerIDFatal(t)

	ctx := context.Background()
	e := newEngineForTesting(ctx, bs, &fakePeerTagger{}, "localhost", 0, WithScoreLedger(NewTestScoreLedger(shortTerm, nil, clock.New())), WithBlockstoreWorkerCount(4))
	e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))

	blks := testutil.GenerateBlocksOfSize(4, 8*1024)
	msg := message.New(false)
	msg.AddEntry(blks[0].Cid(), 4, pb.Message_Wantlist_Have, false)
	msg.AddEntry(blks[1].Cid(), 3, pb.Message_Wantlist_Have, true)
	msg.AddEntry(blks[2].Cid(), 2, pb.Message_Wantlist_Block, false)
	msg.AddEntry(blks[3].Cid(), 1, pb.Message_Wantlist_Block, true)
	e.MessageReceived(context.Background(), partner, msg)

	// Nothing in blockstore, should get DONT_HAVE for entries that wanted it
	var next envChan
	next, env := getNextEnvelope(e, next, 10*time.Millisecond)
	if env == nil {
		t.Fatal("expected envelope")
	}
	if env.Peer != partner {
		t.Fatal("expected message to peer")
	}
	if len(env.Message.Blocks()) > 0 {
		t.Fatal("expected no blocks")
	}
	sentDontHaves := env.Message.BlockPresences()
	if len(sentDontHaves) != 2 {
		t.Fatal("expected 2 DONT_HAVEs")
	}
	if !sentDontHaves[0].Cid.Equals(blks[1].Cid()) &&
		!sentDontHaves[1].Cid.Equals(blks[1].Cid()) {
		t.Fatal("expected DONT_HAVE for want-have")
	}
	if !sentDontHaves[0].Cid.Equals(blks[3].Cid()) &&
		!sentDontHaves[1].Cid.Equals(blks[3].Cid()) {
		t.Fatal("expected DONT_HAVE for want-block")
	}

	// Receive all the blocks
	e.ReceivedBlocks(otherPeer, []blocks.Block{blks[0], blks[2]})
	if err := bs.PutMany(context.Background(), blks); err != nil {
		t.Fatal(err)
	}
	e.NotifyNewBlocks(blks)

	// Envelope should contain 2 HAVEs / 2 blocks
	_, env = getNextEnvelope(e, next, 10*time.Millisecond)
	if env == nil {
		t.Fatal("expected envelope")
	}
	if env.Peer != partner {
		t.Fatal("expected message to peer")
	}
	if len(env.Message.Blocks()) != 2 {
		t.Fatal("expected 2 blocks")
	}
	sentHave := env.Message.BlockPresences()
	if len(sentHave) != 2 || sentHave[0].Type != pb.Message_Have || sentHave[1].Type != pb.Message_Have {
		t.Fatal("expected 2 HAVEs")
	}
}

func TestWantlistForPeer(t *testing.T) {
	test.Flaky(t)

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	partner := libp2ptest.RandPeerIDFatal(t)
	otherPeer := libp2ptest.RandPeerIDFatal(t)

	ctx := context.Background()
	e := newEngineForTesting(ctx, bs, &fakePeerTagger{}, "localhost", 0, WithScoreLedger(NewTestScoreLedger(shortTerm, nil, clock.New())), WithBlockstoreWorkerCount(4))
	e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))

	blks := testutil.GenerateBlocksOfSize(4, 8*1024)
	msg := message.New(false)
	msg.AddEntry(blks[0].Cid(), 2, pb.Message_Wantlist_Have, false)
	msg.AddEntry(blks[1].Cid(), 3, pb.Message_Wantlist_Have, false)
	e.MessageReceived(context.Background(), partner, msg)

	msg2 := message.New(false)
	msg2.AddEntry(blks[2].Cid(), 1, pb.Message_Wantlist_Block, false)
	msg2.AddEntry(blks[3].Cid(), 4, pb.Message_Wantlist_Block, false)
	e.MessageReceived(context.Background(), partner, msg2)

	entries := e.WantlistForPeer(otherPeer)
	if len(entries) != 0 {
		t.Fatal("expected wantlist to contain no wants for other peer")
	}

	entries = e.WantlistForPeer(partner)
	if len(entries) != 4 {
		t.Fatal("expected wantlist to contain all wants from parter")
	}
	if entries[0].Priority != 4 || entries[1].Priority != 3 || entries[2].Priority != 2 || entries[3].Priority != 1 {
		t.Fatal("expected wantlist to be sorted")
	}

}

func TestTaskComparator(t *testing.T) {
	test.Flaky(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	cids := make(map[cid.Cid]int)
	blks := make([]blocks.Block, 0, len(keys))
	for i, letter := range keys {
		block := blocks.NewBlock([]byte(letter))
		blks = append(blks, block)
		cids[block.Cid()] = i
	}

	fpt := &fakePeerTagger{}
	sl := NewTestScoreLedger(shortTerm, nil, clock.New())
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	if err := bs.PutMany(ctx, blks); err != nil {
		t.Fatal(err)
	}

	// use a single task worker so that the order of outgoing messages is deterministic
	e := newEngineForTesting(ctx, bs, fpt, "localhost", 0, WithScoreLedger(sl), WithBlockstoreWorkerCount(4), WithTaskWorkerCount(1),
		// if this Option is omitted, the test fails
		WithTaskComparator(func(ta, tb *TaskInfo) bool {
			// prioritize based on lexicographic ordering of block content
			return cids[ta.Cid] < cids[tb.Cid]
		}),
	)
	e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))

	// rely on randomness of Go map's iteration order to add Want entries in random order
	peerIDs := make([]peer.ID, len(keys))
	for _, i := range cids {
		peerID := libp2ptest.RandPeerIDFatal(t)
		peerIDs[i] = peerID
		partnerWantBlocks(e, keys[i:i+1], peerID)
	}

	// check that outgoing messages are sent in the correct order
	for i, peerID := range peerIDs {
		next := <-e.Outbox()
		envelope := <-next
		if peerID != envelope.Peer {
			t.Errorf("expected message for peer ID %#v but instead got message for peer ID %#v", peerID, envelope.Peer)
		}
		responseBlocks := envelope.Message.Blocks()
		if len(responseBlocks) != 1 {
			t.Errorf("expected 1 block in response but instead got %v", len(blks))
		} else if responseBlocks[0].Cid() != blks[i].Cid() {
			t.Errorf("expected block with CID %#v but instead got block with CID %#v", blks[i].Cid(), responseBlocks[0].Cid())
		}
	}
}

func TestPeerBlockFilter(t *testing.T) {
	test.Flaky(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Generate a few keys
	keys := []string{"a", "b", "c", "d"}
	blks := make([]blocks.Block, 0, len(keys))
	for _, letter := range keys {
		block := blocks.NewBlock([]byte(letter))
		blks = append(blks, block)
	}

	// Generate a few partner peers
	peerIDs := make([]peer.ID, 3)
	peerIDs[0] = libp2ptest.RandPeerIDFatal(t)
	peerIDs[1] = libp2ptest.RandPeerIDFatal(t)
	peerIDs[2] = libp2ptest.RandPeerIDFatal(t)

	// Setup the main peer
	fpt := &fakePeerTagger{}
	sl := NewTestScoreLedger(shortTerm, nil, clock.New())
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	if err := bs.PutMany(ctx, blks); err != nil {
		t.Fatal(err)
	}

	e := newEngineForTesting(ctx, bs, fpt, "localhost", 0, WithScoreLedger(sl), WithBlockstoreWorkerCount(4),
		WithPeerBlockRequestFilter(func(p peer.ID, c cid.Cid) bool {
			// peer 0 has access to everything
			if p == peerIDs[0] {
				return true
			}
			// peer 1 can only access key c and d
			if p == peerIDs[1] {
				return blks[2].Cid().Equals(c) || blks[3].Cid().Equals(c)
			}
			// peer 2 and other can only access key d
			return blks[3].Cid().Equals(c)
		}),
	)
	e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))

	// Setup the test
	type testCaseEntry struct {
		peerIndex int
		wantBlks  string
		wantHaves string
	}

	type testCaseExp struct {
		blks      string
		haves     string
		dontHaves string
	}

	type testCase struct {
		only bool
		wl   testCaseEntry
		exp  testCaseExp
	}

	testCases := []testCase{
		// Peer 0 has access to everything: want-block `a` succeeds.
		{
			wl: testCaseEntry{
				peerIndex: 0,
				wantBlks:  "a",
			},
			exp: testCaseExp{
				blks: "a",
			},
		},
		// Peer 0 has access to everything: want-have `b` succeeds.
		{
			wl: testCaseEntry{
				peerIndex: 0,
				wantHaves: "b1",
			},
			exp: testCaseExp{
				haves:     "b",
				dontHaves: "1",
			},
		},
		// Peer 1 has access to [c, d]: want-have `a` result in dont-have.
		{
			wl: testCaseEntry{
				peerIndex: 1,
				wantHaves: "ac",
			},
			exp: testCaseExp{
				haves:     "c",
				dontHaves: "a",
			},
		},
		// Peer 1 has access to [c, d]: want-block `b` result in dont-have.
		{
			wl: testCaseEntry{
				peerIndex: 1,
				wantBlks:  "bd",
			},
			exp: testCaseExp{
				blks:      "d",
				dontHaves: "b",
			},
		},
		// Peer 2 has access to [d]: want-have `a` and want-block `b` result in dont-have.
		{
			wl: testCaseEntry{
				peerIndex: 2,
				wantHaves: "a",
				wantBlks:  "bcd1",
			},
			exp: testCaseExp{
				haves:     "",
				blks:      "d",
				dontHaves: "abc1",
			},
		},
	}

	var onlyTestCases []testCase
	for _, testCase := range testCases {
		if testCase.only {
			onlyTestCases = append(onlyTestCases, testCase)
		}
	}
	if len(onlyTestCases) > 0 {
		testCases = onlyTestCases
	}

	for i, testCase := range testCases {
		// Create wants requests
		wl := testCase.wl

		t.Logf("test case %v: Peer%v / want-blocks '%s' / want-haves '%s'",
			i, wl.peerIndex, wl.wantBlks, wl.wantHaves)

		wantBlks := strings.Split(wl.wantBlks, "")
		wantHaves := strings.Split(wl.wantHaves, "")

		partnerWantBlocksHaves(e, wantBlks, wantHaves, true, peerIDs[wl.peerIndex])

		// Check result
		exp := testCase.exp

		next := <-e.Outbox()
		envelope := <-next

		expBlks := strings.Split(exp.blks, "")
		expHaves := strings.Split(exp.haves, "")
		expDontHaves := strings.Split(exp.dontHaves, "")

		err := checkOutput(t, e, envelope, expBlks, expHaves, expDontHaves)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPeerBlockFilterMutability(t *testing.T) {
	test.Flaky(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Generate a few keys
	keys := []string{"a", "b", "c", "d"}
	blks := make([]blocks.Block, 0, len(keys))
	for _, letter := range keys {
		block := blocks.NewBlock([]byte(letter))
		blks = append(blks, block)
	}

	partnerID := libp2ptest.RandPeerIDFatal(t)

	// Setup the main peer
	fpt := &fakePeerTagger{}
	sl := NewTestScoreLedger(shortTerm, nil, clock.New())
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	if err := bs.PutMany(ctx, blks); err != nil {
		t.Fatal(err)
	}

	filterAllowList := make(map[cid.Cid]bool)

	e := newEngineForTesting(ctx, bs, fpt, "localhost", 0, WithScoreLedger(sl), WithBlockstoreWorkerCount(4),
		WithPeerBlockRequestFilter(func(p peer.ID, c cid.Cid) bool {
			return filterAllowList[c]
		}),
	)
	e.StartWorkers(ctx, process.WithTeardown(func() error { return nil }))

	// Setup the test
	type testCaseEntry struct {
		allowList string
		wantBlks  string
		wantHaves string
	}

	type testCaseExp struct {
		blks      string
		haves     string
		dontHaves string
	}

	type testCase struct {
		only bool
		wls  []testCaseEntry
		exps []testCaseExp
	}

	testCases := []testCase{
		{
			wls: []testCaseEntry{
				{
					// Peer has no accesses & request a want-block
					allowList: "",
					wantBlks:  "a",
				},
				{
					// Then Peer is allowed access to a
					allowList: "a",
					wantBlks:  "a",
				},
			},
			exps: []testCaseExp{
				{
					dontHaves: "a",
				},
				{
					blks: "a",
				},
			},
		},
		{
			wls: []testCaseEntry{
				{
					// Peer has access to bc
					allowList: "bc",
					wantHaves: "bc",
				},
				{
					// Then Peer loses access to b
					allowList: "c",
					wantBlks:  "bc", // Note: We request a block here to force a response from the node
				},
			},
			exps: []testCaseExp{
				{
					haves: "bc",
				},
				{
					blks:      "c",
					dontHaves: "b",
				},
			},
		},
		{
			wls: []testCaseEntry{
				{
					// Peer has no accesses & request a want-have
					allowList: "",
					wantHaves: "d",
				},
				{
					// Then Peer gains access to d
					allowList: "d",
					wantHaves: "d",
				},
			},
			exps: []testCaseExp{
				{
					dontHaves: "d",
				},
				{
					haves: "d",
				},
			},
		},
	}

	var onlyTestCases []testCase
	for _, testCase := range testCases {
		if testCase.only {
			onlyTestCases = append(onlyTestCases, testCase)
		}
	}
	if len(onlyTestCases) > 0 {
		testCases = onlyTestCases
	}

	for i, testCase := range testCases {
		for j := range testCase.wls {
			wl := testCase.wls[j]
			exp := testCase.exps[j]

			// Create wants requests
			t.Logf("test case %v, %v: allow-list '%s' / want-blocks '%s' / want-haves '%s'",
				i, j, wl.allowList, wl.wantBlks, wl.wantHaves)

			allowList := strings.Split(wl.allowList, "")
			wantBlks := strings.Split(wl.wantBlks, "")
			wantHaves := strings.Split(wl.wantHaves, "")

			// Update the allow list
			filterAllowList = make(map[cid.Cid]bool)
			for _, letter := range allowList {
				block := blocks.NewBlock([]byte(letter))
				filterAllowList[block.Cid()] = true
			}

			// Send the request
			partnerWantBlocksHaves(e, wantBlks, wantHaves, true, partnerID)

			// Check result
			next := <-e.Outbox()
			envelope := <-next

			expBlks := strings.Split(exp.blks, "")
			expHaves := strings.Split(exp.haves, "")
			expDontHaves := strings.Split(exp.dontHaves, "")

			err := checkOutput(t, e, envelope, expBlks, expHaves, expDontHaves)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestTaggingPeers(t *testing.T) {
	test.Flaky(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	sanfrancisco := newTestEngine(ctx, "sf")
	seattle := newTestEngine(ctx, "sea")

	keys := []string{"a", "b", "c", "d", "e"}
	for _, letter := range keys {
		block := blocks.NewBlock([]byte(letter))
		if err := sanfrancisco.Blockstore.Put(context.Background(), block); err != nil {
			t.Fatal(err)
		}
	}
	partnerWantBlocks(sanfrancisco.Engine, keys, seattle.Peer)
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
	test.Flaky(t)

	peerSampleIntervalHalf := 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sampleCh := make(chan struct{})
	mockClock := clock.NewMock()
	me := newTestEngineWithSampling(ctx, "engine", peerSampleIntervalHalf*2, sampleCh, mockClock)
	mockClock.Add(1 * time.Millisecond)
	friend := peer.ID("friend")

	block := blocks.NewBlock([]byte("foobar"))
	msg := message.New(false)
	msg.AddBlock(block)

	for i := 0; i < 3; i++ {
		if untagged := me.PeerTagger.count(me.Engine.tagUseful); untagged != 0 {
			t.Fatalf("%d peers should be untagged but weren't", untagged)
		}
		mockClock.Add(peerSampleIntervalHalf)
		me.Engine.MessageSent(friend, msg)

		mockClock.Add(peerSampleIntervalHalf)
		<-sampleCh

		if tagged := me.PeerTagger.count(me.Engine.tagUseful); tagged != 1 {
			t.Fatalf("1 peer should be tagged, but %d were", tagged)
		}

		for j := 0; j < longTermRatio; j++ {
			mockClock.Add(peerSampleIntervalHalf * 2)
			<-sampleCh
		}
	}

	if me.PeerTagger.count(me.Engine.tagUseful) == 0 {
		t.Fatal("peers should still be tagged due to long-term usefulness")
	}

	for j := 0; j < longTermRatio; j++ {
		mockClock.Add(peerSampleIntervalHalf * 2)
		<-sampleCh
	}

	if me.PeerTagger.count(me.Engine.tagUseful) == 0 {
		t.Fatal("peers should still be tagged due to long-term usefulness")
	}

	for j := 0; j < longTermRatio; j++ {
		mockClock.Add(peerSampleIntervalHalf * 2)
		<-sampleCh
	}

	if me.PeerTagger.count(me.Engine.tagUseful) != 0 {
		t.Fatal("peers should finally be untagged")
	}
}

func partnerWantBlocks(e *Engine, wantBlocks []string, partner peer.ID) {
	add := message.New(false)
	for i, letter := range wantBlocks {
		block := blocks.NewBlock([]byte(letter))
		add.AddEntry(block.Cid(), int32(len(wantBlocks)-i), pb.Message_Wantlist_Block, true)
	}
	e.MessageReceived(context.Background(), partner, add)
}

func partnerWantBlocksHaves(e *Engine, wantBlocks []string, wantHaves []string, sendDontHave bool, partner peer.ID) {
	add := message.New(false)
	priority := int32(len(wantHaves) + len(wantBlocks))
	for _, letter := range wantHaves {
		block := blocks.NewBlock([]byte(letter))
		add.AddEntry(block.Cid(), priority, pb.Message_Wantlist_Have, sendDontHave)
		priority--
	}
	for _, letter := range wantBlocks {
		block := blocks.NewBlock([]byte(letter))
		add.AddEntry(block.Cid(), priority, pb.Message_Wantlist_Block, sendDontHave)
		priority--
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

type envChan <-chan *Envelope

func getNextEnvelope(e *Engine, next envChan, t time.Duration) (envChan, *Envelope) {
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()

	if next == nil {
		next = <-e.Outbox() // returns immediately
	}

	select {
	case env, ok := <-next: // blocks till next envelope ready
		if !ok {
			log.Warnf("got closed channel")
			return nil, nil
		}
		return nil, env
	case <-ctx.Done():
		// log.Warnf("got timeout")
	}
	return next, nil
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
