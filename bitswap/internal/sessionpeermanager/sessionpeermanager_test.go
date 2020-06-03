package sessionpeermanager

import (
	"sync"
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type fakePeerTagger struct {
	lk             sync.Mutex
	taggedPeers    []peer.ID
	protectedPeers map[peer.ID]map[string]struct{}
	wait           sync.WaitGroup
}

func newFakePeerTagger() *fakePeerTagger {
	return &fakePeerTagger{
		protectedPeers: make(map[peer.ID]map[string]struct{}),
	}
}

func (fpt *fakePeerTagger) TagPeer(p peer.ID, tag string, n int) {
	fpt.wait.Add(1)

	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	fpt.taggedPeers = append(fpt.taggedPeers, p)
}

func (fpt *fakePeerTagger) UntagPeer(p peer.ID, tag string) {
	defer fpt.wait.Done()

	fpt.lk.Lock()
	defer fpt.lk.Unlock()
	for i := 0; i < len(fpt.taggedPeers); i++ {
		if fpt.taggedPeers[i] == p {
			fpt.taggedPeers[i] = fpt.taggedPeers[len(fpt.taggedPeers)-1]
			fpt.taggedPeers = fpt.taggedPeers[:len(fpt.taggedPeers)-1]
			return
		}
	}
}

func (fpt *fakePeerTagger) Protect(p peer.ID, tag string) {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()

	tags, ok := fpt.protectedPeers[p]
	if !ok {
		tags = make(map[string]struct{})
		fpt.protectedPeers[p] = tags
	}
	tags[tag] = struct{}{}
}

func (fpt *fakePeerTagger) Unprotect(p peer.ID, tag string) bool {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()

	if tags, ok := fpt.protectedPeers[p]; ok {
		delete(tags, tag)
		if len(tags) == 0 {
			delete(fpt.protectedPeers, p)
		}
		return len(tags) > 0
	}

	return false
}

func (fpt *fakePeerTagger) isProtected(p peer.ID) bool {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()

	return len(fpt.protectedPeers[p]) > 0
}

func TestAddPeers(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	spm := New(1, &fakePeerTagger{})

	isNew := spm.AddPeer(peers[0])
	if !isNew {
		t.Fatal("Expected peer to be new")
	}

	isNew = spm.AddPeer(peers[0])
	if isNew {
		t.Fatal("Expected peer to no longer be new")
	}

	isNew = spm.AddPeer(peers[1])
	if !isNew {
		t.Fatal("Expected peer to be new")
	}
}

func TestRemovePeers(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	spm := New(1, &fakePeerTagger{})

	existed := spm.RemovePeer(peers[0])
	if existed {
		t.Fatal("Expected peer not to exist")
	}

	spm.AddPeer(peers[0])
	spm.AddPeer(peers[1])

	existed = spm.RemovePeer(peers[0])
	if !existed {
		t.Fatal("Expected peer to exist")
	}
	existed = spm.RemovePeer(peers[1])
	if !existed {
		t.Fatal("Expected peer to exist")
	}
	existed = spm.RemovePeer(peers[0])
	if existed {
		t.Fatal("Expected peer not to have existed")
	}
}

func TestHasPeers(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	spm := New(1, &fakePeerTagger{})

	if spm.HasPeers() {
		t.Fatal("Expected not to have peers yet")
	}

	spm.AddPeer(peers[0])
	if !spm.HasPeers() {
		t.Fatal("Expected to have peers")
	}

	spm.AddPeer(peers[1])
	if !spm.HasPeers() {
		t.Fatal("Expected to have peers")
	}

	spm.RemovePeer(peers[0])
	if !spm.HasPeers() {
		t.Fatal("Expected to have peers")
	}

	spm.RemovePeer(peers[1])
	if spm.HasPeers() {
		t.Fatal("Expected to no longer have peers")
	}
}

func TestHasPeer(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	spm := New(1, &fakePeerTagger{})

	if spm.HasPeer(peers[0]) {
		t.Fatal("Expected not to have peer yet")
	}

	spm.AddPeer(peers[0])
	if !spm.HasPeer(peers[0]) {
		t.Fatal("Expected to have peer")
	}

	spm.AddPeer(peers[1])
	if !spm.HasPeer(peers[1]) {
		t.Fatal("Expected to have peer")
	}

	spm.RemovePeer(peers[0])
	if spm.HasPeer(peers[0]) {
		t.Fatal("Expected not to have peer")
	}

	if !spm.HasPeer(peers[1]) {
		t.Fatal("Expected to have peer")
	}
}

func TestPeers(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	spm := New(1, &fakePeerTagger{})

	if len(spm.Peers()) > 0 {
		t.Fatal("Expected not to have peers yet")
	}

	spm.AddPeer(peers[0])
	if len(spm.Peers()) != 1 {
		t.Fatal("Expected to have one peer")
	}

	spm.AddPeer(peers[1])
	if len(spm.Peers()) != 2 {
		t.Fatal("Expected to have two peers")
	}

	spm.RemovePeer(peers[0])
	if len(spm.Peers()) != 1 {
		t.Fatal("Expected to have one peer")
	}
}

func TestPeersDiscovered(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	spm := New(1, &fakePeerTagger{})

	if spm.PeersDiscovered() {
		t.Fatal("Expected not to have discovered peers yet")
	}

	spm.AddPeer(peers[0])
	if !spm.PeersDiscovered() {
		t.Fatal("Expected to have discovered peers")
	}

	spm.RemovePeer(peers[0])
	if !spm.PeersDiscovered() {
		t.Fatal("Expected to still have discovered peers")
	}
}

func TestPeerTagging(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	fpt := &fakePeerTagger{}
	spm := New(1, fpt)

	spm.AddPeer(peers[0])
	if len(fpt.taggedPeers) != 1 {
		t.Fatal("Expected to have tagged one peer")
	}

	spm.AddPeer(peers[0])
	if len(fpt.taggedPeers) != 1 {
		t.Fatal("Expected to have tagged one peer")
	}

	spm.AddPeer(peers[1])
	if len(fpt.taggedPeers) != 2 {
		t.Fatal("Expected to have tagged two peers")
	}

	spm.RemovePeer(peers[1])
	if len(fpt.taggedPeers) != 1 {
		t.Fatal("Expected to have untagged peer")
	}
}

func TestProtectConnection(t *testing.T) {
	peers := testutil.GeneratePeers(1)
	peerA := peers[0]
	fpt := newFakePeerTagger()
	spm := New(1, fpt)

	// Should not protect connection if peer hasn't been added yet
	spm.ProtectConnection(peerA)
	if fpt.isProtected(peerA) {
		t.Fatal("Expected peer not to be protected")
	}

	// Once peer is added, should be able to protect connection
	spm.AddPeer(peerA)
	spm.ProtectConnection(peerA)
	if !fpt.isProtected(peerA) {
		t.Fatal("Expected peer to be protected")
	}

	// Removing peer should unprotect connection
	spm.RemovePeer(peerA)
	if fpt.isProtected(peerA) {
		t.Fatal("Expected peer to be unprotected")
	}
}

func TestShutdown(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	fpt := newFakePeerTagger()
	spm := New(1, fpt)

	spm.AddPeer(peers[0])
	spm.AddPeer(peers[1])
	if len(fpt.taggedPeers) != 2 {
		t.Fatal("Expected to have tagged two peers")
	}

	spm.ProtectConnection(peers[0])
	if !fpt.isProtected(peers[0]) {
		t.Fatal("Expected peer to be protected")
	}

	spm.Shutdown()

	if len(fpt.taggedPeers) != 0 {
		t.Fatal("Expected to have untagged all peers")
	}
	if len(fpt.protectedPeers) != 0 {
		t.Fatal("Expected to have unprotected all peers")
	}
}
