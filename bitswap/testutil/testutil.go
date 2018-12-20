package testutil

import (
	"bytes"

	random "github.com/jbenet/go-random"

	bsmsg "github.com/ipfs/go-bitswap/message"
	"github.com/ipfs/go-bitswap/wantlist"
	"github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	peer "github.com/libp2p/go-libp2p-peer"
)

var blockGenerator = blocksutil.NewBlockGenerator()
var prioritySeq int
var seedSeq int64

func randomBytes(n int64, seed int64) []byte {
	data := new(bytes.Buffer)
	random.WritePseudoRandomBytes(n, data, seed)
	return data.Bytes()
}

// GenerateBlocksOfSize generates a series of blocks of the given byte size
func GenerateBlocksOfSize(n int, size int64) []blocks.Block {
	generatedBlocks := make([]blocks.Block, 0, n)
	for i := 0; i < n; i++ {
		seedSeq++
		b := blocks.NewBlock(randomBytes(size, seedSeq))
		generatedBlocks = append(generatedBlocks, b)

	}
	return generatedBlocks
}

// GenerateCids produces n content identifiers.
func GenerateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		cids = append(cids, c)
	}
	return cids
}

// GenerateWantlist makes a populated wantlist.
func GenerateWantlist(n int, ses uint64) *wantlist.ThreadSafe {
	wl := wantlist.NewThreadSafe()
	for i := 0; i < n; i++ {
		prioritySeq++
		entry := wantlist.NewRefEntry(blockGenerator.Next().Cid(), prioritySeq)
		wl.AddEntry(entry, ses)
	}
	return wl
}

// GenerateMessageEntries makes fake bitswap message entries.
func GenerateMessageEntries(n int, isCancel bool) []*bsmsg.Entry {
	bsmsgs := make([]*bsmsg.Entry, 0, n)
	for i := 0; i < n; i++ {
		prioritySeq++
		msg := &bsmsg.Entry{
			Entry:  wantlist.NewRefEntry(blockGenerator.Next().Cid(), prioritySeq),
			Cancel: isCancel,
		}
		bsmsgs = append(bsmsgs, msg)
	}
	return bsmsgs
}

var peerSeq int

// GeneratePeers creates n peer ids.
func GeneratePeers(n int) []peer.ID {
	peerIds := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		peerSeq++
		p := peer.ID(peerSeq)
		peerIds = append(peerIds, p)
	}
	return peerIds
}

var nextSession uint64

// GenerateSessionID make a unit session identifier.
func GenerateSessionID() uint64 {
	nextSession++
	return uint64(nextSession)
}

// ContainsPeer returns true if a peer is found n a list of peers.
func ContainsPeer(peers []peer.ID, p peer.ID) bool {
	for _, n := range peers {
		if p == n {
			return true
		}
	}
	return false
}

// IndexOf returns the index of a given cid in an array of blocks
func IndexOf(blks []blocks.Block, c cid.Cid) int {
	for i, n := range blks {
		if n.Cid() == c {
			return i
		}
	}
	return -1
}

// ContainsBlock returns true if a block is found n a list of blocks
func ContainsBlock(blks []blocks.Block, block blocks.Block) bool {
	return IndexOf(blks, block.Cid()) != -1
}
