package reprovider

import (
	"context"
	"crypto/sha256"
	"strconv"
	"sync"
	"time"

	"github.com/ipfs/boxo/provider"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"github.com/probe-lab/go-libdht/kad"
	"github.com/probe-lab/go-libdht/kad/key"
	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
)

var (
	_ provider.Provider    = reprovideSweeper{}
	_ provider.ProvideMany = reprovideSweeper{}
)

type KadRouter interface {
	GetClosestPeers(context.Context, string) ([]peer.ID, error)
	Provide(context.Context, cid.Cid, bool) error
}

// TODO: support resuming reprovide service after a restart

// TODO: add queue of cids waiting to be reprovided (if node offline, etc.)

type reprovideSweeper struct {
	ctx    context.Context
	host   host.Host
	router KadRouter
	order  bit256.Key

	replicationFactor int
	now               func() time.Time
	reprovideInterval time.Duration
	maxReprovideDelay time.Duration

	cids   *trie.Trie[bit256.Key, cid.Cid]
	cidsLk *sync.Mutex
	// TODO: if necessary make val a struct (region)
	schedule   *trie.Trie[bitstr.Key, time.Duration] // time module reprovideInterval
	scheduleLk *sync.Mutex
}

// Options should be
// * reprovideInterval
// * maxReprovideDelay
// * now (maybe not even an option)

func NewReproviderSweeper(ctx context.Context, host host.Host, router KadRouter,
	now func() time.Time, reprovideInterval, maxReprovideDelay time.Duration,
) provider.Provider {
	// TODO:
	return reprovideSweeper{
		host:              host,
		router:            router,
		order:             peerIDToBit256(host.ID()),
		now:               now,
		reprovideInterval: reprovideInterval,
		maxReprovideDelay: maxReprovideDelay,
		cids:              trie.New[bit256.Key, cid.Cid](),
		cidsLk:            &sync.Mutex{},
		schedule:          trie.New[bitstr.Key, time.Duration](),
		scheduleLk:        &sync.Mutex{},
	}
}

// run is only called when the reprovider has its first CIDs to reprovide
func (s reprovideSweeper) run() {
	s.scheduleLk.Lock()
	// we intentionally want to panic if s.schedule is empty
	cursor := trie.Closest(s.schedule, bitstr.Key(key.BitString(s.order)), 1)[0]
	s.scheduleLk.Unlock()

	timer := time.NewTimer(cursor.Data)
	cycleStart := s.now()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
		}
		s.scheduleLk.Lock()
		cursor = nextNonEmptyLeaf(s.schedule, cursor.Key, s.order)
		s.scheduleLk.Unlock()

		s.reprovideForPrefix(cursor.Key)

		timeElapsedInCycle := s.now().Sub(cycleStart) % s.reprovideInterval
		nextReprovideDelay := cursor.Data - timeElapsedInCycle
		timer.Reset(nextReprovideDelay)

		// TODO: add warning if reprovides are failing behind (unlikely, but better check)
	}
}

// TODO: merge & split regions

type region struct {
	prefix bitstr.Key
	peers  *trie.Trie[bit256.Key, peer.ID]
	cids   *trie.Trie[bit256.Key, cid.Cid]
}

// returned regions ordered according to `s.order`
func (s reprovideSweeper) regionsFromPeers(peers []peer.ID) []region {
	peersTrie := trie.New[bit256.Key, peer.ID]()
	for _, p := range peers {
		k := peerIDToBit256(p)
		peersTrie.Add(k, p)
	}
	regions := extractMinimalRegions(peersTrie, "", s.replicationFactor, s.order)
	s.cidsLk.Lock()
	for i, r := range regions {
		t := s.cids.Copy() // TODO: verify if copy is required
		// Navigate to the subtrie matching the prefix
		for i := range r.prefix {
			t = t.Branch(int(r.prefix.Bit(i)))
		}
		regions[i].cids = t
	}
	s.cidsLk.Unlock()
	return regions
}

// returns the list of all non-overlapping subtries of `t` having at least
// `size` elements, sorted according to `order`. every element is included in
// exactly one region.
func extractMinimalRegions(t *trie.Trie[bit256.Key, peer.ID], path bitstr.Key, size int, order bit256.Key) []region {
	if t.IsEmptyLeaf() {
		return nil
	}
	if t.Branch(0).Size() >= size && t.Branch(1).Size() >= size {
		b := int(order.Bit(len(path)))
		return append(extractMinimalRegions(t.Branch(b), path+bitstr.Key(rune('0'+b)), size, order),
			extractMinimalRegions(t.Branch(1-b), path+bitstr.Key(rune('1'-b)), size, order)...)
	}
	return []region{{prefix: path, peers: t}}
}

func (s reprovideSweeper) reprovideForPrefix(prefix bitstr.Key) {
	peers, err := s.closestPeersToPrefix(prefix)
	_ = err // TODO: handle me
	regions := s.regionsFromPeers(peers)
	// TODO: depending on number of regions, merge or split regions for next round
	for _, r := range regions {
		s.regionReprovide(r)
		// TODO: schedule next reprovide occurence
		// TODO: cids should be added to DHT provider store
		// TODO: persist to datastore that region identified by prefix was reprovided `now`
	}
}

func (s reprovideSweeper) closestPeersToPrefix(prefix bitstr.Key) ([]peer.ID, error) {
	// TODO: should return AT LEAST all peers matching the prefix in the network,
	// but if there are less than 20, also return the closest branches of the
	// trie until there are more than 20 nodes.
	return nil, nil
}

func (s reprovideSweeper) regionReprovide(r region) {
	// assume all peers from region are reachable (we connected to them before)
	// we don't try again on failure, skip all missing keys
	cidsAllocations := s.cidsAllocationsToPeers(r)
	for p, cids := range cidsAllocations {
		// TODO: allow some reasonable parallelism
		s.provideCidsToPeer(p, cids)
	}
}

func (s reprovideSweeper) cidsAllocationsToPeers(r region) map[peer.ID][]cid.Cid {
	// TODO: this is a very greedy approach, can be greatly optimized
	keysPerPeer := make(map[peer.ID][]cid.Cid)
	for _, cidEntry := range allKeys(r.cids, s.order) {
		for _, peerEntry := range trie.Closest(r.peers, cidEntry.Key, s.replicationFactor) {
			pid := peerEntry.Data
			if _, ok := keysPerPeer[pid]; !ok {
				keysPerPeer[pid] = []cid.Cid{cidEntry.Data}
			} else {
				keysPerPeer[pid] = append(keysPerPeer[pid], cidEntry.Data)
			}
		}
	}
	return keysPerPeer
}

func (s reprovideSweeper) provideCidsToPeer(p peer.ID, cids []cid.Cid) {
	// TODO: handle this with custom msgSender
	// TODO: maybe allow "some" pipelining?
}

const maxPrefixSize = 30

// reprovideTimeForPrefix calculates the scheduled time offset for reproviding
// keys associated with a given prefix based on its bitstring prefix. The
// function maps the given binary prefix to a fraction of the overall reprovide
// interval (s.reprovideInterval), such that keys with prefixes closer to a
// configured target s.order (in XOR distance) are scheduled earlier and those
// further away later in the cycle.
//
// For any prefix of bit length n, the function generates 2^n distinct
// reprovide times that evenly partition the entire reprovide interval. The
// process first truncates s.order to n bits and then XORs it with the provided
// prefix. The resulting binary string is converted to an integer,
// corresponding to the index of the 2^n possible reprovide times to use for
// the prefix.
//
// This method ensures a deterministic and evenly distributed reprovide
// schedule, where the temporal position within the cycle is based on the
// binary representation of the key's prefix.
func (s reprovideSweeper) reprovideTimeForPrefix(prefix bitstr.Key) time.Duration {
	if len(prefix) == 0 {
		// Empty prefix: all reprovides occur at the beginning of the cycle.
		return 0
	}
	if len(prefix) > maxPrefixSize {
		// Truncate the prefix to the maximum allowed size to avoid overly fine
		// slicing of time.
		prefix = prefix[:maxPrefixSize]
	}
	// Number of possible bitstrings of the same length as prefix.
	maxInt := int64(1 << len(prefix))
	// XOR the prefix with the order key to reorder the schedule: keys "close" to
	// s.order are scheduled first in the cycle, and those "far" from it are
	// scheduled later.
	order := bitstr.Key(key.BitString(s.order)[:len(prefix)])
	k := prefix.Xor(order)
	val, _ := strconv.ParseInt(string(k), 2, 64)
	// Calculate the time offset as a fraction of the overall reprovide interval.
	return time.Duration(int64(s.reprovideInterval) * val / maxInt)
}

func (s reprovideSweeper) Provide(ctx context.Context, c cid.Cid, _ bool) error {
	k := cidToBit256(c)
	s.cidsLk.Lock()
	if added := s.cids.Add(k, c); !added {
		// cid is already being provided
		s.cidsLk.Unlock()
		return nil
	}
	s.cidsLk.Unlock()

	if err := s.router.Provide(ctx, c, true); err != nil {
		// unable to provide the cid, don't reprovide it later
		s.cidsLk.Lock()
		defer s.cidsLk.Unlock()
		s.cids.Remove(k)
		return err
	}

	// TODO: use chan instead of mutex

	// if k isn't part of a scheduled keyspace region, add it to the schedule
	s.scheduleLk.Lock()
	defer s.scheduleLk.Unlock()
	if !trieHasPrefixOfKey(s.schedule, k) {
		bitstrK := bitstr.Key(key.BitString(k))
		// FIXME: this doesn't work, we may need to wake up earlier because of this
		s.schedule.Add(bitstrK, s.reprovideTimeForPrefix(bitstrK))
		if s.schedule.Size() == 1 {
			// first entry added to schedule, start daemon
			go s.run()
		}
	}
	return nil
}

func (s reprovideSweeper) ProvideMany(ctx context.Context, keys []multihash.Multihash) error {
	// TODO: implement me
	return nil
}

func cidToBit256(c cid.Cid) bit256.Key {
	hash := sha256.Sum256(c.Hash())
	return bit256.NewKey(hash[:])
}

func peerIDToBit256(id peer.ID) bit256.Key {
	hash := sha256.Sum256([]byte(id))
	return bit256.NewKey(hash[:])
}

// trieHasPrefixOfKey checks if the trie contains a leave whose key is a prefix
// (or a match) of the provided k
func trieHasPrefixOfKey[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1) bool {
	return trieHasPrefixOfKeyAtDepth(t, k, 0)
}

func trieHasPrefixOfKeyAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K1, depth int) bool {
	if t.IsLeaf() {
		if !t.HasKey() {
			return false
		}
		return key.CommonPrefixLength(*t.Key(), k) == (*t.Key()).BitLen()
	}
	b := int(k.Bit(depth))
	return trieHasPrefixOfKeyAtDepth(t.Branch(b), k, depth+1)
}

// nextNonEmptyLeaf returns the leaf right after the provided key `k` in the
// trie according to the provided `order`.
func nextNonEmptyLeaf[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K0, order K1) trie.Entry[K0, D] {
	return nextNonEmptyLeafAtDepth(t, k, order, 0, false)
}

func nextNonEmptyLeafAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], k K0, order K1, depth int, hitBottom bool) trie.Entry[K0, D] {
	if hitBottom {
		if t.IsNonEmptyLeaf() {
			// Found the next non-empty leaf.
			return trie.Entry[K0, D]{Key: *t.Key(), Data: t.Data()}
		}
		// Going down the trie, looking for next non-empty leaf according to order.
		orderBit := int(order.Bit(depth))
		return nextNonEmptyLeafAtDepth(t.Branch(orderBit), k, order, depth+1, true)
	}

	if t.IsLeaf() {
		// We have reached the bottom of the trie at k or its closest leaf
		if t.HasKey() {
			cpl := k.CommonPrefixLength(*t.Key())
			if cpl < k.BitLen() && cpl < order.BitLen() && order.Bit(cpl) == k.Bit(cpl) {
				// k is closer to order than t.Key, so t.Key AFTER k, return it
				return trie.Entry[K0, D]{Key: *t.Key(), Data: t.Data()}
			}
		}
		return trie.Entry[K0, D]{}
	}
	kBit := int(k.Bit(depth))
	// Recursive call until we hit the bottom of the trie.
	nextLeaf := nextNonEmptyLeafAtDepth(t.Branch(kBit), k, order, depth+1, false)
	if nextLeaf.Key.BitLen() > 0 {
		// Branch has found the next leaf, return it.
		return nextLeaf
	}
	orderBit := int(order.Bit(depth))
	if kBit == orderBit || depth == 0 {
		// Neighbor branch is up next, according to order.
		return nextNonEmptyLeafAtDepth(t.Branch(1-kBit), k, order, depth+1, true)
	}
	// Next leaf not found, signal it to parent by returning an empty entry.
	return trie.Entry[K0, D]{}
}

// allKeys returns a slice containing all keys in the trie `t` sorted according
// to the provided `order`.
func allKeys[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1) []trie.Entry[K0, D] {
	return allKeysAtDepth(t, order, 0)
}

func allKeysAtDepth[K0 kad.Key[K0], K1 kad.Key[K1], D any](t *trie.Trie[K0, D], order K1, depth int) []trie.Entry[K0, D] {
	if t.IsEmptyLeaf() {
		return nil
	}
	if t.IsNonEmptyLeaf() {
		return []trie.Entry[K0, D]{{Key: *t.Key(), Data: t.Data()}}
	}
	b := int(order.Bit(depth))
	return append(allKeysAtDepth(t.Branch(b), order, depth+1),
		allKeysAtDepth(t.Branch(1-b), order, depth+1)...)
}
