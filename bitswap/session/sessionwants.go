package session

import (
	"math/rand"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
)

type sessionWants struct {
	sync.RWMutex
	toFetch   *cidQueue
	liveWants map[cid.Cid]time.Time
	pastWants *cid.Set
}

// BlocksReceived moves received block CIDs from live to past wants and
// measures latency. It returns the CIDs of blocks that were actually wanted
// (as opposed to duplicates) and the total latency for all incoming blocks.
func (sw *sessionWants) BlocksReceived(cids []cid.Cid) ([]cid.Cid, time.Duration) {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	totalLatency := time.Duration(0)
	wanted := make([]cid.Cid, 0, len(cids))
	for _, c := range cids {
		if sw.unlockedIsWanted(c) {
			wanted = append(wanted, c)

			// If the block CID was in the live wants queue, remove it
			tval, ok := sw.liveWants[c]
			if ok {
				totalLatency += now.Sub(tval)
				delete(sw.liveWants, c)
			} else {
				// Otherwise remove it from the toFetch queue, if it was there
				sw.toFetch.Remove(c)
			}

			// Keep track of CIDs we've successfully fetched
			sw.pastWants.Add(c)
		}
	}

	return wanted, totalLatency
}

// GetNextWants adds any new wants to the list of CIDs to fetch, then moves as
// many CIDs from the fetch queue to the live wants list as possible (given the
// limit). Returns the newly live wants.
func (sw *sessionWants) GetNextWants(limit int, newWants []cid.Cid) []cid.Cid {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	// Add new wants to the fetch queue
	for _, k := range newWants {
		sw.toFetch.Push(k)
	}

	// Move CIDs from fetch queue to the live wants queue (up to the limit)
	currentLiveCount := len(sw.liveWants)
	toAdd := limit - currentLiveCount

	var live []cid.Cid
	for ; toAdd > 0 && sw.toFetch.Len() > 0; toAdd-- {
		c := sw.toFetch.Pop()
		live = append(live, c)
		sw.liveWants[c] = now
	}

	return live
}

// PrepareBroadcast saves the current time for each live want and returns the
// live want CIDs.
func (sw *sessionWants) PrepareBroadcast() []cid.Cid {
	now := time.Now()

	sw.Lock()
	defer sw.Unlock()

	live := make([]cid.Cid, 0, len(sw.liveWants))
	for c := range sw.liveWants {
		live = append(live, c)
		sw.liveWants[c] = now
	}
	return live
}

// CancelPending removes the given CIDs from the fetch queue.
func (sw *sessionWants) CancelPending(keys []cid.Cid) {
	sw.Lock()
	defer sw.Unlock()

	for _, k := range keys {
		sw.toFetch.Remove(k)
	}
}

// ForEachUniqDup iterates over each of the given CIDs and calls isUniqFn
// if the session is expecting a block for the CID, or isDupFn if the session
// has already received the block.
func (sw *sessionWants) ForEachUniqDup(ks []cid.Cid, isUniqFn, isDupFn func()) {
	sw.RLock()

	for _, k := range ks {
		if sw.unlockedIsWanted(k) {
			isUniqFn()
		} else if sw.pastWants.Has(k) {
			isDupFn()
		}
	}

	sw.RUnlock()
}

// LiveWants returns a list of live wants
func (sw *sessionWants) LiveWants() []cid.Cid {
	sw.RLock()
	defer sw.RUnlock()

	live := make([]cid.Cid, 0, len(sw.liveWants))
	for c := range sw.liveWants {
		live = append(live, c)
	}
	return live
}

// RandomLiveWant returns a randomly selected live want
func (sw *sessionWants) RandomLiveWant() cid.Cid {
	i := rand.Uint64()

	sw.RLock()
	defer sw.RUnlock()

	if len(sw.liveWants) == 0 {
		return cid.Cid{}
	}
	i %= uint64(len(sw.liveWants))
	// picking a random live want
	for k := range sw.liveWants {
		if i == 0 {
			return k
		}
		i--
	}
	return cid.Cid{}
}

// Has live wants indicates if there are any live wants
func (sw *sessionWants) HasLiveWants() bool {
	sw.RLock()
	defer sw.RUnlock()

	return len(sw.liveWants) > 0
}

// IsWanted indicates if the session is expecting to receive the block with the
// given CID
func (sw *sessionWants) IsWanted(c cid.Cid) bool {
	sw.RLock()
	defer sw.RUnlock()

	return sw.unlockedIsWanted(c)
}

// FilterInteresting filters the list so that it only contains keys for
// blocks that the session is waiting to receive or has received in the past
func (sw *sessionWants) FilterInteresting(ks []cid.Cid) []cid.Cid {
	sw.RLock()
	defer sw.RUnlock()

	var interested []cid.Cid
	for _, k := range ks {
		if sw.unlockedIsWanted(k) || sw.pastWants.Has(k) {
			interested = append(interested, k)
		}
	}

	return interested
}

func (sw *sessionWants) unlockedIsWanted(c cid.Cid) bool {
	_, ok := sw.liveWants[c]
	if !ok {
		ok = sw.toFetch.Has(c)
	}
	return ok
}
