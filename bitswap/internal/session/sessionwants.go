package session

import (
	"fmt"
	"math/rand"
	"time"

	cid "github.com/ipfs/go-cid"
)

// sessionWants keeps track of which cids are waiting to be sent out, and which
// peers are "live" - ie, we've sent a request but haven't received a block yet
type sessionWants struct {
	toFetch   *cidQueue
	liveWants map[cid.Cid]time.Time
}

func newSessionWants() sessionWants {
	return sessionWants{
		toFetch:   newCidQueue(),
		liveWants: make(map[cid.Cid]time.Time),
	}
}

func (sw *sessionWants) String() string {
	return fmt.Sprintf("%d pending / %d live", sw.toFetch.Len(), len(sw.liveWants))
}

// BlocksRequested is called when the client makes a request for blocks
func (sw *sessionWants) BlocksRequested(newWants []cid.Cid) {
	for _, k := range newWants {
		sw.toFetch.Push(k)
	}
}

// GetNextWants moves as many CIDs from the fetch queue to the live wants
// list as possible (given the limit). Returns the newly live wants.
func (sw *sessionWants) GetNextWants(limit int) []cid.Cid {
	now := time.Now()

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

// WantsSent is called when wants are sent to a peer
func (sw *sessionWants) WantsSent(ks []cid.Cid) {
	now := time.Now()
	for _, c := range ks {
		if _, ok := sw.liveWants[c]; !ok && sw.toFetch.Has(c) {
			sw.toFetch.Remove(c)
			sw.liveWants[c] = now
		}
	}
}

// BlocksReceived removes received block CIDs from the live wants list and
// measures latency. It returns the CIDs of blocks that were actually
// wanted (as opposed to duplicates) and the total latency for all incoming blocks.
func (sw *sessionWants) BlocksReceived(ks []cid.Cid) ([]cid.Cid, time.Duration) {
	wanted := make([]cid.Cid, 0, len(ks))
	totalLatency := time.Duration(0)
	if len(ks) == 0 {
		return wanted, totalLatency
	}

	now := time.Now()
	for _, c := range ks {
		if sw.isWanted(c) {
			wanted = append(wanted, c)

			sentAt, ok := sw.liveWants[c]
			if ok && !sentAt.IsZero() {
				totalLatency += now.Sub(sentAt)
			}

			// Remove the CID from the live wants / toFetch queue
			delete(sw.liveWants, c)
			sw.toFetch.Remove(c)
		}
	}

	return wanted, totalLatency
}

// PrepareBroadcast saves the current time for each live want and returns the
// live want CIDs.
func (sw *sessionWants) PrepareBroadcast() []cid.Cid {
	// TODO: Change this to return wants in order so that the session will
	// send out Find Providers request for the first want
	// (Note that maps return keys in random order)
	now := time.Now()
	live := make([]cid.Cid, 0, len(sw.liveWants))
	for c := range sw.liveWants {
		live = append(live, c)
		sw.liveWants[c] = now
	}
	return live
}

// CancelPending removes the given CIDs from the fetch queue.
func (sw *sessionWants) CancelPending(keys []cid.Cid) {
	for _, k := range keys {
		sw.toFetch.Remove(k)
	}
}

// LiveWants returns a list of live wants
func (sw *sessionWants) LiveWants() []cid.Cid {
	live := make([]cid.Cid, 0, len(sw.liveWants))
	for c := range sw.liveWants {
		live = append(live, c)
	}
	return live
}

func (sw *sessionWants) RandomLiveWant() cid.Cid {
	if len(sw.liveWants) == 0 {
		return cid.Cid{}
	}

	// picking a random live want
	i := rand.Intn(len(sw.liveWants))
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
	return len(sw.liveWants) > 0
}

// Indicates whether the want is in either of the fetch or live queues
func (sw *sessionWants) isWanted(c cid.Cid) bool {
	_, ok := sw.liveWants[c]
	if !ok {
		ok = sw.toFetch.Has(c)
	}
	return ok
}
