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
	// The wants that have not yet been sent out
	toFetch *cidQueue
	// Wants that have been sent but have not received a response
	liveWants *cidQueue
	// The time at which live wants were sent
	sentAt map[cid.Cid]time.Time
	// The maximum number of want-haves to send in a broadcast
	broadcastLimit int
}

func newSessionWants(broadcastLimit int) sessionWants {
	return sessionWants{
		toFetch:        newCidQueue(),
		liveWants:      newCidQueue(),
		sentAt:         make(map[cid.Cid]time.Time),
		broadcastLimit: broadcastLimit,
	}
}

func (sw *sessionWants) String() string {
	return fmt.Sprintf("%d pending / %d live", sw.toFetch.Len(), sw.liveWants.Len())
}

// BlocksRequested is called when the client makes a request for blocks
func (sw *sessionWants) BlocksRequested(newWants []cid.Cid) {
	for _, k := range newWants {
		sw.toFetch.Push(k)
	}
}

// GetNextWants is called when the session has not yet discovered peers with
// the blocks that it wants. It moves as many CIDs from the fetch queue to
// the live wants queue as possible (given the broadcast limit).
// Returns the newly live wants.
func (sw *sessionWants) GetNextWants() []cid.Cid {
	now := time.Now()

	// Move CIDs from fetch queue to the live wants queue (up to the limit)
	currentLiveCount := sw.liveWants.Len()
	toAdd := sw.broadcastLimit - currentLiveCount

	var live []cid.Cid
	for ; toAdd > 0 && sw.toFetch.Len() > 0; toAdd-- {
		c := sw.toFetch.Pop()
		live = append(live, c)
		sw.liveWants.Push(c)
		sw.sentAt[c] = now
	}

	return live
}

// WantsSent is called when wants are sent to a peer
func (sw *sessionWants) WantsSent(ks []cid.Cid) {
	now := time.Now()
	for _, c := range ks {
		if _, ok := sw.sentAt[c]; !ok && sw.toFetch.Has(c) {
			sw.toFetch.Remove(c)
			sw.liveWants.Push(c)
			sw.sentAt[c] = now
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

			// Measure latency
			sentAt, ok := sw.sentAt[c]
			if ok && !sentAt.IsZero() {
				totalLatency += now.Sub(sentAt)
			}

			// Remove the CID from the live wants / toFetch queue
			sw.liveWants.Remove(c)
			delete(sw.sentAt, c)
			sw.toFetch.Remove(c)
		}
	}

	return wanted, totalLatency
}

// PrepareBroadcast saves the current time for each live want and returns the
// live want CIDs up to the broadcast limit.
func (sw *sessionWants) PrepareBroadcast() []cid.Cid {
	now := time.Now()
	live := sw.liveWants.Cids()
	if len(live) > sw.broadcastLimit {
		live = live[:sw.broadcastLimit]
	}
	for _, c := range live {
		sw.sentAt[c] = now
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
	return sw.liveWants.Cids()
}

// RandomLiveWant returns a randomly selected live want
func (sw *sessionWants) RandomLiveWant() cid.Cid {
	if len(sw.sentAt) == 0 {
		return cid.Cid{}
	}

	// picking a random live want
	i := rand.Intn(len(sw.sentAt))
	for k := range sw.sentAt {
		if i == 0 {
			return k
		}
		i--
	}
	return cid.Cid{}
}

// Has live wants indicates if there are any live wants
func (sw *sessionWants) HasLiveWants() bool {
	return sw.liveWants.Len() > 0
}

// Indicates whether the want is in either of the fetch or live queues
func (sw *sessionWants) isWanted(c cid.Cid) bool {
	ok := sw.liveWants.Has(c)
	if !ok {
		ok = sw.toFetch.Has(c)
	}
	return ok
}
