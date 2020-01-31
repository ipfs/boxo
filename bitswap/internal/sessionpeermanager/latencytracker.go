package sessionpeermanager

import (
	"time"

	"github.com/ipfs/go-cid"
)

type requestData struct {
	startedAt    time.Time
	wasCancelled bool
	timeoutFunc  *time.Timer
}

type latencyTracker struct {
	requests map[cid.Cid]*requestData
}

func newLatencyTracker() *latencyTracker {
	return &latencyTracker{requests: make(map[cid.Cid]*requestData)}
}

type afterTimeoutFunc func(cid.Cid)

func (lt *latencyTracker) SetupRequests(keys []cid.Cid, timeoutDuration time.Duration, afterTimeout afterTimeoutFunc) {
	startedAt := time.Now()
	for _, k := range keys {
		if _, ok := lt.requests[k]; !ok {
			lt.requests[k] = &requestData{
				startedAt,
				false,
				time.AfterFunc(timeoutDuration, makeAfterTimeout(afterTimeout, k)),
			}
		}
	}
}

func makeAfterTimeout(afterTimeout afterTimeoutFunc, k cid.Cid) func() {
	return func() { afterTimeout(k) }
}

func (lt *latencyTracker) CheckDuration(key cid.Cid) (time.Duration, bool) {
	request, ok := lt.requests[key]
	var latency time.Duration
	if ok {
		latency = time.Since(request.startedAt)
	}
	return latency, ok
}

func (lt *latencyTracker) RemoveRequest(key cid.Cid) {
	request, ok := lt.requests[key]
	if ok {
		request.timeoutFunc.Stop()
		delete(lt.requests, key)
	}
}

func (lt *latencyTracker) RecordCancel(keys []cid.Cid) {
	for _, key := range keys {
		request, ok := lt.requests[key]
		if ok {
			request.wasCancelled = true
		}
	}
}

func (lt *latencyTracker) WasCancelled(key cid.Cid) bool {
	request, ok := lt.requests[key]
	return ok && request.wasCancelled
}

func (lt *latencyTracker) Shutdown() {
	for _, request := range lt.requests {
		request.timeoutFunc.Stop()
	}
}
