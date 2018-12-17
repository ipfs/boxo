package sessionpeermanager

import (
	"time"

	"github.com/ipfs/go-cid"
)

const (
	timeoutDuration = 5 * time.Second
)

type requestData struct {
	startedAt   time.Time
	timeoutFunc *time.Timer
}

type latencyTracker struct {
	requests map[cid.Cid]*requestData
}

func newLatencyTracker() *latencyTracker {
	return &latencyTracker{requests: make(map[cid.Cid]*requestData)}
}

type afterTimeoutFunc func(cid.Cid)

func (lt *latencyTracker) SetupRequests(keys []cid.Cid, afterTimeout afterTimeoutFunc) {
	startedAt := time.Now()
	for _, k := range keys {
		if _, ok := lt.requests[k]; !ok {
			lt.requests[k] = &requestData{startedAt, time.AfterFunc(timeoutDuration, makeAfterTimeout(afterTimeout, k))}
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
		latency = time.Now().Sub(request.startedAt)
	}
	return latency, ok
}

func (lt *latencyTracker) RecordResponse(key cid.Cid) (time.Duration, bool) {
	request, ok := lt.requests[key]
	var latency time.Duration
	if ok {
		latency = time.Now().Sub(request.startedAt)
		request.timeoutFunc.Stop()
		delete(lt.requests, key)
	}
	return latency, ok
}

func (lt *latencyTracker) Shutdown() {
	for _, request := range lt.requests {
		request.timeoutFunc.Stop()
	}
}
