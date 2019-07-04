package sessionpeermanager

import (
	"time"

	"github.com/ipfs/go-cid"
)

const (
	newLatencyWeight = 0.5
)

type peerData struct {
	hasLatency bool
	latency    time.Duration
	lt         *latencyTracker
}

func newPeerData() *peerData {
	return &peerData{
		hasLatency: false,
		lt:         newLatencyTracker(),
		latency:    0,
	}
}

func (pd *peerData) AdjustLatency(k cid.Cid, hasFallbackLatency bool, fallbackLatency time.Duration) {
	latency, hasLatency := pd.lt.CheckDuration(k)
	pd.lt.RemoveRequest(k)
	if !hasLatency {
		latency, hasLatency = fallbackLatency, hasFallbackLatency
	}
	if hasLatency {
		if pd.hasLatency {
			pd.latency = time.Duration(float64(pd.latency)*(1.0-newLatencyWeight) + float64(latency)*newLatencyWeight)
		} else {
			pd.latency = latency
			pd.hasLatency = true
		}
	}
}
