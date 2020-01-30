package session

import (
	"math"
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func TestPeerResponseTrackerInit(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	prt := newPeerResponseTracker()

	if prt.choose([]peer.ID{}) != "" {
		t.Fatal("expected empty peer ID")
	}
	if prt.choose([]peer.ID{peers[0]}) != peers[0] {
		t.Fatal("expected single peer ID")
	}
	p := prt.choose(peers)
	if p != peers[0] && p != peers[1] {
		t.Fatal("expected randomly chosen peer")
	}
}

func TestPeerResponseTrackerProbabilityUnknownPeers(t *testing.T) {
	peers := testutil.GeneratePeers(4)
	prt := newPeerResponseTracker()

	choices := []int{0, 0, 0, 0}
	count := 1000
	for i := 0; i < count; i++ {
		p := prt.choose(peers)
		if p == peers[0] {
			choices[0]++
		} else if p == peers[1] {
			choices[1]++
		} else if p == peers[2] {
			choices[2]++
		} else if p == peers[3] {
			choices[3]++
		}
	}

	for _, c := range choices {
		if c == 0 {
			t.Fatal("expected each peer to be chosen at least once")
		}
		if math.Abs(float64(c-choices[0])) > 0.2*float64(count) {
			t.Fatal("expected unknown peers to have roughly equal chance of being chosen")
		}
	}
}

func TestPeerResponseTrackerProbabilityOneKnownOneUnknownPeer(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	prt := newPeerResponseTracker()

	prt.receivedBlockFrom(peers[0])

	chooseFirst := 0
	chooseSecond := 0
	for i := 0; i < 1000; i++ {
		p := prt.choose(peers)
		if p == peers[0] {
			chooseFirst++
		} else if p == peers[1] {
			chooseSecond++
		}
	}

	if chooseSecond == 0 {
		t.Fatal("expected unknown peer to occasionally be chosen")
	}
	if chooseSecond > chooseFirst {
		t.Fatal("expected known peer to be chosen more often")
	}
}

func TestPeerResponseTrackerProbabilityProportional(t *testing.T) {
	peers := testutil.GeneratePeers(3)
	prt := newPeerResponseTracker()

	probabilities := []float64{0.1, 0.6, 0.3}
	count := 1000
	for pi, prob := range probabilities {
		for i := 0; float64(i) < float64(count)*prob; i++ {
			prt.receivedBlockFrom(peers[pi])
		}
	}

	var choices []int
	for range probabilities {
		choices = append(choices, 0)
	}

	for i := 0; i < count; i++ {
		p := prt.choose(peers)
		if p == peers[0] {
			choices[0]++
		} else if p == peers[1] {
			choices[1]++
		} else if p == peers[2] {
			choices[2]++
		}
	}

	for i, c := range choices {
		if c == 0 {
			t.Fatal("expected each peer to be chosen at least once")
		}
		if math.Abs(float64(c)-(float64(count)*probabilities[i])) > 0.2*float64(count) {
			t.Fatal("expected peers to be chosen proportionally to probability")
		}
	}
}
