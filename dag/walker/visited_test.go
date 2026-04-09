package walker

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeCID creates a deterministic CIDv1 raw from an integer seed.
func makeCID(i int) cid.Cid {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(i))
	h := sha256.Sum256(buf[:])
	hash, _ := mh.Encode(h[:], mh.SHA2_256)
	return cid.NewCidV1(cid.Raw, hash)
}

func makeCIDs(n int) []cid.Cid {
	cids := make([]cid.Cid, n)
	for i := range n {
		cids[i] = makeCID(i)
	}
	return cids
}

func TestBloomParams(t *testing.T) {
	t.Run("default FP rate", func(t *testing.T) {
		bpe, k := BloomParams(DefaultBloomFPRate)
		// 1 in 4.75M -> k=round(log2(4750000))=22, bpe=ceil(22/ln2)=32
		assert.Equal(t, uint(32), bpe)
		assert.Equal(t, uint(22), k)
	})

	t.Run("lower FP rate uses less memory", func(t *testing.T) {
		bpeLow, _ := BloomParams(1_000_000) // 1 in 1M
		bpeHigh, _ := BloomParams(DefaultBloomFPRate)
		assert.Less(t, bpeLow, bpeHigh)
	})

	t.Run("higher FP rate uses more memory", func(t *testing.T) {
		bpeDefault, _ := BloomParams(DefaultBloomFPRate)
		bpeHigher, _ := BloomParams(10_000_000)
		assert.Greater(t, bpeHigher, bpeDefault)
	})

	t.Run("fpRate=1 gives minimum params", func(t *testing.T) {
		bpe, k := BloomParams(1)
		assert.Equal(t, uint(1), k)
		assert.GreaterOrEqual(t, bpe, uint(1))
	})
}

func TestMapTracker(t *testing.T) {
	t.Run("visit and has", func(t *testing.T) {
		m := NewMapTracker()
		c := makeCID(0)

		assert.False(t, m.Has(c))
		assert.True(t, m.Visit(c)) // first visit
		assert.True(t, m.Has(c))
		assert.False(t, m.Visit(c)) // second visit
	})

	t.Run("distinct CIDs are independent", func(t *testing.T) {
		m := NewMapTracker()
		c1 := makeCID(0)
		c2 := makeCID(1)

		m.Visit(c1)
		assert.True(t, m.Has(c1))
		assert.False(t, m.Has(c2))
	})
}

func TestBloomTracker(t *testing.T) {
	t.Run("visit and has", func(t *testing.T) {
		bt, err := NewBloomTracker(MinBloomCapacity, DefaultBloomFPRate)
		require.NoError(t, err)

		c := makeCID(0)

		assert.False(t, bt.Has(c))
		assert.True(t, bt.Visit(c)) // first visit
		assert.True(t, bt.Has(c))
		assert.False(t, bt.Visit(c)) // second visit
	})

	t.Run("count tracks unique inserts", func(t *testing.T) {
		bt, err := NewBloomTracker(MinBloomCapacity, DefaultBloomFPRate)
		require.NoError(t, err)

		cids := makeCIDs(100)
		for _, c := range cids {
			bt.Visit(c)
		}
		assert.Equal(t, uint64(100), bt.Count())

		// revisiting doesn't increase count
		for _, c := range cids {
			bt.Visit(c)
		}
		assert.Equal(t, uint64(100), bt.Count())
	})

	t.Run("chain growth on saturation", func(t *testing.T) {
		bt, err := NewBloomTracker(MinBloomCapacity, DefaultBloomFPRate)
		require.NoError(t, err)
		assert.Len(t, bt.chain, 1)

		cids := makeCIDs(5 * MinBloomCapacity)
		for _, c := range cids {
			bt.Visit(c)
		}
		assert.Greater(t, len(bt.chain), 1, "chain should grow")

		// all inserted CIDs should still be found across the chain
		for _, c := range cids {
			assert.True(t, bt.Has(c), "CID should be found after chain growth")
		}
	})

	t.Run("count survives chain growth", func(t *testing.T) {
		// Insert just past capacity to trigger exactly one grow().
		// Minimal items in bloom2 keeps FP exposure negligible.
		const total = MinBloomCapacity + 2
		bt, err := NewBloomTracker(MinBloomCapacity, DefaultBloomFPRate)
		require.NoError(t, err)

		cids := makeCIDs(total)
		for _, c := range cids {
			bt.Visit(c)
		}
		assert.Greater(t, len(bt.chain), 1, "chain should have grown")
		assert.Equal(t, uint64(total), bt.Count())
	})

	t.Run("below MinBloomCapacity returns error", func(t *testing.T) {
		_, err := NewBloomTracker(MinBloomCapacity-1, DefaultBloomFPRate)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expectedItems must be")
	})

	t.Run("zero fpRate returns error", func(t *testing.T) {
		_, err := NewBloomTracker(MinBloomCapacity, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fpRate must be > 0")
	})

	t.Run("FP regression at measurable rate", func(t *testing.T) {
		// Use fpRate=1000 (1 in 1000) so we get enough FPs in 100K
		// probes to be statistically measurable.
		//
		// Catches regressions in BloomParams derivation, bbloom
		// behavior, or parameter coupling bugs. We allow 5x tolerance
		// because bbloom's power-of-two rounding makes the actual rate
		// better than target.
		const (
			fpTarget = 1000
			n        = 50_000
			probes   = 100_000
		)
		bt, err := NewBloomTracker(uint(n), fpTarget)
		require.NoError(t, err)

		for _, c := range makeCIDs(n) {
			bt.Visit(c)
		}

		fpCount := 0
		for i := n; i < n+probes; i++ {
			if bt.Has(makeCID(i)) {
				fpCount++
			}
		}
		observedRate := float64(fpCount) / float64(probes)
		expectedRate := 1.0 / float64(fpTarget)

		t.Logf("FP regression: %d / %d = %.4f%% (target: %.4f%%, 1 in %d)",
			fpCount, probes, observedRate*100, expectedRate*100, fpTarget)

		assert.Less(t, observedRate, expectedRate*5,
			"FP rate %.4f%% is >5x worse than target %.4f%%", observedRate*100, expectedRate*100)
	})

	t.Run("FP regression at default rate", func(t *testing.T) {
		// DefaultBloomFPRate is ~1 in 4.75M. With 100K probes the
		// expected FP count is 100K/4.75M = ~0.02, so we should see
		// exactly 0. Any non-zero result indicates a regression.
		const n = 100_000
		bt, err := NewBloomTracker(uint(n), DefaultBloomFPRate)
		require.NoError(t, err)

		for _, c := range makeCIDs(n) {
			bt.Visit(c)
		}

		fpCount := 0
		for i := n; i < 2*n; i++ {
			if bt.Has(makeCID(i)) {
				fpCount++
			}
		}
		t.Logf("default rate FPs: %d / %d (expected: 0 at ~1 in %d)",
			fpCount, n, DefaultBloomFPRate)
		assert.Equal(t, 0, fpCount,
			"at ~1 in 4.75M FP rate, 100K probes should produce 0 FPs")
	})
}

func TestBloomAndMapEquivalence(t *testing.T) {
	bt, err := NewBloomTracker(MinBloomCapacity, DefaultBloomFPRate)
	require.NoError(t, err)
	mt := NewMapTracker()

	cids := makeCIDs(500)

	for _, c := range cids {
		bv := bt.Visit(c)
		mv := mt.Visit(c)
		assert.Equal(t, mv, bv, "Visit mismatch for %s", c)
	}

	for _, c := range cids {
		bh := bt.Has(c)
		mh := mt.Has(c)
		assert.Equal(t, mh, bh, "Has mismatch for %s", c)
	}
}

func TestCidSetSatisfiesInterface(t *testing.T) {
	var tracker VisitedTracker = cid.NewSet()

	c := makeCID(42)
	assert.False(t, tracker.Has(c))
	assert.True(t, tracker.Visit(c))
	assert.True(t, tracker.Has(c))
	assert.False(t, tracker.Visit(c))
}

func TestBloomTrackerUniqueKeys(t *testing.T) {
	bt1, err := NewBloomTracker(MinBloomCapacity, DefaultBloomFPRate)
	require.NoError(t, err)
	bt2, err := NewBloomTracker(MinBloomCapacity, DefaultBloomFPRate)
	require.NoError(t, err)

	cids := makeCIDs(500)
	for _, c := range cids {
		bt1.Visit(c)
		bt2.Visit(c)
	}

	for _, c := range cids {
		assert.True(t, bt1.Has(c))
		assert.True(t, bt2.Has(c))
	}

	// Check 10K non-inserted CIDs: false positives should differ
	// between trackers since each uses independent random SipHash keys.
	// With shared keys, fpBoth == fp1 == fp2 (correlated).
	// With independent keys, P(both FP on same CID) is negligible.
	var fp1, fp2, fpBoth int
	for i := 500; i < 10500; i++ {
		c := makeCID(i)
		h1 := bt1.Has(c)
		h2 := bt2.Has(c)
		if h1 {
			fp1++
		}
		if h2 {
			fp2++
		}
		if h1 && h2 {
			fpBoth++
		}
	}
	t.Logf("independent key FPs: bt1=%d, bt2=%d, both=%d (out of 10000 probes)", fp1, fp2, fpBoth)
	if fp1 > 0 && fp2 > 0 {
		assert.Less(t, fpBoth, fp1, "FPs should be uncorrelated between instances")
	}
}
