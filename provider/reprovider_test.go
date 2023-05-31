package provider

import (
	"bytes"
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
)

type allFeatures interface {
	Provide
	ProvideMany
	Ready
}

type mockProvideMany struct {
	delay time.Duration
	lk    sync.Mutex
	keys  []mh.Multihash
	calls uint
}

func (m *mockProvideMany) ProvideMany(ctx context.Context, keys []mh.Multihash) error {
	m.lk.Lock()
	defer m.lk.Unlock()
	m.keys = append(m.keys, keys...)
	m.calls++
	time.Sleep(time.Duration(len(keys)) * m.delay)
	return nil
}

func (m *mockProvideMany) Provide(ctx context.Context, key cid.Cid, _ bool) error {
	m.lk.Lock()
	defer m.lk.Unlock()
	m.keys = append(m.keys, key.Hash())
	m.calls++
	time.Sleep(m.delay)
	return nil
}

func (m *mockProvideMany) Ready() bool {
	return true
}

func (m *mockProvideMany) GetKeys() (keys []mh.Multihash, calls uint) {
	m.lk.Lock()
	defer m.lk.Unlock()
	return append([]mh.Multihash(nil), m.keys...), m.calls
}

var _ allFeatures = (*mockProvideMany)(nil)

type allButMany interface {
	Provide
	Ready
}

type singleMockWrapper struct {
	allButMany
}

func TestReprovider(t *testing.T) {
	t.Parallel()
	t.Run("many", func(t *testing.T) {
		t.Parallel()
		testProvider(t, false)
	})
	t.Run("single", func(t *testing.T) {
		t.Parallel()
		testProvider(t, true)
	})
}

func testProvider(t *testing.T, singleProvide bool) {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())

	// It has to be so big because the combo of noisy CI runners + OSes that don't
	// have scheduler as good as linux's one add a whole lot of jitter.
	const provideDelay = time.Millisecond * 5
	orig := &mockProvideMany{
		delay: provideDelay,
	}
	var provider Provide = orig
	if singleProvide {
		provider = singleMockWrapper{orig}
	}

	const numProvides = 100
	keysToProvide := make([]cid.Cid, numProvides)
	for i := range keysToProvide {
		h, err := mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		c := cid.NewCidV1(cid.Raw, h)
		keysToProvide[i] = c
	}

	var keyWait sync.Mutex
	keyWait.Lock()
	batchSystem, err := New(ds, Online(provider), KeyProvider(func(ctx context.Context) (<-chan cid.Cid, error) {
		ch := make(chan cid.Cid)
		go func() {
			defer keyWait.Unlock()
			for _, k := range keysToProvide {
				select {
				case ch <- k:
				case <-ctx.Done():
					return
				}
			}
		}()
		return ch, nil
	}),
		initialReprovideDelay(0),
		ThroughputReport(func(_, complete bool, n uint, d time.Duration) bool {
			if !singleProvide && complete {
				t.Errorf("expected an incomplete report but got a complete one")
			}

			const twentyFivePercent = provideDelay / 4
			const seventyFivePercent = provideDelay - twentyFivePercent
			const hundredTwentyFivePercent = provideDelay + twentyFivePercent

			avg := d / time.Duration(n)

			// windows's and darwin's schedulers and timers are too unreliable for this check
			if runtime.GOOS != "windows" && runtime.GOOS != "darwin" && !(seventyFivePercent <= avg && avg <= hundredTwentyFivePercent) {
				t.Errorf("average computed duration is not within bounds, expected between %v and %v but got %v.", seventyFivePercent, hundredTwentyFivePercent, avg)
			}
			return false
		}, numProvides/2),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer batchSystem.Close()

	keyWait.Lock()
	time.Sleep(pauseDetectionThreshold + time.Millisecond*50) // give it time to call provider after that

	keys, calls := orig.GetKeys()
	if len(keys) != numProvides {
		t.Fatalf("expected %d provider keys, got %d", numProvides, len(keys))
	}
	if singleProvide {
		if calls != 100 {
			t.Fatalf("expected 100 call single provide call, got %d", calls)
		}
	} else {
		// Two because of ThroughputReport's limit being half.
		if calls != 2 {
			t.Fatalf("expected 2 call batched provide call, got %d", calls)
		}
	}

	provMap := make(map[string]struct{})
	for _, k := range keys {
		provMap[string(k)] = struct{}{}
	}

	for i := 0; i < numProvides; i++ {
		h, err := mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		if _, found := provMap[string(h)]; !found {
			t.Fatalf("could not find provider with value %d", i)
		}
	}
}

func TestOfflineRecordsThenOnlineRepublish(t *testing.T) {
	// Don't run in Parallel as this test is time sensitive.

	someHash, err := mh.Sum([]byte("Vires in Numeris!"), mh.BLAKE3, -1)
	assert.NoError(t, err)
	c := cid.NewCidV1(cid.Raw, someHash)

	ds := dssync.MutexWrap(datastore.NewMapDatastore())

	// First public using an offline system to enqueue in the datastore.
	sys, err := New(ds)
	assert.NoError(t, err)

	err = sys.Provide(c)
	assert.NoError(t, err)

	err = sys.Close()
	assert.NoError(t, err)

	// Secondly restart an online datastore and we want to see this previously provided cid published.
	prov := &mockProvideMany{}
	sys, err = New(ds, Online(prov), initialReprovideDelay(0))
	assert.NoError(t, err)

	time.Sleep(pauseDetectionThreshold + time.Millisecond*10) // give it time to call provider after that

	err = sys.Close()
	assert.NoError(t, err)

	prov.lk.Lock()
	defer prov.lk.Unlock()
	if len(prov.keys) != 1 {
		t.Fatalf("expected to see 1 provide; got %d", len(prov.keys))
	}
	if !bytes.Equal(prov.keys[0], someHash) {
		t.Fatalf("keys are not equal expected %v, got %v", someHash, prov.keys[0])
	}
}
