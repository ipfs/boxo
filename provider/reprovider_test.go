package provider

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/internal/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	return slices.Clone(m.keys), m.calls
}

var _ allFeatures = (*mockProvideMany)(nil)

type allButMany interface {
	Provide
	Ready
}

type singleMockWrapper struct {
	allButMany
}

func initialReprovideDelay(duration time.Duration) Option {
	return func(system *reprovider) error {
		system.initialReprovideDelaySet = true
		system.initialReprovideDelay = duration
		return nil
	}
}

func TestReprovider(t *testing.T) {
	t.Parallel()
	t.Run("single", func(t *testing.T) {
		t.Parallel()
		testProvider(t, true)
	})
	t.Run("many", func(t *testing.T) {
		t.Parallel()
		testProvider(t, false)
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
			defer close(ch)
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
			if runtime.GOOS != "windows" && runtime.GOOS != "darwin" && (seventyFivePercent > avg || avg > hundredTwentyFivePercent) {
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
	time.Sleep(time.Millisecond * 50) // give it time to call provider after that

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

	for i := range numProvides {
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
	if runtime.GOOS == "windows" {
		test.Flaky(t)
	}
	// Don't run in Parallel as this test is time sensitive.

	someHash, err := mh.Sum([]byte("Vires in Numeris!"), mh.BLAKE3, -1)
	assert.NoError(t, err)
	c := cid.NewCidV1(cid.Raw, someHash)

	ds := dssync.MutexWrap(datastore.NewMapDatastore())

	// First public using an offline system to enqueue in the datastore.
	sys, err := New(ds)
	assert.NoError(t, err)

	err = sys.Provide(context.Background(), c, true)
	assert.NoError(t, err)

	err = sys.Close()
	assert.NoError(t, err)

	// Secondly restart an online datastore and we want to see this previously provided cid published.
	prov := &mockProvideMany{}
	sys, err = New(ds, Online(prov), initialReprovideDelay(0))
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 10) // give it time to call provider after that

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

func newMockKeyChanFunc(cids []cid.Cid) KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		outCh := make(chan cid.Cid)

		go func() {
			defer close(outCh)
			for _, c := range cids {
				select {
				case <-ctx.Done():
					return
				case outCh <- c:
				}
			}
		}()

		return outCh, nil
	}
}

func makeCIDs(n int) []cid.Cid {
	cids := make([]cid.Cid, n)
	for i := range n {
		buf := make([]byte, 63)
		_, err := rand.Read(buf)
		if err != nil {
			panic(err)
		}
		data, err := mh.Encode(buf, mh.SHA2_256)
		if err != nil {
			panic(err)
		}
		cids[i] = cid.NewCidV1(0, data)
	}

	return cids
}

func TestNewPrioritizedProvider(t *testing.T) {
	cids := makeCIDs(6)

	testCases := []struct {
		name     string
		priority []cid.Cid
		all      []cid.Cid
		expected []cid.Cid
	}{
		{
			name:     "basic test",
			priority: cids[:3],
			all:      cids[3:],
			expected: cids,
		},
		{
			name:     "basic test inverted",
			priority: cids[3:],
			all:      cids[:3],
			expected: append(cids[3:], cids[:3]...),
		},
		{
			name:     "no repeated",
			priority: cids[3:],
			all:      cids[3:],
			expected: cids[3:],
		},
		{
			name:     "no repeated if duplicates in the prioritized channel",
			priority: []cid.Cid{cids[0], cids[1], cids[0]},
			all:      []cid.Cid{cids[2], cids[4], cids[5]},
			expected: []cid.Cid{cids[0], cids[1], cids[2], cids[4], cids[5]},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			stream := NewPrioritizedProvider(newMockKeyChanFunc(tc.priority), newMockKeyChanFunc(tc.all))
			ch, err := stream(ctx)
			require.NoError(t, err)

			received := []cid.Cid{}
			for c := range ch {
				received = append(received, c)
			}
			require.Equal(t, tc.expected, received)
		})
	}
}

// TestPrioritizedProvider_StreamErrorContinues verifies that a failure
// in one stream does not prevent subsequent streams from running.
// e.g. MFS flush failure should not block pinned content from being provided.
func TestPrioritizedProvider_StreamErrorContinues(t *testing.T) {
	cids := makeCIDs(3)

	failingStream := func(_ context.Context) (<-chan cid.Cid, error) {
		return nil, fmt.Errorf("stream init failed")
	}
	goodStream := newMockKeyChanFunc(cids)

	// failing stream first, good stream second
	stream := NewPrioritizedProvider(failingStream, goodStream)
	ch, err := stream(t.Context())
	require.NoError(t, err)

	var received []cid.Cid
	for c := range ch {
		received = append(received, c)
	}
	// good stream should still produce its CIDs despite the first stream failing
	require.Equal(t, cids, received)
}

// TestPrioritizedProvider_ContextCancellation verifies that context
// cancellation stops the provider cleanly without hanging.
func TestPrioritizedProvider_ContextCancellation(t *testing.T) {
	// slow stream that blocks until context is cancelled
	slowStream := func(ctx context.Context) (<-chan cid.Cid, error) {
		ch := make(chan cid.Cid)
		go func() {
			defer close(ch)
			<-ctx.Done()
		}()
		return ch, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	stream := NewPrioritizedProvider(slowStream)
	ch, err := stream(ctx)
	require.NoError(t, err)

	cancel()
	// channel should close promptly after cancellation
	for range ch {
		t.Fatal("should not receive CIDs after cancellation")
	}
}

// TestPrioritizedProvider_ThreeStreams verifies correct ordering and
// dedup across three streams (the common case: MFS + pinned + direct).
func TestPrioritizedProvider_ThreeStreams(t *testing.T) {
	cids := makeCIDs(9)
	s1 := newMockKeyChanFunc(cids[:3])                          // highest priority
	s2 := newMockKeyChanFunc(append(cids[1:4:4], cids[4:6]...)) // overlaps with s1
	s3 := newMockKeyChanFunc(cids[6:])                          // lowest priority

	stream := NewPrioritizedProvider(s1, s2, s3)
	ch, err := stream(t.Context())
	require.NoError(t, err)

	var received []cid.Cid
	for c := range ch {
		received = append(received, c)
	}
	// s1: 0,1,2 (all new)
	// s2: 1,2 skipped (seen in s1), 3,4,5 new
	// s3: 6,7,8 (all new, last stream so no dedup tracking)
	require.Equal(t, []cid.Cid{
		cids[0], cids[1], cids[2], // s1
		cids[3], cids[4], cids[5], // s2 (deduped 1,2)
		cids[6], cids[7], cids[8], // s3
	}, received)
}

// TestPrioritizedProvider_AllStreamsFail verifies that when every
// stream fails, the output channel closes cleanly with no CIDs.
func TestPrioritizedProvider_AllStreamsFail(t *testing.T) {
	fail := func(_ context.Context) (<-chan cid.Cid, error) {
		return nil, fmt.Errorf("fail")
	}
	stream := NewPrioritizedProvider(fail, fail, fail)
	ch, err := stream(t.Context())
	require.NoError(t, err)

	var received []cid.Cid
	for c := range ch {
		received = append(received, c)
	}
	require.Empty(t, received)
}

// TestPrioritizedProvider_ErrorContinues verifies that a failing stream
// does not prevent subsequent streams from being processed. This is a
// regression test for a bug where the goroutine returned on error
// instead of continuing to the next stream.
func TestPrioritizedProvider_ErrorContinues(t *testing.T) {
	cids := makeCIDs(3)
	fail := func(_ context.Context) (<-chan cid.Cid, error) {
		return nil, fmt.Errorf("stream error")
	}
	good := newMockKeyChanFunc(cids)

	stream := NewPrioritizedProvider(fail, good)
	ch, err := stream(t.Context())
	require.NoError(t, err)

	var received []cid.Cid
	for c := range ch {
		received = append(received, c)
	}
	require.Equal(t, cids, received,
		"CIDs from the good stream must still be emitted after a prior stream fails")
}

// TestNewConcatProvider verifies that ConcatProvider concatenates
// streams in order without deduplication. Unlike PrioritizedProvider,
// duplicate CIDs across streams are NOT filtered.
func TestNewConcatProvider(t *testing.T) {
	cids := makeCIDs(6)

	t.Run("concatenates in order", func(t *testing.T) {
		s1 := newMockKeyChanFunc(cids[:3])
		s2 := newMockKeyChanFunc(cids[3:])

		stream := NewConcatProvider(s1, s2)
		ch, err := stream(t.Context())
		require.NoError(t, err)

		var received []cid.Cid
		for c := range ch {
			received = append(received, c)
		}
		require.Equal(t, cids, received)
	})

	t.Run("duplicates are NOT filtered", func(t *testing.T) {
		// same CIDs in both streams -- ConcatProvider passes them all through
		s1 := newMockKeyChanFunc(cids[:3])
		s2 := newMockKeyChanFunc(cids[:3])

		stream := NewConcatProvider(s1, s2)
		ch, err := stream(t.Context())
		require.NoError(t, err)

		var received []cid.Cid
		for c := range ch {
			received = append(received, c)
		}
		expected := append(cids[:3:3], cids[:3]...)
		require.Equal(t, expected, received)
	})

	t.Run("stream error skips to next", func(t *testing.T) {
		failing := func(_ context.Context) (<-chan cid.Cid, error) {
			return nil, fmt.Errorf("init failed")
		}
		good := newMockKeyChanFunc(cids[:3])

		stream := NewConcatProvider(failing, good)
		ch, err := stream(t.Context())
		require.NoError(t, err)

		var received []cid.Cid
		for c := range ch {
			received = append(received, c)
		}
		require.Equal(t, cids[:3], received)
	})

	t.Run("single stream", func(t *testing.T) {
		stream := NewConcatProvider(newMockKeyChanFunc(cids))
		ch, err := stream(t.Context())
		require.NoError(t, err)

		var received []cid.Cid
		for c := range ch {
			received = append(received, c)
		}
		require.Equal(t, cids, received)
	})

	t.Run("empty streams", func(t *testing.T) {
		empty := newMockKeyChanFunc(nil)
		stream := NewConcatProvider(empty, newMockKeyChanFunc(cids[:2]))
		ch, err := stream(t.Context())
		require.NoError(t, err)

		var received []cid.Cid
		for c := range ch {
			received = append(received, c)
		}
		require.Equal(t, cids[:2], received)
	})

	t.Run("context cancellation stops cleanly", func(t *testing.T) {
		slowStream := func(ctx context.Context) (<-chan cid.Cid, error) {
			ch := make(chan cid.Cid)
			go func() {
				defer close(ch)
				<-ctx.Done()
			}()
			return ch, nil
		}

		ctx, cancel := context.WithCancel(t.Context())
		stream := NewConcatProvider(slowStream)
		ch, err := stream(ctx)
		require.NoError(t, err)

		cancel()
		for range ch {
			t.Fatal("should not receive CIDs after cancellation")
		}
	})
}
