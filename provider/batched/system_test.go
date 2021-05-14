package batched

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	mh "github.com/multiformats/go-multihash"

	q "github.com/ipfs/go-ipfs-provider/queue"
)

type mockProvideMany struct {
	lk   sync.Mutex
	keys []mh.Multihash
}

func (m *mockProvideMany) ProvideMany(ctx context.Context, keys []mh.Multihash) error {
	m.lk.Lock()
	defer m.lk.Unlock()
	m.keys = keys
	return nil
}

func (m *mockProvideMany) Ready() bool {
	return true
}

func (m *mockProvideMany) GetKeys() []mh.Multihash {
	m.lk.Lock()
	defer m.lk.Unlock()
	return m.keys[:]
}

var _ provideMany = (*mockProvideMany)(nil)

func TestBatched(t *testing.T) {
	ctx := context.Background()
	defer ctx.Done()

	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	queue, err := q.NewQueue(ctx, "test", ds)
	if err != nil {
		t.Fatal(err)
	}

	provider := &mockProvideMany{}

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	const numProvides = 100
	keysToProvide := make(map[cid.Cid]int)
	for i := 0; i < numProvides; i++ {
		h, err := mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		c := cid.NewCidV1(cid.Raw, h)
		keysToProvide[c] = i
	}

	batchSystem, err := New(provider, queue, KeyProvider(func(ctx context.Context) (<-chan cid.Cid, error) {
		ch := make(chan cid.Cid)
		go func() {
			for k := range keysToProvide {
				select {
				case ch <- k:
				case <-ctx.Done():
					return
				}
			}
		}()
		return ch, nil
	}), initialReprovideDelay(0))
	if err != nil {
		t.Fatal(err)
	}

	batchSystem.Run()

	var keys []mh.Multihash
	for {
		if ctx.Err() != nil {
			t.Fatal("test hung")
		}
		keys = provider.GetKeys()
		if len(keys) != 0 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

	if len(keys) != numProvides {
		t.Fatalf("expected %d provider keys, got %d", numProvides, len(keys))
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
