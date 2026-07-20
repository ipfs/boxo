// Package offline implements Routing with a client which
// is only able to perform offline operations.
package offline

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

// ErrOffline is returned when trying to perform operations that
// require connectivity.
var ErrOffline = errors.New("routing system in offline mode")

// NewOfflineRouter returns a Routing implementation which only performs
// offline operations. It allows to Put and Get signed dht
// records to and from the local datastore.
//
// Records are stored in the same datastore layout as the
// go-libp2p-kad-dht value store (go-libp2p-kad-dht v0.42.0 and later),
// so a node that shares dstore between this router and a DHT instance
// can resolve records offline that were published online and vice
// versa. Records stored by earlier versions of this package (root-level
// base32 keys, the pre-v0.42.0 kad-dht layout) are not read anymore.
func NewOfflineRouter(dstore ds.Datastore, validator record.Validator) routing.Routing {
	return &offlineRouting{
		vs:        records.NewValueStore(dstore, validator, amino.DefaultMaxRecordAge),
		validator: validator,
	}
}

// offlineRouting implements the Routing interface,
// but only provides the capability to Put and Get signed dht
// records to and from the local datastore.
type offlineRouting struct {
	vs        *records.ValueStore
	validator record.Validator
}

func (c *offlineRouting) PutValue(ctx context.Context, key string, val []byte, _ ...routing.Option) error {
	rec := record.MakePutRecord(key, val)
	err := c.vs.Put(ctx, key, rec)
	if errors.Is(err, records.ErrOldRecord) {
		// be idempotent to be nice.
		if stored, gerr := c.vs.Get(ctx, key); gerr == nil && stored != nil && bytes.Equal(stored.GetValue(), val) {
			return nil
		}
	}
	return err
}

func (c *offlineRouting) GetValue(ctx context.Context, key string, _ ...routing.Option) ([]byte, error) {
	rec, err := c.vs.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, routing.ErrNotFound
	}

	val := rec.GetValue()
	// The value store only age-checks records on read; run the validator
	// so callers never see records that are expired by their own rules
	// (e.g. IPNS EOL).
	if err := c.validator.Validate(key, val); err != nil {
		return nil, err
	}
	return val, nil
}

func (c *offlineRouting) SearchValue(ctx context.Context, key string, _ ...routing.Option) (<-chan []byte, error) {
	out := make(chan []byte, 1)
	go func() {
		defer close(out)
		v, err := c.GetValue(ctx, key)
		if err == nil {
			out <- v
		}
	}()
	return out, nil
}

func (c *offlineRouting) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	return peer.AddrInfo{}, ErrOffline
}

func (c *offlineRouting) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	close(out)
	return out
}

func (c *offlineRouting) Provide(_ context.Context, k cid.Cid, _ bool) error {
	return ErrOffline
}

func (c *offlineRouting) Ping(ctx context.Context, p peer.ID) (time.Duration, error) {
	return 0, ErrOffline
}

func (c *offlineRouting) Bootstrap(context.Context) error {
	return nil
}

// ensure offlineRouting matches the Routing interface
var _ routing.Routing = &offlineRouting{}
