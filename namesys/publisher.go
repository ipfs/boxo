package namesys

import (
	"context"
	"strings"
	"sync"
	"time"

	opts "github.com/ipfs/boxo/coreiface/options/namesys"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	ds "github.com/ipfs/go-datastore"
	dsquery "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/whyrusleeping/base32"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const ipnsPrefix = "/ipns/"

// IpnsPublisher is capable of publishing and resolving names to the IPFS
// routing system.
type IpnsPublisher struct {
	routing routing.ValueStore
	ds      ds.Datastore

	// Used to ensure we assign IPNS records *sequential* sequence numbers.
	mu sync.Mutex
}

// NewIpnsPublisher constructs a publisher for the IPFS Routing name system.
func NewIpnsPublisher(route routing.ValueStore, ds ds.Datastore) *IpnsPublisher {
	if ds == nil {
		panic("nil datastore")
	}
	return &IpnsPublisher{routing: route, ds: ds}
}

// Publish implements Publisher. Accepts a keypair and a value,
// and publishes it out to the routing system
func (p *IpnsPublisher) Publish(ctx context.Context, k crypto.PrivKey, value path.Path, options ...opts.PublishOption) error {
	log.Debugf("Publish %s", value)

	ctx, span := StartSpan(ctx, "IpnsPublisher.Publish", trace.WithAttributes(attribute.String("Value", value.String())))
	defer span.End()

	record, err := p.updateRecord(ctx, k, value, options...)
	if err != nil {
		return err
	}

	return PutRecordToRouting(ctx, p.routing, k.GetPublic(), record)
}

// IpnsDsKey returns a datastore key given an IPNS identifier (peer
// ID). Defines the storage key for IPNS records in the local datastore.
func IpnsDsKey(id peer.ID) ds.Key {
	return ds.NewKey("/ipns/" + base32.RawStdEncoding.EncodeToString([]byte(id)))
}

// ListPublished returns the latest IPNS records published by this node and
// their expiration times.
//
// This method will not search the routing system for records published by other
// nodes.
func (p *IpnsPublisher) ListPublished(ctx context.Context) (map[peer.ID]*ipns.Record, error) {
	query, err := p.ds.Query(ctx, dsquery.Query{
		Prefix: ipnsPrefix,
	})
	if err != nil {
		return nil, err
	}
	defer query.Close()

	records := make(map[peer.ID]*ipns.Record)
	for {
		select {
		case result, ok := <-query.Next():
			if !ok {
				return records, nil
			}
			if result.Error != nil {
				return nil, result.Error
			}
			rec, err := ipns.UnmarshalRecord(result.Value)
			if err != nil {
				// Might as well return what we can.
				log.Error("found an invalid IPNS entry:", err)
				continue
			}
			if !strings.HasPrefix(result.Key, ipnsPrefix) {
				log.Errorf("datastore query for keys with prefix %s returned a key: %s", ipnsPrefix, result.Key)
				continue
			}
			k := result.Key[len(ipnsPrefix):]
			pid, err := base32.RawStdEncoding.DecodeString(k)
			if err != nil {
				log.Errorf("ipns ds key invalid: %s", result.Key)
				continue
			}
			records[peer.ID(pid)] = rec
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// GetPublished returns the record this node has published corresponding to the
// given peer ID.
//
// If `checkRouting` is true and we have no existing record, this method will
// check the routing system for any existing records.
func (p *IpnsPublisher) GetPublished(ctx context.Context, id peer.ID, checkRouting bool) (*ipns.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	value, err := p.ds.Get(ctx, IpnsDsKey(id))
	switch err {
	case nil:
	case ds.ErrNotFound:
		if !checkRouting {
			return nil, nil
		}
		ipnskey := string(ipns.NameFromPeer(id).RoutingKey())
		value, err = p.routing.GetValue(ctx, ipnskey)
		if err != nil {
			// Not found or other network issue. Can't really do
			// anything about this case.
			if err != routing.ErrNotFound {
				log.Debugf("error when determining the last published IPNS record for %s: %s", id, err)
			}

			return nil, nil
		}
	default:
		return nil, err
	}

	return ipns.UnmarshalRecord(value)
}

func (p *IpnsPublisher) updateRecord(ctx context.Context, k crypto.PrivKey, value path.Path, options ...opts.PublishOption) (*ipns.Record, error) {
	id, err := peer.IDFromPrivateKey(k)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// get previous records sequence number
	rec, err := p.GetPublished(ctx, id, true)
	if err != nil {
		return nil, err
	}

	seqno := uint64(0)
	if rec != nil {
		seqno, err = rec.Sequence()
		if err != nil {
			return nil, err
		}

		p, err := rec.Value()
		if err != nil {
			return nil, err
		}
		if value != path.Path(p.String()) {
			// Don't bother incrementing the sequence number unless the
			// value changes.
			seqno++
		}
	}

	opts := opts.ProcessPublishOptions(options)

	// Create record
	r, err := ipns.NewRecord(k, value, seqno, opts.EOL, opts.TTL, ipns.WithV1Compatibility(opts.CompatibleWithV1))
	if err != nil {
		return nil, err
	}

	data, err := ipns.MarshalRecord(r)
	if err != nil {
		return nil, err
	}

	// Put the new record.
	key := IpnsDsKey(id)
	if err := p.ds.Put(ctx, key, data); err != nil {
		return nil, err
	}
	if err := p.ds.Sync(ctx, key); err != nil {
		return nil, err
	}
	return r, nil
}

// PutRecordToRouting publishes the given entry using the provided ValueStore,
// keyed on the ID associated with the provided public key. The public key is
// also made available to the routing system so that entries can be verified.
func PutRecordToRouting(ctx context.Context, r routing.ValueStore, k crypto.PubKey, rec *ipns.Record) error {
	ctx, span := StartSpan(ctx, "PutRecordToRouting")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, 2) // At most two errors (IPNS, and public key)

	id, err := peer.IDFromPublicKey(k)
	if err != nil {
		return err
	}

	go func() {
		errs <- PublishEntry(ctx, r, string(ipns.NameFromPeer(id).RoutingKey()), rec)
	}()

	// Publish the public key if a public key cannot be extracted from the ID
	// TODO: once v0.4.16 is widespread enough, we can stop doing this
	// and at that point we can even deprecate the /pk/ namespace in the dht
	//
	// NOTE: This check actually checks if the public key has been embedded
	// in the IPNS entry. This check is sufficient because we embed the
	// public key in the IPNS entry if it can't be extracted from the ID.
	if _, err := rec.PubKey(); err == nil {
		go func() {
			errs <- PublishPublicKey(ctx, r, PkKeyForID(id), k)
		}()

		if err := waitOnErrChan(ctx, errs); err != nil {
			return err
		}
	}

	return waitOnErrChan(ctx, errs)
}

func waitOnErrChan(ctx context.Context, errs chan error) error {
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PublishPublicKey stores the given public key in the ValueStore with the
// given key.
func PublishPublicKey(ctx context.Context, r routing.ValueStore, k string, pubk crypto.PubKey) error {
	ctx, span := StartSpan(ctx, "PublishPublicKey", trace.WithAttributes(attribute.String("Key", k)))
	defer span.End()

	log.Debugf("Storing pubkey at: %s", k)
	pkbytes, err := crypto.MarshalPublicKey(pubk)
	if err != nil {
		return err
	}

	// Store associated public key
	return r.PutValue(ctx, k, pkbytes)
}

// PublishEntry stores the given IpnsEntry in the ValueStore with the given
// ipnskey.
func PublishEntry(ctx context.Context, r routing.ValueStore, ipnskey string, rec *ipns.Record) error {
	ctx, span := StartSpan(ctx, "PublishEntry", trace.WithAttributes(attribute.String("IPNSKey", ipnskey)))
	defer span.End()

	data, err := ipns.MarshalRecord(rec)
	if err != nil {
		return err
	}

	log.Debugf("Storing ipns entry at: %x", ipnskey)
	// Store ipns entry at "/ipns/"+h(pubkey)
	return r.PutValue(ctx, ipnskey, data)
}

// PkKeyForID returns the public key routing key for the given peer ID.
func PkKeyForID(id peer.ID) string {
	return "/pk/" + string(id)
}
