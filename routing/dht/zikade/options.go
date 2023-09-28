package zikade

import (
	"context"
	"time"

	"github.com/ipfs/go-datastore"
	dht "github.com/libp2p/go-libp2p-kad-dht/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Option is a configuration option for the DHT
type Option func(*config) error

// Datastore configures the DHT to use the specified datastore.
//
// Defaults to an in-memory leveldb datastore.
func Datastore(dstore datastore.Batching) Option {
	return func(c *config) error {
		ds, ok := dstore.(dht.Datastore)
		if !ok {
			ds = noopTxnDatastore{dstore}
		}

		c.dht.Datastore = ds
		return nil
	}
}

// ModeClient configures the DHT to always operate in client mode.
func ModeClient(c *config) error {
	c.dht.Mode = dht.ModeOptClient
	return nil
}

// ModeServer configures the DHT to always operate in server mode.
func ModeServer(c *config) error {
	c.dht.Mode = dht.ModeOptServer
	return nil
}

// ModeAutoClient configures the DHT to start operating in client mode
// and if publicly reachability is detected to switch to server mode.
func ModeAutoClient(c *config) error {
	c.dht.Mode = dht.ModeOptAutoClient
	return nil
}

// ModeAutoServer configures the DHT to start operating in server mode,
// and if publicly reachability is not detected to switch to client mode.
func ModeAutoServer(c *config) error {
	c.dht.Mode = dht.ModeOptAutoServer
	return nil
}

// BucketSize configures the bucket size (k in the Kademlia paper) of the routing table.
//
// The default value is 20.
func BucketSize(bucketSize int) Option {
	return func(c *config) error {
		c.dht.BucketSize = bucketSize
		return nil
	}
}

// BootstrapPeers configures the bootstrapping nodes that the DHT will connect to to seed
// and the Routing Table.
func BootstrapPeers(bootstrappers ...peer.AddrInfo) Option {
	return func(c *config) error {
		c.dht.BootstrapPeers = bootstrappers
		return nil
	}
}

// QueryConcurrency defines the maximum number of in-flight queries that may be waiting for message responses at any one time.
func QueryConcurrency(v int) Option {
	return func(c *config) error {
		c.dht.Query.Concurrency = v
		return nil
	}
}

// QueryRequestConcurrency defines the maximum number of concurrent requests that each query may have in flight.
func QueryRequestConcurrency(v int) Option {
	return func(c *config) error {
		c.dht.Query.RequestConcurrency = v
		return nil
	}
}

// QueryRequestTimeout defines the time a query will wait before terminating a request to a node that has not responded.
func QueryRequestTimeout(v time.Duration) Option {
	return func(c *config) error {
		c.dht.Query.RequestTimeout = v
		return nil
	}
}

// // Validator configures the DHT to use the specified validator.
// func Validator(v record.Validator) Option {
// 	return func(c *config) error {
// 		c.dht.Validator = v
// 		return nil
// 	}
// }

var (
	_ dht.Datastore             = noopTxnDatastore{}
	_ datastore.Datastore       = noopTxnDatastore{}
	_ datastore.BatchingFeature = noopTxnDatastore{}
	_ datastore.TxnFeature      = noopTxnDatastore{}
	_ datastore.Txn             = noopTxnDatastore{}
)

// noopTxnDatastore adapts a [datastore.Batching] without transaction support to implement the [datastore.TxnFeature] interface.
// All read and write methods are delegated to the wrapped datastore and commit/discard are implemented as no-ops.
// Don't use this to wrap a datastore that already implements [datastore.TxnFeature].
type noopTxnDatastore struct {
	datastore.Batching
}

func (n noopTxnDatastore) NewTransaction(context.Context, bool) (datastore.Txn, error) {
	return n, nil
}

func (noopTxnDatastore) Commit(context.Context) error {
	return nil
}

func (noopTxnDatastore) Discard(context.Context) {}
