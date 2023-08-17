package defaults

import (
	"encoding/binary"
	"time"
)

// these requests take at _least_ two minutes at the moment.
const ProvideTimeout = time.Minute * 3
const ProvSearchDelay = time.Second

// Number of concurrent workers in decision engine that process requests to the blockstore
const BitswapEngineBlockstoreWorkerCount = 128

// the total number of simultaneous threads sending outgoing messages
const BitswapTaskWorkerCount = 8

// how many worker threads to start for decision engine task worker
const BitswapEngineTaskWorkerCount = 8

// the total amount of bytes that a peer should have outstanding, it is utilized by the decision engine
const BitswapMaxOutstandingBytesPerPeer = 1 << 20

// the number of bytes we attempt to make each outgoing bitswap message
const BitswapEngineTargetMessageSize = 16 * 1024

// HasBlockBufferSize is the buffer size of the channel for new blocks
// that need to be provided. They should get pulled over by the
// provideCollector even before they are actually provided.
// TODO: Does this need to be this large givent that?
const HasBlockBufferSize = 256

// Maximum size of the wantlist we are willing to keep in memory.
const MaxQueuedWantlistEntiresPerPeer = 1024

// Copied from github.com/ipfs/go-verifcid#maximumHashLength
// FIXME: expose this in go-verifcid.
const MaximumHashLength = 128
const MaximumAllowedCid = binary.MaxVarintLen64*4 + MaximumHashLength
