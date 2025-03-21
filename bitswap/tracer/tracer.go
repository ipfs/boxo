package tracer

import "github.com/ipfs/boxo/exchange/blockexchange/tracer"

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/tracer
// Tracer provides methods to access all messages sent and received by Bitswap.
// This interface can be used to implement various statistics (this is original intent).
type Tracer tracer.Tracer
