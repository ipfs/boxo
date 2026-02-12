package contentrouter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/internal"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

var logger = logging.Logger("routing/http/contentrouter")

// filterAddrs extracts multiaddrs from types.Multiaddr slice, filtering out nil
// entries as a defensive measure against corrupted data.
// See: https://github.com/ipfs/kubo/issues/11116
func filterAddrs(in []types.Multiaddr) []multiaddr.Multiaddr {
	if len(in) == 0 {
		return nil
	}
	out := make([]multiaddr.Multiaddr, 0, len(in))
	for _, a := range in {
		if a.Multiaddr != nil {
			out = append(out, a.Multiaddr)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

const ttl = 24 * time.Hour

type Client interface {
	FindProviders(ctx context.Context, key cid.Cid) (iter.ResultIter[types.Record], error)
	ProvideBitswap(ctx context.Context, keys []cid.Cid, ttl time.Duration) (time.Duration, error)
	FindPeers(ctx context.Context, pid peer.ID) (peers iter.ResultIter[*types.PeerRecord], err error)
	GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error)
	PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error
}

type contentRouter struct {
	client                Client
	maxProvideConcurrency int
	maxProvideBatchSize   int
}

var (
	_ routing.ContentRouting           = (*contentRouter)(nil)
	_ routing.PeerRouting              = (*contentRouter)(nil)
	_ routing.ValueStore               = (*contentRouter)(nil)
	_ routinghelpers.ProvideManyRouter = (*contentRouter)(nil)
	_ routinghelpers.ReadyAbleRouter   = (*contentRouter)(nil)
)

type option func(c *contentRouter)

func WithMaxProvideConcurrency(max int) option {
	return func(c *contentRouter) {
		c.maxProvideConcurrency = max
	}
}

func WithMaxProvideBatchSize(max int) option {
	return func(c *contentRouter) {
		c.maxProvideBatchSize = max
	}
}

func NewContentRoutingClient(c Client, opts ...option) *contentRouter {
	cr := &contentRouter{
		client:                c,
		maxProvideConcurrency: 5,
		maxProvideBatchSize:   100,
	}
	for _, opt := range opts {
		opt(cr)
	}
	return cr
}

func (c *contentRouter) Provide(ctx context.Context, key cid.Cid, announce bool) error {
	// If 'true' is passed, it also announces it, otherwise it is just kept in the local
	// accounting of which objects are being provided.
	if !announce {
		return nil
	}

	_, err := c.client.ProvideBitswap(ctx, []cid.Cid{key}, ttl)
	return err
}

// ProvideMany provides a set of keys to the remote delegate.
// Large sets of keys are chunked into multiple requests and sent concurrently, according to the concurrency configuration.
// TODO: switch to use [client.Provide] when ready.
func (c *contentRouter) ProvideMany(ctx context.Context, mhKeys []multihash.Multihash) error {
	keys := make([]cid.Cid, 0, len(mhKeys))
	for _, m := range mhKeys {
		keys = append(keys, cid.NewCidV1(cid.Raw, m))
	}

	if len(keys) <= c.maxProvideBatchSize {
		_, err := c.client.ProvideBitswap(ctx, keys, ttl)
		return err
	}

	return internal.DoBatch(
		ctx,
		c.maxProvideBatchSize,
		c.maxProvideConcurrency,
		keys,
		func(ctx context.Context, batch []cid.Cid) error {
			_, err := c.client.ProvideBitswap(ctx, batch, ttl)
			return err
		},
	)
}

// Ready is part of the existing [routing.ReadyAbleRouter] interface.
func (c *contentRouter) Ready() bool {
	return true
}

// peerIDFromDIDKey attempts to derive a libp2p PeerID from a did:key: identifier.
// Currently supports ed25519 public keys (multicodec 0xed).
func peerIDFromDIDKey(id string) (peer.ID, error) {
	if !strings.HasPrefix(id, "did:key:") {
		return "", fmt.Errorf("not a did:key identifier")
	}
	encoded := strings.TrimPrefix(id, "did:key:")

	// Decode the multibase-encoded part (z prefix = base58btc)
	_, data, err := multibase.Decode(encoded)
	if err != nil {
		return "", fmt.Errorf("multibase decode: %w", err)
	}

	// Read the multicodec varint
	codec, n, err := varint.FromUvarint(data)
	if err != nil {
		return "", fmt.Errorf("varint decode: %w", err)
	}

	// Only handle ed25519-pub (0xed) for now
	if multicodec.Code(codec) != multicodec.Ed25519Pub {
		return "", fmt.Errorf("unsupported key type: 0x%x", codec)
	}

	keyBytes := data[n:]
	pubKey, err := crypto.UnmarshalEd25519PublicKey(keyBytes)
	if err != nil {
		return "", fmt.Errorf("unmarshal ed25519: %w", err)
	}

	return peer.IDFromPublicKey(pubKey)
}

// peerIDPlaceholderNonce is a random value generated once per process,
// mixed into placeholder PeerID hashes to ensure different processes
// produce different placeholders for the same provider ID.
// TODO: make this configurable or find a better way to ensure uniqueness
// across processes while keeping determinism within one process.
var peerIDPlaceholderNonce = func() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}()

// peerIDPlaceholderFromArbitraryID generates a placeholder PeerID by hashing
// the given identifier with a per-process nonce and domain-specific salt.
//
// The resulting PeerID is deterministic within a single process (same input
// always produces the same PeerID) but differs across processes. This is a
// compatibility stub for providers that use identifiers which are not
// parseable as PeerIDs or did:key: values. The resulting PeerID is only
// used to pass addresses through legacy libp2p routing APIs (like Kubo and
// Rainbow) that require a PeerID. It does not represent a real libp2p
// identity: no valid private key exists for this PeerID because it is a
// SHA-256 multihash of arbitrary data, not a key-derived identity.
func peerIDPlaceholderFromArbitraryID(id string) peer.ID {
	mh, _ := multihash.Sum([]byte("contentrouter/peerIDPlaceholder:"+peerIDPlaceholderNonce+":"+id), multihash.SHA2_256, -1)
	return peer.ID(mh)
}

// hasHTTPURL returns true if any address is an http:// or https:// URL.
func hasHTTPURL(addrs types.Addresses) bool {
	for i := range addrs {
		if u := addrs[i].URL(); u != nil {
			s := strings.ToLower(u.Scheme)
			if s == "http" || s == "https" {
				return true
			}
		}
	}
	return false
}

// readProviderResponses reads provider records from the iterator into the given
// channel. PeerRecord and BitswapRecord are converted directly. GenericRecord
// is converted on a best-effort basis:
//   - If the ID is a valid libp2p PeerID, the record is always converted
//     regardless of Protocols. This supports the legacy pattern where a
//     PeerID + /https multiaddr was used as a hint to probe for a Trustless
//     IPFS HTTP Gateway (even without explicit protocol declaration).
//   - If the ID is not a PeerID but the record advertises
//     transport-ipfs-gateway-http with HTTP(S) URLs, a PeerID is derived
//     from did:key: or generated as a placeholder.
//   - Other records with non-PeerID identifiers are skipped.
//
// Addresses are converted via [types.Address.ToMultiaddr]; HTTPS URLs
// become /dns/host/tcp/443/https multiaddrs. Non-convertible addresses
// are dropped.
func readProviderResponses(ctx context.Context, iter iter.ResultIter[types.Record], ch chan<- peer.AddrInfo) {
	defer close(ch)
	defer iter.Close()
	for iter.Next() {
		res := iter.Val()
		if res.Err != nil {
			logger.Warnf("error iterating provider responses: %s", res.Err)
			continue
		}
		v := res.Val
		switch v.GetSchema() {
		case types.SchemaPeer:
			result, ok := v.(*types.PeerRecord)
			if !ok {
				logger.Errorw(
					"problem casting find providers result",
					"Schema", v.GetSchema(),
					"Type", reflect.TypeOf(v).String(),
				)
				continue
			}

			select {
			case <-ctx.Done():
				return
			case ch <- peer.AddrInfo{
				ID:    *result.ID,
				Addrs: filterAddrs(result.Addrs),
			}:
			}

		case types.SchemaGeneric:
			result, ok := v.(*types.GenericRecord)
			if !ok {
				logger.Errorw(
					"problem casting find providers result",
					"Schema", v.GetSchema(),
					"Type", reflect.TypeOf(v).String(),
				)
				continue
			}

			pid, err := peer.Decode(result.ID)
			if err != nil {
				// For HTTP gateway providers, try harder to derive a PeerID.
				// Kubo and Rainbow need a PeerID to pass multiaddr addresses
				// over legacy routing APIs even when the provider uses
				// non-PeerID identifiers like did:key:.
				if slices.Contains(result.Protocols, "transport-ipfs-gateway-http") && hasHTTPURL(result.Addrs) {
					pid, err = peerIDFromDIDKey(result.ID)
					if err != nil {
						pid = peerIDPlaceholderFromArbitraryID(result.ID)
					}
				} else {
					// Records with non-PeerID identifiers and no recognized
					// protocol are skipped: without a protocol hint we cannot
					// determine how to use the addresses in legacy routing APIs.
					logger.Debugw("skipping generic record with non-PeerID identifier", "ID", result.ID)
					continue
				}
			}

			// Convert addresses to multiaddrs. URLs are converted via
			// ToMultiaddr (e.g. https://host -> /dns/host/tcp/443/https).
			// Addresses that cannot be converted are dropped.
			var addrs []multiaddr.Multiaddr
			for i := range result.Addrs {
				if ma := result.Addrs[i].ToMultiaddr(); ma != nil {
					addrs = append(addrs, ma)
				}
			}
			if len(addrs) == 0 {
				logger.Debugw("skipping generic record with no convertible addresses", "ID", result.ID)
				continue
			}

			select {
			case <-ctx.Done():
				return
			case ch <- peer.AddrInfo{
				ID:    pid,
				Addrs: addrs,
			}:
			}

		//nolint:staticcheck
		//lint:ignore SA1019 // ignore staticcheck
		case types.SchemaBitswap:
			//lint:ignore SA1019 // ignore staticcheck
			result, ok := v.(*types.BitswapRecord)
			if !ok {
				logger.Errorw(
					"problem casting find providers result",
					"Schema", v.GetSchema(),
					"Type", reflect.TypeOf(v).String(),
				)
				continue
			}

			select {
			case <-ctx.Done():
				return
			case ch <- peer.AddrInfo{
				ID:    *result.ID,
				Addrs: filterAddrs(result.Addrs),
			}:
			}
		}
	}
}

func (c *contentRouter) FindProvidersAsync(ctx context.Context, key cid.Cid, numResults int) <-chan peer.AddrInfo {
	resultsIter, err := c.client.FindProviders(ctx, key)
	if err != nil {
		logger.Warnw("error finding providers", "CID", key, "Error", err)
		ch := make(chan peer.AddrInfo)
		close(ch)
		return ch
	}
	ch := make(chan peer.AddrInfo)
	go readProviderResponses(ctx, resultsIter, ch)
	return ch
}

func (c *contentRouter) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	iter, err := c.client.FindPeers(ctx, pid)
	if err != nil {
		return peer.AddrInfo{}, err
	}
	defer iter.Close()

	for iter.Next() {
		res := iter.Val()
		if res.Err != nil {
			logger.Warnf("error iterating peer responses: %s", res.Err)
			continue
		}

		if *res.Val.ID != pid {
			logger.Warnf("searched for peerID %s, got response for %s:", pid, *res.Val.ID)
			continue
		}

		addrs := filterAddrs(res.Val.Addrs)
		// If there are no addresses there's nothing of value to return
		if len(addrs) == 0 {
			continue
		}

		return peer.AddrInfo{
			ID:    pid,
			Addrs: addrs,
		}, nil
	}

	return peer.AddrInfo{}, routing.ErrNotFound
}

func (c *contentRouter) PutValue(ctx context.Context, key string, data []byte, opts ...routing.Option) error {
	if !strings.HasPrefix(key, "/ipns/") {
		return routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return err
	}

	record, err := ipns.UnmarshalRecord(data)
	if err != nil {
		return err
	}

	return c.client.PutIPNS(ctx, name, record)
}

func (c *contentRouter) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	if !strings.HasPrefix(key, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return nil, err
	}

	record, err := c.client.GetIPNS(ctx, name)
	if err != nil {
		return nil, err
	}

	return ipns.MarshalRecord(record)
}

func (c *contentRouter) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	if !strings.HasPrefix(key, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte)

	go func() {
		record, err := c.client.GetIPNS(ctx, name)
		if err != nil {
			close(ch)
			return
		}

		raw, err := ipns.MarshalRecord(record)
		if err != nil {
			close(ch)
			return
		}

		ch <- raw
		close(ch)
	}()

	return ch, nil
}
