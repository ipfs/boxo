package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	ipns "github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/contentrouter"
	"github.com/ipfs/boxo/routing/http/internal/drjson"
	"github.com/ipfs/boxo/routing/http/server"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	jsontypes "github.com/ipfs/boxo/routing/http/types/json"
	"github.com/ipfs/boxo/routing/http/types/ndjson"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

var (
	_                 contentrouter.Client = &client{}
	logger                                 = logging.Logger("service/delegatedrouting")
	defaultHTTPClient                      = &http.Client{
		Transport: &ResponseBodyLimitedTransport{
			RoundTripper: http.DefaultTransport,
			LimitBytes:   1 << 20,
			UserAgent:    defaultUserAgent,
		},
	}
)

const (
	mediaTypeJSON   = "application/json"
	mediaTypeNDJSON = "application/x-ndjson"
)

type client struct {
	baseURL    string
	httpClient httpClient
	validator  record.Validator
	clock      clock.Clock

	accepts string

	peerID   peer.ID
	addrs    []types.Multiaddr
	identity crypto.PrivKey

	// called immeidately after signing a provide req
	// used for testing, e.g. testing the server with a mangled signature
	afterSignCallback func(req *types.WriteBitswapProviderRecord)
}

// defaultUserAgent is used as a fallback to inform HTTP server which library
// version sent a request
var defaultUserAgent = moduleVersion()

var _ contentrouter.Client = &client{}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Option func(*client)

func WithIdentity(identity crypto.PrivKey) Option {
	return func(c *client) {
		c.identity = identity
	}
}

func WithHTTPClient(h httpClient) Option {
	return func(c *client) {
		c.httpClient = h
	}
}

func WithUserAgent(ua string) Option {
	return func(c *client) {
		if ua == "" {
			return
		}
		httpClient, ok := c.httpClient.(*http.Client)
		if !ok {
			return
		}
		transport, ok := httpClient.Transport.(*ResponseBodyLimitedTransport)
		if !ok {
			return
		}
		transport.UserAgent = ua
	}
}

func WithProviderInfo(peerID peer.ID, addrs []multiaddr.Multiaddr) Option {
	return func(c *client) {
		c.peerID = peerID
		for _, a := range addrs {
			c.addrs = append(c.addrs, types.Multiaddr{Multiaddr: a})
		}
	}
}

func WithStreamResultsRequired() Option {
	return func(c *client) {
		c.accepts = mediaTypeNDJSON
	}
}

// New creates a content routing API client.
// The Provider and identity parameters are option. If they are nil, the `Provide` method will not function.
func New(baseURL string, opts ...Option) (*client, error) {
	client := &client{
		baseURL:    baseURL,
		httpClient: defaultHTTPClient,
		validator:  ipns.Validator{},
		clock:      clock.New(),
		accepts:    strings.Join([]string{mediaTypeNDJSON, mediaTypeJSON}, ","),
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.identity != nil && client.peerID.Size() != 0 && !client.peerID.MatchesPublicKey(client.identity.GetPublic()) {
		return nil, errors.New("identity does not match provider")
	}

	return client, nil
}

// measuringIter measures the length of the iter and then publishes metrics about the whole req once the iter is closed.
// Of course, if the caller forgets to close the iter, this won't publish anything.
type measuringIter[T any] struct {
	iter.Iter[T]
	ctx context.Context
	m   *measurement
}

func (c *measuringIter[T]) Next() bool {
	c.m.length++
	return c.Iter.Next()
}

func (c *measuringIter[T]) Val() T {
	return c.Iter.Val()
}

func (c *measuringIter[T]) Close() error {
	c.m.record(c.ctx)
	return c.Iter.Close()
}

func (c *client) FindProviders(ctx context.Context, key cid.Cid) (provs iter.ResultIter[types.ProviderResponse], err error) {
	// TODO test measurements
	m := newMeasurement("FindProviders")

	url := c.baseURL + server.ProvidePath + key.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", c.accepts)

	m.host = req.Host

	start := c.clock.Now()
	resp, err := c.httpClient.Do(req)

	m.err = err
	m.latency = c.clock.Since(start)

	if err != nil {
		m.record(ctx)
		return nil, err
	}

	m.statusCode = resp.StatusCode
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		m.record(ctx)
		return iter.FromSlice[iter.Result[types.ProviderResponse]](nil), nil
	}

	if resp.StatusCode != http.StatusOK {
		err := httpError(resp.StatusCode, resp.Body)
		resp.Body.Close()
		m.record(ctx)
		return nil, err
	}

	respContentType := resp.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(respContentType)
	if err != nil {
		resp.Body.Close()
		m.err = err
		m.record(ctx)
		return nil, fmt.Errorf("parsing Content-Type: %w", err)
	}

	m.mediaType = mediaType

	var skipBodyClose bool
	defer func() {
		if !skipBodyClose {
			resp.Body.Close()
		}
	}()

	var it iter.ResultIter[types.ProviderResponse]
	switch mediaType {
	case mediaTypeJSON:
		parsedResp := &jsontypes.ReadProvidersResponse{}
		err = json.NewDecoder(resp.Body).Decode(parsedResp)
		var sliceIt iter.Iter[types.ProviderResponse] = iter.FromSlice(parsedResp.Providers)
		it = iter.ToResultIter(sliceIt)
	case mediaTypeNDJSON:
		skipBodyClose = true
		it = ndjson.NewReadProvidersResponseIter(resp.Body)
	default:
		logger.Errorw("unknown media type", "MediaType", mediaType, "ContentType", respContentType)
		return nil, errors.New("unknown content type")
	}

	return &measuringIter[iter.Result[types.ProviderResponse]]{Iter: it, ctx: ctx, m: m}, nil
}

func (c *client) ProvideBitswap(ctx context.Context, keys []cid.Cid, ttl time.Duration) (time.Duration, error) {
	if c.identity == nil {
		return 0, errors.New("cannot provide Bitswap records without an identity")
	}
	if c.peerID.Size() == 0 {
		return 0, errors.New("cannot provide Bitswap records without a peer ID")
	}

	ks := make([]types.CID, len(keys))
	for i, c := range keys {
		ks[i] = types.CID{Cid: c}
	}

	now := c.clock.Now()

	req := types.WriteBitswapProviderRecord{
		Protocol: "transport-bitswap",
		Schema:   types.SchemaBitswap,
		Payload: types.BitswapPayload{
			Keys:        ks,
			AdvisoryTTL: &types.Duration{Duration: ttl},
			Timestamp:   &types.Time{Time: now},
			ID:          &c.peerID,
			Addrs:       c.addrs,
		},
	}
	err := req.Sign(c.peerID, c.identity)
	if err != nil {
		return 0, err
	}

	if c.afterSignCallback != nil {
		c.afterSignCallback(&req)
	}

	advisoryTTL, err := c.provideSignedBitswapRecord(ctx, &req)
	if err != nil {
		return 0, err
	}

	return advisoryTTL, err
}

// ProvideAsync makes a provide request to a delegated router
func (c *client) provideSignedBitswapRecord(ctx context.Context, bswp *types.WriteBitswapProviderRecord) (time.Duration, error) {
	req := jsontypes.WriteProvidersRequest{Providers: []types.WriteProviderRecord{bswp}}

	url := c.baseURL + server.ProvidePath

	b, err := drjson.MarshalJSONBytes(req)
	if err != nil {
		return 0, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewBuffer(b))
	if err != nil {
		return 0, err
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("making HTTP req to provide a signed record: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, httpError(resp.StatusCode, resp.Body)
	}
	var provideResult jsontypes.WriteProvidersResponse
	err = json.NewDecoder(resp.Body).Decode(&provideResult)
	if err != nil {
		return 0, err
	}
	if len(provideResult.ProvideResults) != 1 {
		return 0, fmt.Errorf("expected 1 result but got %d", len(provideResult.ProvideResults))
	}

	v, ok := provideResult.ProvideResults[0].(*types.WriteBitswapProviderRecordResponse)
	if !ok {
		return 0, fmt.Errorf("expected AdvisoryTTL field")
	}

	if v.AdvisoryTTL != nil {
		return v.AdvisoryTTL.Duration, nil
	}

	return 0, nil
}
