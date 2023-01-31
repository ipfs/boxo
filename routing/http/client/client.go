package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	ipns "github.com/ipfs/go-ipns"
	"github.com/ipfs/go-libipfs/routing/http/contentrouter"
	"github.com/ipfs/go-libipfs/routing/http/internal/drjson"
	"github.com/ipfs/go-libipfs/routing/http/server"
	"github.com/ipfs/go-libipfs/routing/http/types"
	logging "github.com/ipfs/go-log/v2"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

var logger = logging.Logger("service/delegatedrouting")

type client struct {
	baseURL    string
	httpClient httpClient
	validator  record.Validator
	clock      clock.Clock

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

type option func(*client)

func WithIdentity(identity crypto.PrivKey) option {
	return func(c *client) {
		c.identity = identity
	}
}

func WithHTTPClient(h httpClient) option {
	return func(c *client) {
		c.httpClient = h
	}
}

func WithUserAgent(ua string) option {
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

func WithProviderInfo(peerID peer.ID, addrs []multiaddr.Multiaddr) option {
	return func(c *client) {
		c.peerID = peerID
		for _, a := range addrs {
			c.addrs = append(c.addrs, types.Multiaddr{Multiaddr: a})
		}
	}
}

// New creates a content routing API client.
// The Provider and identity parameters are option. If they are nil, the `Provide` method will not function.
func New(baseURL string, opts ...option) (*client, error) {
	defaultHTTPClient := &http.Client{
		Transport: &ResponseBodyLimitedTransport{
			RoundTripper: http.DefaultTransport,
			LimitBytes:   1 << 20,
			UserAgent:    defaultUserAgent,
		},
	}
	client := &client{
		baseURL:    baseURL,
		httpClient: defaultHTTPClient,
		validator:  ipns.Validator{},
		clock:      clock.New(),
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.identity != nil && client.peerID.Size() != 0 && !client.peerID.MatchesPublicKey(client.identity.GetPublic()) {
		return nil, errors.New("identity does not match provider")
	}

	return client, nil
}

func (c *client) FindProviders(ctx context.Context, key cid.Cid) (provs []types.ProviderResponse, err error) {
	measurement := newMeasurement("FindProviders")
	defer func() {
		measurement.length = len(provs)
		measurement.record(ctx)
	}()

	url := c.baseURL + server.ProvidePath + key.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	measurement.host = req.Host

	start := c.clock.Now()
	resp, err := c.httpClient.Do(req)

	measurement.err = err
	measurement.latency = c.clock.Since(start)

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	measurement.statusCode = resp.StatusCode
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpError(resp.StatusCode, resp.Body)
	}

	parsedResp := &types.ReadProvidersResponse{}
	err = json.NewDecoder(resp.Body).Decode(parsedResp)
	return parsedResp.Providers, err
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
	req := types.WriteProvidersRequest{Providers: []types.WriteProviderRecord{bswp}}

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
	var provideResult types.WriteProvidersResponse
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
