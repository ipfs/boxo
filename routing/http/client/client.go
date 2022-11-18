package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	delegatedrouting "github.com/ipfs/go-delegated-routing"
	"github.com/ipfs/go-delegated-routing/internal"
	ipns "github.com/ipfs/go-ipns"
	logging "github.com/ipfs/go-log/v2"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

var logger = logging.Logger("service/delegatedrouting")

type Provider struct {
	ID    peer.ID
	Addrs []multiaddr.Multiaddr
}

type client struct {
	baseURL    string
	httpClient httpClient
	validator  record.Validator
	clock      clock.Clock

	provider Provider
	identity crypto.PrivKey
}

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

func WithProvider(p Provider) option {
	return func(c *client) {
		c.provider = p
	}
}

// New creates a content routing API client.
// The Provider and identity parameters are option. If they are nil, the `Provide` method will not function.
func New(baseURL string, opts ...option) (*client, error) {
	client := &client{
		baseURL:    baseURL,
		httpClient: http.DefaultClient,
		validator:  ipns.Validator{},
		clock:      clock.New(),
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.identity != nil && client.provider.ID.Size() != 0 && !client.provider.ID.MatchesPublicKey(client.identity.GetPublic()) {
		return nil, errors.New("identity does not match provider")
	}

	return client, nil
}

func (c *client) FindProviders(ctx context.Context, key cid.Cid) ([]delegatedrouting.Provider, error) {
	url := c.baseURL + "/v1/providers/" + key.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpError(resp.StatusCode, resp.Body)
	}

	parsedResp := &delegatedrouting.FindProvidersResponse{}
	err = json.NewDecoder(resp.Body).Decode(parsedResp)
	return parsedResp.Providers, err
}

func (c *client) ProvideBitswap(ctx context.Context, keys []cid.Cid, ttl time.Duration) (time.Duration, error) {
	if c.identity == nil {
		return 0, errors.New("cannot Provide without an identity")
	}
	if c.provider.ID.Size() == 0 {
		return 0, errors.New("cannot Provide without a provider")
	}

	ks := make([]delegatedrouting.CID, len(keys))
	for i, c := range keys {
		ks[i] = delegatedrouting.CID{Cid: c}
	}

	now := c.clock.Now()

	req := delegatedrouting.BitswapWriteProviderRequest{
		Protocol: "bitswap",
		BitswapWriteProviderRequestPayload: delegatedrouting.BitswapWriteProviderRequestPayload{
			Keys:        ks,
			AdvisoryTTL: delegatedrouting.Duration{Duration: ttl},
			Timestamp:   delegatedrouting.Time{Time: now},
			ID:          c.provider.ID,
			Addrs:       c.provider.Addrs,
		},
	}
	err := req.Sign(c.provider.ID, c.identity)
	if err != nil {
		return 0, err
	}

	advisoryTTL, err := c.provideSignedBitswapRecord(ctx, req)
	if err != nil {
		return 0, err
	}

	return advisoryTTL, err
}

// ProvideAsync makes a provide request to a delegated router
func (c *client) provideSignedBitswapRecord(ctx context.Context, bswp delegatedrouting.BitswapWriteProviderRequest) (time.Duration, error) {
	if !bswp.IsSigned() {
		return 0, errors.New("request is not signed")
	}

	req := delegatedrouting.WriteProvidersRequest{Providers: []delegatedrouting.Provider{bswp}}

	url := c.baseURL + "/v1/providers"

	reqBodyBuf, err := internal.MarshalJSON(req)
	if err != nil {
		return 0, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reqBodyBuf)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("making HTTP req to provide a signed record: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, httpError(resp.StatusCode, resp.Body)
	}
	provideResult := delegatedrouting.WriteProvidersResponse{Protocols: []string{"bitswap"}}
	err = json.NewDecoder(resp.Body).Decode(&provideResult)
	if err != nil {
		return 0, err
	}
	if len(provideResult.ProvideResults) != 1 {
		return 0, fmt.Errorf("expected 1 result but got %d", len(provideResult.ProvideResults))
	}
	return provideResult.ProvideResults[0].(*delegatedrouting.BitswapWriteProviderResponse).AdvisoryTTL, nil
}

func (c *client) Ready(ctx context.Context) (bool, error) {
	url := c.baseURL + "/v1/ping"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		return false, nil
	}
	return false, fmt.Errorf("unexpected HTTP status code '%d'", resp.StatusCode)
}
